// Package ratelimit provides the process-local unary RPC token buckets used
// to stay below the broker's per-method-group limits.
package ratelimit

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	investapi "tinvest/internal/pb/investapi"
)

const DefaultMaxWait = 2 * time.Second

// Limit describes one broker method group. Methods are exact full RPC paths
// (or method names returned by GetUserTariff); Services apply the limit to all
// unary methods on those protobuf services.
type Limit struct {
	Group     string
	Methods   []string
	Services  []string
	PerMinute int
	PerSecond int
	Burst     int
}

// DefaultLimits are conservative public-tariff defaults. A successful
// GetUserTariff response can replace individual method mappings at runtime.
func DefaultLimits() []Limit {
	return []Limit{
		{Group: "marketdata", Services: []string{"MarketDataService"}, PerMinute: 600, Burst: 10},
		{Group: "orders", Services: []string{"OrdersService"}, PerMinute: 100, Burst: 10},
		{Group: "instrument-lists", Methods: []string{
			investapi.InstrumentsService_Bonds_FullMethodName,
			investapi.InstrumentsService_Currencies_FullMethodName,
			investapi.InstrumentsService_Etfs_FullMethodName,
			investapi.InstrumentsService_Futures_FullMethodName,
			investapi.InstrumentsService_Options_FullMethodName,
			investapi.InstrumentsService_Shares_FullMethodName,
			// The June 2026 API update caps the assets endpoints at 15/min in the
			// same group as the six instrument lists. The CLI doesn't call them
			// today, but the limiter models the published limit.
			investapi.InstrumentsService_GetAssets_FullMethodName,
			investapi.InstrumentsService_GetAssetBy_FullMethodName,
		}, PerMinute: 15, Burst: 1},
		{Group: "instruments", Services: []string{"InstrumentsService"}, PerMinute: 200, Burst: 10},
		{Group: "operations", Services: []string{"OperationsService"}, PerMinute: 300, Burst: 10},
		{Group: "users", Services: []string{"UsersService"}, PerMinute: 100, Burst: 10},
		{Group: "stop-orders", Services: []string{"StopOrdersService"}, PerMinute: 50, Burst: 5},
		{Group: "sandbox", Services: []string{"SandboxService"}, PerMinute: 300, Burst: 10},
		{Group: "signals", Services: []string{"SignalService"}, PerMinute: 100, Burst: 10},
		{Group: "default", PerMinute: 100, Burst: 10},
	}
}

// LimitsFromTariff translates GetUserTariff's unary groups into limiter
// configuration. Invalid/empty groups are ignored and static defaults remain.
func LimitsFromTariff(groups []*investapi.UnaryLimit) []Limit {
	limits := make([]Limit, 0, len(groups))
	for i, group := range groups {
		perMinute := int(group.GetLimitPerMinute())
		perSecond := int(group.GetLimitPerSecond())
		if perMinute <= 0 && perSecond <= 0 {
			continue
		}
		burst := 10
		if perSecond > 0 && perSecond < burst {
			burst = perSecond
		} else if perSecond == 0 && perMinute > 0 && perMinute < burst {
			burst = perMinute
		}
		limits = append(limits, Limit{
			Group: "tariff-" + itoa(i), Methods: append([]string(nil), group.GetMethods()...),
			PerMinute: perMinute, PerSecond: perSecond, Burst: burst,
		})
	}
	return limits
}

// Limiter owns one token bucket per method group.
type Limiter struct {
	mu             sync.Mutex
	groups         map[string]*bucket
	methodGroups   map[string]string
	serviceGroups  map[string]string
	tariffMethods  map[string]string
	tariffServices map[string]string
	tariffGroups   map[string]struct{}
	defaultGroup   string
	maxWait        time.Duration
}

type bucket struct {
	rate       float64
	capacity   float64
	tokens     float64
	lastRefill time.Time
}

type localLimitError struct {
	group string
	wait  time.Duration
}

func (e *localLimitError) Error() string { return e.GRPCStatus().Message() }
func (e *localLimitError) GRPCStatus() *status.Status {
	return status.New(codes.ResourceExhausted, "client rate limit for "+e.group+"; retry after "+e.wait.Round(time.Millisecond).String())
}
func (e *localLimitError) NoRetry() bool { return true }

// New returns a limiter with full initial buckets.
func New(limits []Limit, maxWait time.Duration) *Limiter {
	if maxWait <= 0 {
		maxWait = DefaultMaxWait
	}
	l := &Limiter{
		groups: make(map[string]*bucket), methodGroups: make(map[string]string),
		serviceGroups: make(map[string]string), tariffMethods: make(map[string]string),
		tariffServices: make(map[string]string), tariffGroups: make(map[string]struct{}), maxWait: maxWait,
	}
	l.Update(limits)
	return l
}

// Update overlays refreshed tariff groups on the current static defaults.
func (l *Limiter) Update(limits []Limit) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	for _, limit := range limits {
		l.installLocked(limit, l.methodGroups, l.serviceGroups, now)
	}
}

// RefreshTariff atomically replaces the previous GetUserTariff mappings.
// Static groups remain as fallbacks for methods omitted by the refreshed
// response.
func (l *Limiter) RefreshTariff(limits []Limit) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for group := range l.tariffGroups {
		delete(l.groups, group)
	}
	l.tariffMethods = make(map[string]string)
	l.tariffServices = make(map[string]string)
	l.tariffGroups = make(map[string]struct{})
	now := time.Now()
	for _, limit := range limits {
		if l.installLocked(limit, l.tariffMethods, l.tariffServices, now) {
			l.tariffGroups[limit.Group] = struct{}{}
		}
	}
}

func (l *Limiter) installLocked(limit Limit, methods, services map[string]string, now time.Time) bool {
	if limit.Group == "" {
		return false
	}
	rate := effectiveRate(limit)
	if rate <= 0 {
		return false
	}
	burst := limit.Burst
	if burst <= 0 {
		burst = 1
	}
	l.groups[limit.Group] = &bucket{
		rate: rate, capacity: float64(burst), tokens: float64(burst), lastRefill: now,
	}
	if limit.Group == "default" {
		l.defaultGroup = limit.Group
	}
	for _, method := range limit.Methods {
		methods[configuredMethodKey(method)] = limit.Group
	}
	for _, service := range limit.Services {
		services[service] = limit.Group
	}
	return true
}

// Wait consumes one token for method, blocking only as long as maxWait and
// the call deadline both allow. A rejected local reservation uses the gRPC
// RESOURCE_EXHAUSTED code so the existing renderer maps it to RATE_LIMITED.
func (l *Limiter) Wait(ctx context.Context, method string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	group := l.groupLocked(method)
	b := l.groups[group]
	if b == nil {
		l.mu.Unlock()
		return nil
	}
	now := time.Now()
	b.refill(now)
	b.tokens -= 1
	wait := time.Duration(0)
	if b.tokens < 0 {
		wait = time.Duration((-b.tokens / b.rate) * float64(time.Second))
	}
	if wait > l.maxWait || deadlineTooSoon(ctx, now, wait) {
		b.tokens = math.Min(b.capacity, b.tokens+1)
		l.mu.Unlock()
		return &localLimitError{group: group, wait: wait}
	}
	l.mu.Unlock()

	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		b.refill(time.Now())
		b.tokens = math.Min(b.capacity, b.tokens+1)
		l.mu.Unlock()
		return ctx.Err()
	}
}

// UnaryClientInterceptor applies Wait before each unary wire attempt.
func (l *Limiter) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := l.Wait(ctx, method); err != nil {
			return err
		}
		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			return err
		}
		if methodKey(method) == "GetUserTariff" {
			if tariff, ok := reply.(*investapi.GetUserTariffResponse); ok {
				l.RefreshTariff(LimitsFromTariff(tariff.GetUnaryLimits()))
			}
		}
		return nil
	}
}

func (b *bucket) refill(now time.Time) {
	if now.Before(b.lastRefill) {
		return
	}
	b.tokens = math.Min(b.capacity, b.tokens+now.Sub(b.lastRefill).Seconds()*b.rate)
	b.lastRefill = now
}

func (l *Limiter) groupLocked(method string) string {
	if group := lookupMethodGroup(l.tariffMethods, method); group != "" {
		return group
	}
	if group := l.tariffServices[serviceName(method)]; group != "" {
		return group
	}
	if group := lookupMethodGroup(l.methodGroups, method); group != "" {
		return group
	}
	if group := l.serviceGroups[serviceName(method)]; group != "" {
		return group
	}
	return l.defaultGroup
}

func lookupMethodGroup(groups map[string]string, method string) string {
	if group := groups[method]; group != "" {
		return group
	}
	return groups[methodKey(method)]
}

func configuredMethodKey(method string) string {
	if strings.Contains(method, "/") {
		return method
	}
	return methodKey(method)
}

func effectiveRate(limit Limit) float64 {
	rate := 0.0
	if limit.PerMinute > 0 {
		rate = float64(limit.PerMinute) / 60
	}
	if limit.PerSecond > 0 && (rate == 0 || float64(limit.PerSecond) < rate) {
		rate = float64(limit.PerSecond)
	}
	return rate
}

func deadlineTooSoon(ctx context.Context, now time.Time, wait time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return ok && now.Add(wait).After(deadline)
}

func methodKey(method string) string {
	if slash := strings.LastIndexByte(method, '/'); slash >= 0 {
		return method[slash+1:]
	}
	return method
}

func serviceName(method string) string {
	slash := strings.LastIndexByte(method, '/')
	if slash <= 0 {
		return ""
	}
	prefix := method[:slash]
	if dot := strings.LastIndexByte(prefix, '.'); dot >= 0 {
		return prefix[dot+1:]
	}
	return strings.TrimPrefix(prefix, "/")
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := [20]byte{}
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
