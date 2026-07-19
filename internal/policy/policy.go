// Package policy implements the pre-trade guardrails described in
// plan-tinvest-cli.md §6: an instrument allowlist, per-order lot and notional
// caps, an open-order cap, market/short opt-in flags, and a kill-switch file
// whose presence blocks every mutation. These are guardrails against agent
// mistakes, not a security boundary (§6, §14): an agent that can read the
// token can bypass the binary. Their value is catching fat-finger and runaway
// loops before anything reaches the broker.
//
// Every violation is a local decision — no network, no ledger entry — so a
// breached rule fails the command with exit 2 and a machine-readable POLICY
// error before the write-ahead intent is ever created (plan §7/§9).
//
// The policy file is TOML, referenced from a profile's `policy_file`
// (config.Profile.PolicyFile). Unknown keys are rejected so a typo in a safety
// rule fails loudly instead of silently disabling a guardrail.
package policy

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"

	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
)

// Policy is the resolved set of guardrails for one profile. The zero value is
// the permissive default a command uses when no policy file is configured:
// market orders and shorts disabled, no caps, no allowlist, no kill switch —
// note that with no policy file, market orders are still allowed (the opt-in
// only bites when a policy file is present). See Load for the file-backed
// defaults.
type Policy struct {
	// AllowedInstruments is the allowlist of instrument identifiers (uids,
	// FIGIs, or TICKER@CLASSCODE). Empty means allow all. Matching is done
	// against every identity of the resolved instrument plus the raw argument.
	AllowedInstruments []string
	// MaxLotsPerOrder caps lots on a single order. Zero means no cap.
	MaxLotsPerOrder int64
	// MaxNotionalPerOrder caps the notional value (lots * lot size * price) of
	// a single order, as an exact decimal string. Empty means no cap. It applies
	// only to priced (limit) orders: market and bestprice orders have no price at
	// the local validation stage, so they bypass this cap and are gated by
	// AllowMarketOrders instead.
	MaxNotionalPerOrder string
	// NotionalCurrency is the currency MaxNotionalPerOrder is denominated in.
	// When set, an order in a different currency cannot be checked and is
	// rejected rather than waved through.
	NotionalCurrency string
	// MaxOpenOrders caps the number of open orders on the account. Zero means
	// no cap. Enforced via CheckOpenOrders, which the caller feeds a count
	// obtained from a read.
	MaxOpenOrders int
	// AllowMarketOrders gates market and bestprice order types. Default false.
	AllowMarketOrders bool
	// AllowShorts gates sell orders that would open or increase a short
	// position. Default false. NOTE (plan §6): the actual position check needs
	// portfolio data that M1 does not fetch; today this flag is carried and
	// surfaced but the position comparison is a documented TODO for M2. A sell
	// order is not rejected on this flag alone in M1.
	AllowShorts bool
	// KillSwitchFile, when it exists on disk, blocks every mutation. Presence
	// is the signal; contents are irrelevant.
	KillSwitchFile string

	// present records whether this Policy was loaded from a file. It flips the
	// market-order opt-in from advisory to enforced: without a policy file the
	// CLI does not restrict order types (backwards compatible), with one the
	// AllowMarketOrders default of false bites.
	present bool
}

// file mirrors the on-disk policy.toml. Field names are the stable config
// surface; keep them in sync with the README.
type file struct {
	AllowedInstruments  []string `toml:"allowed_instruments"`
	MaxLotsPerOrder     int64    `toml:"max_lots_per_order"`
	MaxNotionalPerOrder string   `toml:"max_notional_per_order"`
	NotionalCurrency    string   `toml:"notional_currency"`
	MaxOpenOrders       int      `toml:"max_open_orders"`
	AllowMarketOrders   bool     `toml:"allow_market_orders"`
	AllowShorts         bool     `toml:"allow_shorts"`
	KillSwitchFile      string   `toml:"kill_switch_file"`
}

// Load reads and validates a policy file. An empty path yields a nil Policy
// (no guardrails configured), which callers treat as permissive. A missing
// file at a configured path is an error: a policy referenced but absent is a
// misconfiguration, not an intent to disable safety. Unknown keys are rejected.
func Load(path string) (*Policy, error) {
	if path == "" {
		return nil, nil
	}
	path = expandHome(path)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("policy: file %s not found", path)
	}
	if err != nil {
		return nil, fmt.Errorf("policy: read %s: %w", path, err)
	}

	var f file
	md, err := toml.Decode(string(data), &f)
	if err != nil {
		return nil, fmt.Errorf("policy: parse %s: %w", path, err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		return nil, fmt.Errorf("policy: unknown key %q in %s", undecoded[0].String(), path)
	}

	// Reject explicitly-set non-positive caps: a cap of 0 or a negative value
	// silently disables the guardrail (the "> 0" checks never fire), which is a
	// dangerous misconfiguration. Omitting the key is how you say "no cap" (F12).
	if md.IsDefined("max_lots_per_order") && f.MaxLotsPerOrder <= 0 {
		return nil, fmt.Errorf("policy: max_lots_per_order must be positive (got %d); omit it for no cap", f.MaxLotsPerOrder)
	}
	if md.IsDefined("max_open_orders") && f.MaxOpenOrders <= 0 {
		return nil, fmt.Errorf("policy: max_open_orders must be positive (got %d); omit it for no cap", f.MaxOpenOrders)
	}

	if f.MaxNotionalPerOrder != "" {
		q, err := render.ParseQuotation(f.MaxNotionalPerOrder)
		if err != nil {
			return nil, fmt.Errorf("policy: invalid max_notional_per_order %q: %w", f.MaxNotionalPerOrder, err)
		}
		if quotationRat(q).Sign() <= 0 {
			return nil, fmt.Errorf("policy: max_notional_per_order must be positive (got %q); omit it for no cap", f.MaxNotionalPerOrder)
		}
		if f.NotionalCurrency == "" {
			return nil, fmt.Errorf("policy: max_notional_per_order requires notional_currency")
		}
	}

	p := &Policy{
		AllowedInstruments:  f.AllowedInstruments,
		MaxLotsPerOrder:     f.MaxLotsPerOrder,
		MaxNotionalPerOrder: f.MaxNotionalPerOrder,
		NotionalCurrency:    strings.ToLower(f.NotionalCurrency),
		MaxOpenOrders:       f.MaxOpenOrders,
		AllowMarketOrders:   f.AllowMarketOrders,
		AllowShorts:         f.AllowShorts,
		KillSwitchFile:      expandHome(f.KillSwitchFile),
		present:             true,
	}
	return p, nil
}

// Violation is a breached guardrail. Rule is the machine-readable rule name;
// Details carries the bound and actual values for programmatic handling.
type Violation struct {
	Rule    string
	Message string
	Details map[string]string
}

func (v *Violation) Error() string { return v.Message }

func newViolation(rule, message string, kv ...string) *Violation {
	details := map[string]string{"rule": rule}
	for i := 0; i+1 < len(kv); i += 2 {
		details[kv[i]] = kv[i+1]
	}
	return &Violation{Rule: rule, Message: message, Details: details}
}

// OrderIntent is everything a policy needs to judge a single order. The
// instrument-derived fields (LotSize, Currency, and the identities) are
// populated after resolution; the request fields come straight from the flags
// or JSON input.
type OrderIntent struct {
	Direction investapi.OrderDirection
	OrderType investapi.OrderType
	Lots      int64
	Price     *investapi.Quotation // nil for market/bestprice

	// Instrument-derived, set post-resolution.
	LotSize   int32
	Currency  string
	UID       string
	FIGI      string
	Ticker    string
	ClassCode string
	RawID     string // the identifier the user passed
}

// CheckKillSwitch reports the kill-switch violation if the configured file
// exists. It is the first gate a mutation passes and needs no order details,
// so a command can run it before touching anything else.
//
// The switch fails CLOSED: a clean not-exists is the only result that lets a
// mutation proceed. Any other stat error (permission denied, I/O error, a
// non-directory path component) blocks the mutation with a POLICY error saying
// the check itself failed — never proceed when we cannot determine whether the
// operator engaged the switch (finding F11).
//
// os.Lstat (not os.Stat) is used so a symlink AT the kill path counts as engaged
// even when its target is missing: os.Stat would follow a dangling symlink to a
// not-exists and treat the switch as absent, defeating it (finding F18). Lstat
// stats the link itself, so any object at the path — file, dir, or dangling
// symlink — engages the switch.
func (p *Policy) CheckKillSwitch() *Violation {
	if p == nil || p.KillSwitchFile == "" {
		return nil
	}
	switch _, err := os.Lstat(p.KillSwitchFile); {
	case err == nil:
		return newViolation("kill_switch",
			fmt.Sprintf("kill switch engaged: %s exists, all mutations blocked", p.KillSwitchFile),
			"kill_switch_file", p.KillSwitchFile)
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return newViolation("kill_switch",
			fmt.Sprintf("kill switch check failed for %s: %v; refusing to proceed", p.KillSwitchFile, err),
			"kill_switch_file", p.KillSwitchFile, "error", err.Error())
	}
}

// CheckLocal runs the guardrails that need no instrument resolution: the kill
// switch, the market-order opt-in, and the lot cap. It is meant to run before
// any network call so obviously-bad orders fail with exit 2 without a token.
func (p *Policy) CheckLocal(in OrderIntent) *Violation {
	if p == nil {
		return nil
	}
	if v := p.CheckKillSwitch(); v != nil {
		return v
	}
	if p.present && !p.AllowMarketOrders {
		switch in.OrderType {
		case investapi.OrderType_ORDER_TYPE_MARKET, investapi.OrderType_ORDER_TYPE_BESTPRICE:
			return newViolation("allow_market_orders",
				"market and bestprice orders are disabled by policy (allow_market_orders=false)",
				"order_type", in.OrderType.String())
		}
	}
	if p.MaxLotsPerOrder > 0 && in.Lots > p.MaxLotsPerOrder {
		return newViolation("max_lots_per_order",
			fmt.Sprintf("order of %d lots exceeds max_lots_per_order=%d", in.Lots, p.MaxLotsPerOrder),
			"limit", fmt.Sprint(p.MaxLotsPerOrder), "actual", fmt.Sprint(in.Lots))
	}
	return nil
}

// CheckResolved runs the guardrails that need the resolved instrument: the
// allowlist and the notional cap. It runs after CheckLocal, once the
// instrument's identities, lot size, and currency are known.
func (p *Policy) CheckResolved(in OrderIntent) *Violation {
	if p == nil {
		return nil
	}
	if v := p.checkAllowlist(in); v != nil {
		return v
	}
	if v := p.checkNotional(in); v != nil {
		return v
	}
	return nil
}

func (p *Policy) checkAllowlist(in OrderIntent) *Violation {
	if len(p.AllowedInstruments) == 0 {
		return nil
	}
	tickerClass := ""
	if in.Ticker != "" && in.ClassCode != "" {
		tickerClass = in.Ticker + "@" + in.ClassCode
	}
	for _, allowed := range p.AllowedInstruments {
		a := strings.TrimSpace(allowed)
		if a == "" {
			continue
		}
		if strings.EqualFold(a, in.UID) ||
			strings.EqualFold(a, in.FIGI) ||
			strings.EqualFold(a, in.RawID) ||
			(tickerClass != "" && strings.EqualFold(a, tickerClass)) {
			return nil
		}
	}
	return newViolation("allowed_instruments",
		fmt.Sprintf("instrument %s (%s) is not in the policy allowlist", in.UID, in.RawID),
		"instrument_uid", in.UID, "raw_id", in.RawID)
}

func (p *Policy) checkNotional(in OrderIntent) *Violation {
	if p.MaxNotionalPerOrder == "" {
		return nil
	}
	// Market and bestprice orders carry no price at this local, pre-network
	// validation stage, so their notional cannot be computed without a quote
	// (GetOrderPrice / last price) — a network call the policy layer is
	// deliberately free of. max_notional therefore does NOT apply to market
	// orders; allow_market_orders is their guardrail (see the field docs and
	// README). This is a documented bypass, not an oversight (finding F11).
	if in.Price == nil {
		return nil
	}
	// Fail closed when the metadata needed to enforce the cap is missing: a cap
	// that compares across currencies (unknown instrument currency) or silently
	// under-counts (unknown lot size) is worse than no cap — it lets a breaching
	// order through (finding F12).
	if in.Currency == "" {
		return newViolation("max_notional_per_order",
			"cannot enforce max_notional_per_order: the instrument's currency is unknown",
			"policy_currency", p.NotionalCurrency)
	}
	if p.NotionalCurrency != "" && !strings.EqualFold(p.NotionalCurrency, in.Currency) {
		return newViolation("max_notional_per_order",
			fmt.Sprintf("order currency %s differs from policy notional currency %s; cannot enforce notional cap", in.Currency, p.NotionalCurrency),
			"order_currency", in.Currency, "policy_currency", p.NotionalCurrency)
	}
	if in.LotSize <= 0 {
		return newViolation("max_notional_per_order",
			"cannot enforce max_notional_per_order: the instrument's lot size is unknown",
			"policy_currency", p.NotionalCurrency)
	}

	// lots * lotSize can overflow int64 for extreme lot counts, which would wrap
	// to a small or negative value and silently slip a huge order under the cap.
	// Do the multiplication in big.Int so the notional is always exact (F11).
	lotTotal := new(big.Int).Mul(big.NewInt(in.Lots), big.NewInt(int64(in.LotSize)))
	notional := quotationRat(in.Price)
	notional.Mul(notional, new(big.Rat).SetInt(lotTotal))

	limitQ, err := render.ParseQuotation(p.MaxNotionalPerOrder)
	if err != nil {
		// Load already validated this; treat a late parse failure as no cap
		// rather than blocking on an internal inconsistency.
		return nil
	}
	limit := quotationRat(limitQ)

	if notional.Cmp(limit) > 0 {
		return newViolation("max_notional_per_order",
			fmt.Sprintf("order notional %s %s exceeds max_notional_per_order=%s %s",
				ratString(notional), strings.ToUpper(orDefault(in.Currency, p.NotionalCurrency)),
				p.MaxNotionalPerOrder, strings.ToUpper(p.NotionalCurrency)),
			"limit", p.MaxNotionalPerOrder, "actual", ratString(notional), "currency", p.NotionalCurrency)
	}
	return nil
}

// CheckOpenOrders enforces MaxOpenOrders against a count the caller obtained
// from a read (GetOrders). It is separate from CheckLocal/CheckResolved
// because it inherently needs a network fact; it still runs before the
// mutation, so a breach fails with exit 2 and no order is placed.
func (p *Policy) CheckOpenOrders(open int) *Violation {
	if p == nil || p.MaxOpenOrders <= 0 {
		return nil
	}
	if open >= p.MaxOpenOrders {
		return newViolation("max_open_orders",
			fmt.Sprintf("account has %d open orders, at or above max_open_orders=%d", open, p.MaxOpenOrders),
			"limit", fmt.Sprint(p.MaxOpenOrders), "actual", fmt.Sprint(open))
	}
	return nil
}

func quotationRat(q *investapi.Quotation) *big.Rat {
	// units + nano/1e9, sign shared across the pair (contract guarantee).
	r := new(big.Rat).SetInt64(q.GetUnits())
	r.Add(r, big.NewRat(int64(q.GetNano()), 1_000_000_000))
	return r
}

// ratString renders a rational as a plain decimal with up to 9 fractional
// digits, trailing zeros trimmed — matching render.DecimalString's style.
func ratString(r *big.Rat) string {
	s := r.FloatString(9)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return s
}

func orDefault(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}
