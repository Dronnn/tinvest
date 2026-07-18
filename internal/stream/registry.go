package stream

import "sync"

// Registry stores the authoritative active subscription requests. Re-adding
// a key replaces its request in place, so reconnect replay never duplicates a
// request batch or grows an unbounded send history.
type Registry[T any] struct {
	mu       sync.RWMutex
	order    []string
	requests map[string]registryEntry[T]
}

func NewRegistry[T any]() *Registry[T] {
	return &Registry[T]{requests: make(map[string]registryEntry[T])}
}

type registryEntry[T any] struct {
	request      *T
	subscription bool
}

// Add inserts or replaces one active subscription.
func (r *Registry[T]) Add(key string, request *T) {
	r.add(key, request, true)
}

// AddControl inserts or replaces a replayable setup request that must not be
// included in consumer-visible subscription counts.
func (r *Registry[T]) AddControl(key string, request *T) {
	r.add(key, request, false)
}

func (r *Registry[T]) add(key string, request *T, subscription bool) {
	if r == nil || key == "" || request == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.requests[key]; !exists {
		r.order = append(r.order, key)
	}
	r.requests[key] = registryEntry[T]{request: request, subscription: subscription}
}

// Remove deletes an active subscription.
func (r *Registry[T]) Remove(key string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.requests[key]; !exists {
		return
	}
	delete(r.requests, key)
	for index, existing := range r.order {
		if existing == key {
			r.order = append(r.order[:index], r.order[index+1:]...)
			break
		}
	}
}

// Snapshot returns active requests in stable insertion order.
func (r *Registry[T]) Snapshot() []*T {
	requests, _ := r.ReplaySnapshot()
	return requests
}

// ReplaySnapshot returns the stable replay list and its subscription request
// batch count from the same registry state. Replayable control requests are
// included in the list but excluded from the count.
func (r *Registry[T]) ReplaySnapshot() ([]*T, int) {
	if r == nil {
		return nil, 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	requests := make([]*T, 0, len(r.order))
	subscriptionCount := 0
	for _, key := range r.order {
		entry := r.requests[key]
		if entry.request == nil {
			continue
		}
		requests = append(requests, entry.request)
		if entry.subscription {
			subscriptionCount++
		}
	}
	return requests, subscriptionCount
}

// SubscriptionCount returns the number of subscription request batches,
// excluding replayable control requests.
func (r *Registry[T]) SubscriptionCount() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, entry := range r.requests {
		if entry.request != nil && entry.subscription {
			count++
		}
	}
	return count
}
