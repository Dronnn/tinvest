package stream

import "sync"

// Registry stores the authoritative active subscription requests. Re-adding
// a key replaces its request in place, so reconnect replay never duplicates a
// logical subscription or grows an unbounded send history.
type Registry[T any] struct {
	mu       sync.RWMutex
	order    []string
	requests map[string]*T
}

func NewRegistry[T any]() *Registry[T] {
	return &Registry[T]{requests: make(map[string]*T)}
}

// Add inserts or replaces one active subscription.
func (r *Registry[T]) Add(key string, request *T) {
	if r == nil || key == "" || request == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.requests[key]; !exists {
		r.order = append(r.order, key)
	}
	r.requests[key] = request
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
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	requests := make([]*T, 0, len(r.order))
	for _, key := range r.order {
		if request := r.requests[key]; request != nil {
			requests = append(requests, request)
		}
	}
	return requests
}
