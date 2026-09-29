package proxypool

import (
	"sync"
)

// memoryCache provides a thread-safe, passive in-memory implementation of CacheSource.
type memoryCache struct {
	mu   sync.RWMutex
	data map[string]ProxyState
}

// newMemoryCache initializes an in-memory cache backend.
func newMemoryCache() *memoryCache {
	return &memoryCache{
		data: make(map[string]ProxyState),
	}
}

func (m *memoryCache) Get(url string) (ProxyState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, exists := m.data[url]
	return state, exists
}

func (m *memoryCache) Set(state ProxyState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[state.URL] = state
}

func (m *memoryCache) All() []ProxyState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := make([]ProxyState, 0, len(m.data))
	for _, v := range m.data {
		list = append(list, v)
	}
	return list
}

func (m *memoryCache) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]ProxyState)
}
