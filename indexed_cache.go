package proxypool

// indexedCacheSource keeps the consumer index synchronized with passive cache mutations.
// The underlying CacheSource remains responsible only for storage.
type indexedCacheSource struct {
	inner CacheSource
	index *aliveIndex
}

func (c *indexedCacheSource) Get(url string) (ProxyState, bool) { return c.inner.Get(url) }
func (c *indexedCacheSource) Set(state ProxyState) {
	c.inner.Set(state)
	c.index.upsert(state)
}
func (c *indexedCacheSource) All() []ProxyState { return c.inner.All() }
func (c *indexedCacheSource) Delete(url string) {
	c.inner.Delete(url)
	c.index.delete(url)
}
func (c *indexedCacheSource) Clear() {
	c.inner.Clear()
	c.index.replace(nil)
}
