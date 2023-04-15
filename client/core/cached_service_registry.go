package core

import (
	"github.com/Nextsummer/micro/pkg/queue"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
)

var cachedServiceRegistryOnce sync.Once
var cachedServiceRegistry *CachedServiceRegistry

type CachedServiceRegistry struct {
	serviceRegistry cmap.ConcurrentMap[string, *queue.Array[ServiceInstanceAddress]]
}

func GetCachedServiceRegistryInstance() *CachedServiceRegistry {
	cachedServiceRegistryOnce.Do(func() {
		cachedServiceRegistry = &CachedServiceRegistry{cmap.New[*queue.Array[ServiceInstanceAddress]]()}
	})
	return cachedServiceRegistry
}

// Determine whether the service name is cached
func (c CachedServiceRegistry) isCached(serviceName string) bool {
	return c.serviceRegistry.Has(serviceName)
}

// Cache the list of service instance addresses
func (c *CachedServiceRegistry) cache(serviceName string, serviceInstanceAddress *queue.Array[ServiceInstanceAddress]) {
	c.serviceRegistry.Set(serviceName, serviceInstanceAddress)
}
