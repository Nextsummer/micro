package utils

const (
	SlotsCount = 16384
)

// RouteSlot Route the service to the slot
func RouteSlot(serviceName string) int32 {
	hashCode := StringHashCode(serviceName)
	slot := hashCode % SlotsCount
	if slot == 0 {
		slot++
	}
	return slot
}
