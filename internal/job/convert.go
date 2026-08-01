package job

import "math"

// ToInt32 safely converts an int to int32, clamping to math.MaxInt32 if
// it would overflow. Used for values (retry counts, attempt limits)
// that are always small in practice but pass through a narrowing
// conversion at the proto/gRPC boundary.
func ToInt32(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	if n < math.MinInt32 {
		return math.MinInt32
	}
	// #nosec G115 -- n is bounds-checked above; this conversion cannot
	// overflow. gosec's static analysis doesn't trace the preceding
	// guard, so this is a false positive, not a suppressed real issue.
	return int32(n)
}
