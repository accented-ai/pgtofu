package schema

import (
	"strconv"
	"unicode/utf8"
)

// IdentifierAllocator allocates names that remain unique after PostgreSQL
// truncates them to NAMEDATALEN-1 bytes.
type IdentifierAllocator struct {
	used map[string]struct{}
}

// NewIdentifierAllocator reserves existing names in a new identifier namespace.
func NewIdentifierAllocator(reserved ...string) *IdentifierAllocator {
	allocator := &IdentifierAllocator{
		used: make(map[string]struct{}, len(reserved)),
	}

	for _, identifier := range reserved {
		allocator.used[TruncateIdentifier(identifier)] = struct{}{}
	}

	return allocator
}

// Allocate returns and reserves the first available truncated identifier.
func (a *IdentifierAllocator) Allocate(identifier string) string {
	for sequence := 0; ; sequence++ {
		suffix := ""
		if sequence > 0 {
			suffix = strconv.Itoa(sequence)
		}

		candidate := truncateIdentifier(identifier, MaxIdentifierLength-len(suffix)) + suffix
		if _, exists := a.used[candidate]; exists {
			continue
		}

		a.used[candidate] = struct{}{}

		return candidate
	}
}

func truncateIdentifier(identifier string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}

	if len(identifier) <= maxBytes {
		return identifier
	}

	for maxBytes > 0 && !utf8.RuneStart(identifier[maxBytes]) {
		maxBytes--
	}

	return identifier[:maxBytes]
}
