package extractor

type stringPtr struct {
	ptr *string
}

type int32Ptr struct {
	ptr *int32
}

type NullScanner struct {
	strings map[string]*stringPtr
	ints    map[string]*int32Ptr
}

func NewNullScanner() *NullScanner {
	return &NullScanner{
		strings: make(map[string]*stringPtr),
		ints:    make(map[string]*int32Ptr),
	}
}

func (ns *NullScanner) String(name string) **string {
	if ns.strings[name] == nil {
		ns.strings[name] = &stringPtr{ptr: nil}
	}

	return &ns.strings[name].ptr
}

func (ns *NullScanner) Int32(name string) **int32 {
	if ns.ints[name] == nil {
		ns.ints[name] = &int32Ptr{ptr: nil}
	}

	return &ns.ints[name].ptr
}

func (ns *NullScanner) GetString(name string) string {
	sp := ns.strings[name]
	if sp == nil || sp.ptr == nil {
		return ""
	}

	return *sp.ptr
}

func (ns *NullScanner) GetInt(name string) *int {
	ip := ns.ints[name]
	if ip == nil || ip.ptr == nil {
		return nil
	}

	val := int(*ip.ptr)

	return &val
}
