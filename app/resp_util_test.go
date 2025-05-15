package main

func EqualRESPValue(a, b RESPValue) bool {
	if a.Type != b.Type {
		return false
	}

	switch a.Type {
	case SimpleString, Error, BulkString:
		return a.String == b.String
	case Integer:
		return a.Integer == b.Integer
	case Array:
		if len(a.Array) != len(b.Array) {
			return false
		}
		for i := range a.Array {
			if !EqualRESPValue(a.Array[i], b.Array[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
