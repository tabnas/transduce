// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// isJSONNumber reports whether s is a number by RFC 8259's grammar:
// -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?. Only such a lexeme is
// kept, so a renderer can write it as it stands.
func isJSONNumber(s string) bool {
	i, n := 0, len(s)
	digits := func() int {
		start := i
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		return i - start
	}
	if i < n && s[i] == '-' {
		i++
	}
	switch {
	case i < n && s[i] == '0':
		i++
	case i < n && s[i] >= '1' && s[i] <= '9':
		digits()
	default:
		return false
	}
	if i < n && s[i] == '.' {
		i++
		if digits() == 0 {
			return false
		}
	}
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < n && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if digits() == 0 {
			return false
		}
	}
	return i == n
}
