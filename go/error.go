// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"fmt"
	"strings"

	tabnas "github.com/tabnas/parser/go"
)

// FailFromTabnas is the engine's own error as an input failure carrying
// its code, position and report.
func FailFromTabnas(e *tabnas.TabnasError) *Fail {
	f := NewFail(CodeInputInvalid, fmt.Sprintf("%s: %s", e.Code, strings.TrimRight(e.Detail, " \t\r\n")))
	if e.Row > 0 {
		f.Row = uint64(e.Row)
		f.Column = uint64(e.Col)
	}
	return f
}

// failFromError is FailFromTabnas for any error the engine returns: a
// *TabnasError, or anything else as INPUT_INVALID with its text.
func failFromError(err error) *Fail {
	if te, ok := err.(*tabnas.TabnasError); ok {
		return FailFromTabnas(te)
	}
	return InputFail(err.Error())
}
