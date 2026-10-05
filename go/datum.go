// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"math"
)

// DatumFromValue converts an engine value. Undefined is null, as the
// engine serializes it; the metadata wrappers (Text, MapRef, ListRef)
// unwrap; a plain Go map, which has no order, gives its members in
// sorted key order.
func DatumFromValue(v any) Datum {
	rec := datumRecorder{b: *NewDatumBuilder(math.MaxInt, "max_capture_bytes", LastWins)}
	_, _ = walkValueOrdered(v, &rec, nil)
	d, _ := rec.b.Take()
	return d
}

type datumRecorder struct{ b DatumBuilder }

func (r *datumRecorder) Event(ev Event) (Flow, *Fail) {
	if f := r.b.Event(ev); f != nil {
		return Continue, f
	}
	return Continue, nil
}
