// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"fmt"
	"strconv"
)

// SelectedRecorder is a route sink that keeps every match.
type SelectedRecorder struct {
	Matches []Selected
	Ended   bool
}

// Selected records s.
func (r *SelectedRecorder) Selected(s Selected) (Flow, *Fail) {
	r.Matches = append(r.Matches, s)
	return Continue, nil
}

// End records that the document ended.
func (r *SelectedRecorder) End() (Flow, *Fail) {
	r.Ended = true
	return Continue, nil
}

// FnRoute is a route sink made of a function; End is a no-op.
type FnRoute func(s Selected) (Flow, *Fail)

// Selected calls the function.
func (f FnRoute) Selected(s Selected) (Flow, *Fail) { return f(s) }

// End does nothing.
func (f FnRoute) End() (Flow, *Fail) { return Continue, nil }

type activeCapture struct {
	id      CaptureID
	builder *DatumBuilder
	// depth the value began at, from which the matcher spells the value's
	// path when a delivery or a failure needs one.
	depth int
}

type observing struct {
	id    CaptureID
	depth int
}

// Router recognizes every capture in one pass and delivers completed
// matches to a RouteSink. A Materialize capture is built into a Datum
// under a byte budget and delivered when its value completes; an Observe
// capture delivers only the path, at the value's end, and retains
// nothing. Materialized captures may not nest or coincide: the router
// holds at most one value at a time, which is what makes its retention
// one selected scope rather than a document. That is checked at
// construction, conservatively, with Selector.MayOverlap, and again at
// run time.
type Router struct {
	specs           []CaptureSpec
	matcher         *Matcher
	downstream      RouteSink
	beginner        RouteBeginner
	active          *activeCapture
	observing       []observing
	maxDepth        int
	maxCaptureBytes int
	duplicates      Duplicates
	// metrics is for the capture accounting only; the source counts
	// events, keys and scalars through Guarded, so a chain sharing one
	// Metrics counts each once.
	metrics *Metrics
	ended   bool
}

// NewRouter builds a router; two specs that may overlap are refused with
// CAPTURE_OVERLAP_UNSUPPORTED unless both observe.
func NewRouter(specs []CaptureSpec, limits Limits, duplicates Duplicates, metrics *Metrics, downstream RouteSink) (*Router, *Fail) {
	for i, a := range specs {
		for _, b := range specs[i+1:] {
			bothObserve := a.Mode == Observe && b.Mode == Observe
			if !bothObserve && a.Selector.MayOverlap(b.Selector) {
				return nil, NewFail(CodeCaptureOverlapUnsupported, fmt.Sprintf(
					"captures %s (%s) and %s (%s) may select overlapping scopes; only observed captures may overlap",
					strconv.Quote(a.Tag), a.Selector, strconv.Quote(b.Tag), b.Selector))
			}
		}
	}
	return newRouterUnchecked(specs, limits, duplicates, metrics, downstream), nil
}

func newRouterUnchecked(specs []CaptureSpec, limits Limits, duplicates Duplicates, metrics *Metrics, downstream RouteSink) *Router {
	selectors := make([]Selector, len(specs))
	for i, s := range specs {
		selectors[i] = s.Selector
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	beginner, _ := downstream.(RouteBeginner)
	return &Router{
		specs:           append([]CaptureSpec(nil), specs...),
		matcher:         NewMatcher(selectors),
		downstream:      downstream,
		beginner:        beginner,
		maxDepth:        limits.MaxDepth,
		maxCaptureBytes: limits.MaxCaptureBytes,
		duplicates:      duplicates,
		metrics:         metrics,
	}
}

// Specs is the router's captures.
func (r *Router) Specs() []CaptureSpec { return r.specs }

// Downstream is the route sink.
func (r *Router) Downstream() RouteSink { return r.downstream }

// Ended reports whether End has been delivered.
func (r *Router) Ended() bool { return r.ended }

func (r *Router) begin(id CaptureID, depth int) *Fail {
	spec := &r.specs[id]
	if r.beginner != nil {
		if f := r.beginner.Began(id, spec.Tag); f != nil {
			if f.Path == "" {
				f.Path = r.matcher.Path(depth).String()
			}
			return f
		}
	}
	if spec.Mode == Observe {
		r.observing = append(r.observing, observing{id: id, depth: depth})
		return nil
	}
	if r.active != nil {
		p := r.matcher.Path(depth).String()
		return NewFail(CodeCaptureOverlapUnsupported, fmt.Sprintf(
			"capture %s began at %s while capture %s was still being materialized",
			strconv.Quote(spec.Tag), p, strconv.Quote(r.specs[r.active.id].Tag))).AtPath(p)
	}
	budget := Budget{Bytes: r.maxCaptureBytes, Name: "max_capture_bytes"}
	if spec.Budget != nil {
		budget = *spec.Budget
	}
	r.active = &activeCapture{id: id, builder: NewDatumBuilder(budget.Bytes, budget.Name, r.duplicates), depth: depth}
	return nil
}

func (r *Router) deliver(id CaptureID, path Path, value *Datum) (Flow, *Fail) {
	return r.downstream.Selected(Selected{ID: id, Tag: r.specs[id].Tag, Path: path, Value: value})
}

// Event takes one source event.
func (r *Router) Event(ev Event) (Flow, *Fail) {
	hit, f := r.matcher.Event(ev)
	if f != nil {
		return Continue, f
	}
	if hit.Kind == HitStart && hit.Depth+1 > r.maxDepth {
		p := r.matcher.Path(hit.Depth).String()
		return Continue, LimitFail("max_depth", uint64(r.maxDepth),
			fmt.Sprintf("a container at %s is nested deeper than %d", p, r.maxDepth)).AtPath(p)
	}
	for k := 0; k < hit.Begins; k++ {
		if f := r.begin(r.matcher.Begins()[k], hit.Depth); f != nil {
			return Continue, f
		}
	}
	if a := r.active; a != nil {
		if f := a.builder.Event(ev); f != nil {
			// The builder reports no position; the value's path is
			// spelled here, only now that something went wrong.
			return Continue, f.AtPath(r.matcher.Path(a.depth).String())
		}
		if a.builder.Finished() {
			bytes := uint64(a.builder.Bytes())
			value, _ := a.builder.Take()
			r.active = nil
			p := r.matcher.Path(hit.Depth)
			r.metrics.Capture(bytes)
			flow, f := r.deliver(a.id, p, &value)
			r.metrics.Release(bytes)
			if f != nil {
				return Continue, f
			}
			if flow == Stop {
				return Stop, nil
			}
		}
	}
	if hit.Kind == HitScalar || hit.Kind == HitClose {
		for n := len(r.observing); n > 0 && r.observing[n-1].depth == hit.Depth; n = len(r.observing) {
			id := r.observing[n-1].id
			r.observing = r.observing[:n-1]
			flow, f := r.deliver(id, r.matcher.Path(hit.Depth), nil)
			if f != nil {
				return Continue, f
			}
			if flow == Stop {
				return Stop, nil
			}
		}
	}
	if hit.Kind == HitEnd {
		r.ended = true
		return r.downstream.End()
	}
	return Continue, nil
}
