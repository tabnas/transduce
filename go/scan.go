// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Transition is the result of one scan-emit step: the next state and
// what to emit for it.
type Transition[S, O any] struct {
	State   S
	Outputs []O
}

// Stay is a step that emits nothing.
func Stay[S, O any](state S) Transition[S, O] { return Transition[S, O]{State: state} }

// Emit is a step that emits one item.
func Emit[S, O any](state S, output O) Transition[S, O] {
	return Transition[S, O]{State: state, Outputs: []O{output}}
}

// ScanEmit is declarative state evolution over a stream: a pure step
// function takes the state and one item and returns the next state with
// the items to emit; a finish function turns the final state into the
// closing items. The operator owns the state and the iteration; the
// functions own nothing. Feed items with Item, then call Finish exactly
// once when the input completed successfully; never call it after a
// failure or a cancellation.
type ScanEmit[S, I, O any] struct {
	state    S
	hasState bool
	step     func(S, I) (Transition[S, O], *Fail)
	finish   func(S) ([]O, *Fail)
	out      func(O) (Flow, *Fail)
}

// NewScanEmit is the operator over an initial state and its functions.
func NewScanEmit[S, I, O any](
	initial S,
	step func(S, I) (Transition[S, O], *Fail),
	finish func(S) ([]O, *Fail),
	out func(O) (Flow, *Fail),
) *ScanEmit[S, I, O] {
	return &ScanEmit[S, I, O]{state: initial, hasState: true, step: step, finish: finish, out: out}
}

// Item takes one input item. Its outputs go downstream before it returns.
// A step that fails leaves the operator without a state, so nothing
// further is accepted.
func (s *ScanEmit[S, I, O]) Item(item I) (Flow, *Fail) {
	if !s.hasState {
		return Continue, ProtocolFail("scan-emit received an item after it finished")
	}
	s.hasState = false
	t, f := s.step(s.state, item)
	if f != nil {
		return Continue, f
	}
	s.state, s.hasState = t.State, true
	return s.emit(t.Outputs)
}

// Finish ends the input: it emits the closing items.
func (s *ScanEmit[S, I, O]) Finish() (Flow, *Fail) {
	if !s.hasState || s.finish == nil {
		return Continue, ProtocolFail("scan-emit finished twice")
	}
	state, finish := s.state, s.finish
	s.hasState, s.finish = false, nil
	outputs, f := finish(state)
	if f != nil {
		return Continue, f
	}
	return s.emit(outputs)
}

func (s *ScanEmit[S, I, O]) emit(outputs []O) (Flow, *Fail) {
	for _, o := range outputs {
		flow, f := s.out(o)
		if f != nil {
			return Continue, f
		}
		if flow == Stop {
			return Stop, nil
		}
	}
	return Continue, nil
}
