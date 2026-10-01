// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"sort"
)

// CaptureID is which selector matched: its position in the slice given
// to NewMatcher.
type CaptureID = int

type nodeID = int

const rootNode nodeID = 0

type propChild struct {
	name  string
	child nodeID
}

type indexChild struct {
	index int
	child nodeID
}

// trieNode holds the selectors' next steps below one position.
type trieNode struct {
	properties []propChild
	indexes    []indexChild
	eachIndex  nodeID // -1 for none
	eachMember nodeID // -1 for none
	// terminals are the selectors that end here, in id order.
	terminals []CaptureID
}

func newTrieNode() trieNode { return trieNode{eachIndex: -1, eachMember: -1} }

// level is one open container. Levels are reused across the run, so a
// member costs no allocation.
type level struct {
	object bool
	// started is the values started in this container so far; the
	// current or last one is at index started-1.
	started int
	// key is the current member's key, for objects.
	key string
	// inValue: between a key and its value.
	inValue bool
	// start, end: the trie nodes that named this container,
	// nodeStack[start:end].
	start, end int
}

// HitKind is what one event did to the document's structure.
type HitKind uint8

const (
	// HitKey is a member name; the value follows.
	HitKey HitKind = iota
	// HitStart is a container that began.
	HitStart
	// HitScalar is a whole scalar value: it began and completed here.
	HitScalar
	// HitClose is a container that completed.
	HitClose
	// HitEnd is the document completing.
	HitEnd
)

// Hit is the matcher's answer for one event.
//
// Begins is how many captures begin with this event: their ids are
// Matcher.Begins, valid until the next event; zero unless Kind is
// HitStart or HitScalar. Depth is, for HitStart and HitScalar, the number
// of containers enclosing the value; for HitClose, the number enclosing
// the container that closed. The value that began at a depth completes
// at the same depth, which is how a stage pairs the two without keeping
// a path.
type Hit struct {
	Kind   HitKind
	Begins int
	Depth  int
}

// Matcher recognizes every selector of a set over one event stream, in
// one pass, with no allocation per event: the selectors are compiled into
// one trie of steps, and the matcher walks the document with a stack of
// open containers and, per level, the set of trie nodes that named that
// container. It also validates the protocol as it goes, so every stage
// downstream of it can trust the sequence; a malformed stream is a
// PROTOCOL_ORDER_ERROR. A concrete Path is built only on request.
type Matcher struct {
	nodes     []trieNode
	levels    []level
	depth     int
	nodeStack []nodeID
	begins    []CaptureID
	rootDone  bool
	ended     bool
}

// NewMatcher compiles the selectors; their positions are the capture ids.
func NewMatcher(selectors []Selector) *Matcher {
	nodes := []trieNode{newTrieNode()}
	for id, sel := range selectors {
		at := rootNode
		for _, step := range sel.steps {
			at = trieChild(&nodes, at, step)
		}
		nodes[at].terminals = append(nodes[at].terminals, id)
	}
	return &Matcher{nodes: nodes, nodeStack: make([]nodeID, 0, 8)}
}

// Depth is the number of open containers right now.
func (m *Matcher) Depth() int { return m.depth }

// Ended reports whether End has been seen.
func (m *Matcher) Ended() bool { return m.ended }

// Path is the concrete path of the value at depth enclosing containers:
// the current position of each of those containers. Asked at a HitStart
// or HitScalar it names the value; asked at a HitClose it names the
// container that closed. It allocates, so it is for deliveries and
// failures, not for every event.
func (m *Matcher) Path(depth int) Path {
	if depth > m.depth {
		depth = m.depth
	}
	p := make(Path, 0, depth)
	for _, lv := range m.levels[:depth] {
		if lv.object {
			p = append(p, KeySegment(lv.key))
		} else {
			i := lv.started - 1
			if i < 0 {
				i = 0
			}
			p = append(p, IndexSegment(i))
		}
	}
	return p
}

// Begins is the captures that began with the last event, in id order.
func (m *Matcher) Begins() []CaptureID { return m.begins }

// Event takes one event.
func (m *Matcher) Event(ev Event) (Hit, *Fail) {
	if m.ended {
		return Hit{}, ProtocolFail("an event arrived after the document ended")
	}
	switch ev.Kind {
	case Key:
		if m.depth == 0 || !m.levels[m.depth-1].object {
			return Hit{}, ProtocolFail("a key arrived outside an object")
		}
		lv := &m.levels[m.depth-1]
		if lv.inValue {
			return Hit{}, ProtocolFail("a key arrived where a value was expected")
		}
		lv.key = ev.Text
		lv.inValue = true
		m.begins = m.begins[:0]
		return Hit{Kind: HitKey, Depth: m.depth}, nil
	case ObjectStart, ArrayStart:
		depth := m.depth
		start, f := m.beginValue()
		if f != nil {
			return Hit{}, f
		}
		end := len(m.nodeStack)
		lv := level{object: ev.Kind == ObjectStart, start: start, end: end}
		if depth == len(m.levels) {
			m.levels = append(m.levels, lv)
		} else {
			m.levels[depth] = lv
		}
		m.depth++
		return Hit{Kind: HitStart, Begins: len(m.begins), Depth: depth}, nil
	case ObjectEnd, ArrayEnd:
		if m.depth == 0 {
			return Hit{}, ProtocolFail("a container ended that had not started")
		}
		lv := &m.levels[m.depth-1]
		wantObject := ev.Kind == ObjectEnd
		if lv.object != wantObject {
			if wantObject {
				return Hit{}, ProtocolFail("an object ended inside an array")
			}
			return Hit{}, ProtocolFail("an array ended inside an object")
		}
		if lv.inValue {
			return Hit{}, ProtocolFail("an object ended after a key without its value")
		}
		start := lv.start
		m.depth--
		m.nodeStack = m.nodeStack[:start]
		m.completeValue()
		m.begins = m.begins[:0]
		return Hit{Kind: HitClose, Depth: m.depth}, nil
	case End:
		if m.depth > 0 {
			return Hit{}, ProtocolFail("the document ended inside a container")
		}
		if !m.rootDone {
			return Hit{}, ProtocolFail("the document ended before its root value")
		}
		m.ended = true
		m.begins = m.begins[:0]
		return Hit{Kind: HitEnd}, nil
	default:
		depth := m.depth
		start, f := m.beginValue()
		if f != nil {
			return Hit{}, f
		}
		m.nodeStack = m.nodeStack[:start]
		m.completeValue()
		return Hit{Kind: HitScalar, Begins: len(m.begins), Depth: depth}, nil
	}
}

// beginValue checks that a value is allowed at the current position,
// pushes the trie nodes naming it onto the node stack, and fills begins.
// It returns where on the node stack the value's nodes start.
func (m *Matcher) beginValue() (int, *Fail) {
	base := 0
	if m.depth == 0 {
		if m.rootDone {
			return 0, ProtocolFail("a second root value arrived")
		}
		m.nodeStack = append(m.nodeStack[:0], rootNode)
	} else {
		lv := &m.levels[m.depth-1]
		if lv.object && !lv.inValue {
			return 0, ProtocolFail("a value arrived inside an object without a key")
		}
		index := lv.started
		lv.started++
		m.nodeStack = m.nodeStack[:lv.end]
		for at := lv.start; at < lv.end; at++ {
			node := &m.nodes[m.nodeStack[at]]
			if lv.object {
				if node.eachMember >= 0 {
					m.nodeStack = append(m.nodeStack, node.eachMember)
				}
				for _, pc := range node.properties {
					if pc.name == lv.key {
						m.nodeStack = append(m.nodeStack, pc.child)
						break
					}
				}
			} else {
				if node.eachIndex >= 0 {
					m.nodeStack = append(m.nodeStack, node.eachIndex)
				}
				for _, ic := range node.indexes {
					if ic.index == index {
						m.nodeStack = append(m.nodeStack, ic.child)
						break
					}
				}
			}
		}
		base = lv.end
	}
	m.begins = m.begins[:0]
	for _, node := range m.nodeStack[base:] {
		m.begins = append(m.begins, m.nodes[node].terminals...)
	}
	if len(m.begins) > 1 {
		sort.Ints(m.begins)
	}
	return base, nil
}

// completeValue marks the value at the current position complete.
func (m *Matcher) completeValue() {
	if m.depth == 0 {
		m.rootDone = true
	} else {
		m.levels[m.depth-1].inValue = false
	}
}

// trieChild is the child of at for step, created on first use.
func trieChild(nodes *[]trieNode, at nodeID, step Step) nodeID {
	n := &(*nodes)[at]
	switch step.Kind {
	case StepProperty:
		for _, pc := range n.properties {
			if pc.name == step.Name {
				return pc.child
			}
		}
	case StepIndex:
		for _, ic := range n.indexes {
			if ic.index == step.Index {
				return ic.child
			}
		}
	case StepEachIndex:
		if n.eachIndex >= 0 {
			return n.eachIndex
		}
	case StepEachMember:
		if n.eachMember >= 0 {
			return n.eachMember
		}
	}
	c := len(*nodes)
	*nodes = append(*nodes, newTrieNode())
	n = &(*nodes)[at]
	switch step.Kind {
	case StepProperty:
		n.properties = append(n.properties, propChild{name: step.Name, child: c})
	case StepIndex:
		n.indexes = append(n.indexes, indexChild{index: step.Index, child: c})
	case StepEachIndex:
		n.eachIndex = c
	case StepEachMember:
		n.eachMember = c
	}
	return c
}
