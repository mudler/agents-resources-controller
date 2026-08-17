// Package selector parses and evaluates device selectors such as
// "vendor=nvidia,vram>=40G". It deliberately depends on nothing else in the
// project so it can be reasoned about and tested on its own.
package selector

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrEmpty = errors.New("selector is empty")

type op string

const (
	opEq  op = "="
	opNe  op = "!="
	opGte op = ">="
	opLte op = "<="
)

type term struct {
	key   string
	op    op
	value string
}

// Selector is a conjunction of terms: every term must match.
type Selector struct {
	terms []term
}

// Parse reads a comma-separated conjunction. Order is preserved so String()
// round-trips predictably.
func Parse(s string) (Selector, error) {
	var sel Selector
	if strings.TrimSpace(s) == "" {
		return sel, ErrEmpty
	}
	for _, raw := range strings.Split(s, ",") {
		part := strings.TrimSpace(raw)
		if part == "" {
			return Selector{}, fmt.Errorf("empty term in selector %q", s)
		}
		t, err := parseTerm(part)
		if err != nil {
			return Selector{}, err
		}
		sel.terms = append(sel.terms, t)
	}
	return sel, nil
}

// Longest operators first: ">=" must win over "=".
var operators = []op{opNe, opGte, opLte, opEq}

func parseTerm(part string) (term, error) {
	for _, o := range operators {
		key, value, found := strings.Cut(part, string(o))
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return term{}, fmt.Errorf("term %q needs a key and a value", part)
		}
		// Reject keys or values containing operator characters to catch typos like "a=b=c"
		if containsOperatorChar(key) || containsOperatorChar(value) {
			return term{}, fmt.Errorf("term %q contains embedded operator characters", part)
		}
		return term{key: key, op: o, value: value}, nil
	}
	return term{}, fmt.Errorf("term %q has no operator (expected one of =, !=, >=, <=)", part)
}

// containsOperatorChar reports whether s contains any operator character: =, !, <, >
func containsOperatorChar(s string) bool {
	for _, ch := range s {
		if ch == '=' || ch == '!' || ch == '<' || ch == '>' {
			return true
		}
	}
	return false
}

func (s Selector) String() string {
	parts := make([]string, 0, len(s.terms))
	for _, t := range s.terms {
		parts = append(parts, t.key+string(t.op)+t.value)
	}
	return strings.Join(parts, ",")
}

// Match reports whether every term holds for these labels. A term whose key
// is absent never matches — including "!=", because an absent label is not
// proof that the device differs, and handing out a device on the strength of
// a fact we do not have is the mistake this system exists to avoid.
func (s Selector) Match(labels map[string]string) bool {
	for _, t := range s.terms {
		have, ok := labels[t.key]
		if !ok {
			return false
		}
		if !t.matches(have) {
			return false
		}
	}
	return true
}

func (t term) matches(have string) bool {
	if a, b, ok := numeric(have, t.value); ok {
		switch t.op {
		case opEq:
			return a == b
		case opNe:
			return a != b
		case opGte:
			return a >= b
		case opLte:
			return a <= b
		}
		return false
	}
	// Ordered comparison where exactly ONE side is a quantity is not an
	// ordering question at all — it is a question that cannot be answered, so
	// it does not match. Lexicographic ordering of two non-quantities stays
	// (see below): `model>=a100` matching `h100` is a documented, intended
	// use.
	//
	// The asymmetric case is not hypothetical. nvidia-smi reports "[N/A]" for
	// total memory on a GB10 (unified memory) and on a Thor, so both boxes
	// carry vram=[N/A]M. Comparing that against the quantity 40G fell through
	// to strings, where '[' (0x5B) sorts after '4' (0x34) — so `vram>=40G`
	// matched BOTH, and an agent asking for 80G of VRAM was scheduled onto
	// hardware whose VRAM is unknown. Same rule as the absent-key case above:
	// a fact we do not have is not evidence the device qualifies.
	if t.op == opGte || t.op == opLte {
		_, haveQty := parseQuantity(have)
		_, wantQty := parseQuantity(t.value)
		if haveQty != wantQty {
			return false
		}
	}
	switch t.op {
	case opEq:
		return have == t.value
	case opNe:
		return have != t.value
	case opGte:
		return have >= t.value
	case opLte:
		return have <= t.value
	}
	return false
}

// numeric reports both sides as numbers when both parse, so "vram>=40G"
// compares 80G as larger and "81920M" as larger still.
func numeric(a, b string) (float64, float64, bool) {
	x, okA := parseQuantity(a)
	y, okB := parseQuantity(b)
	return x, y, okA && okB
}

var suffixes = map[byte]float64{
	'K': 1 << 10,
	'M': 1 << 20,
	'G': 1 << 30,
	'T': 1 << 40,
}

func parseQuantity(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	mult := 1.0
	last := s[len(s)-1]
	if m, ok := suffixes[last&^0x20]; ok { // accept upper and lower case
		mult = m
		s = s[:len(s)-1]
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, false
	}
	return v * mult, true
}
