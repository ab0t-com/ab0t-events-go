package events

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// PayloadFilter narrows delivery by a value inside the event payload, on top of
// the event-type filter. The JSON value at dot-path Path must equal Equals
// (compared as a string). It is dependency-free (encoding/json only).
//
// Path is a dot-separated path into nested JSON objects, e.g. "status" or
// "file.owner.id". The value it resolves to is compared to Equals as a string:
//   - a JSON string compares to its literal contents ("active" == "active");
//   - a JSON number compares to its source text (42 == "42", 42.5 == "42.5");
//   - a JSON bool compares to "true"/"false"; JSON null compares to "null".
//
// A missing path, or a value that is an object/array (not a scalar), never
// matches. Subscriptions carry zero or more filters; see Subscription.MatchesPayload.
type PayloadFilter struct {
	Path   string `json:"path"`
	Equals string `json:"equals"`
}

// MatchesPayload reports whether a subscription's payload filters all match the
// given event payload. A subscription with no payload filters matches every
// payload (so this is backward compatible with type-only subscriptions).
// Multiple filters combine with AND: every one must match.
func (s Subscription) MatchesPayload(payload []byte) bool {
	for _, f := range s.PayloadFilters {
		if !f.matches(payload) {
			return false
		}
	}
	return true
}

// matches reports whether the payload's value at f.Path equals f.Equals.
func (f PayloadFilter) matches(payload []byte) bool {
	got, ok := lookupJSONString(payload, f.Path)
	return ok && got == f.Equals
}

// lookupJSONString resolves a dot-path into a JSON document and returns the value
// at that path as a comparable string, plus whether it was found and is a scalar.
func lookupJSONString(payload []byte, path string) (string, bool) {
	if len(payload) == 0 || path == "" {
		return "", false
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber() // keep numbers exact (42 stays "42", not "4.2e+01")
	var cur any
	if err := dec.Decode(&cur); err != nil {
		return "", false
	}
	for _, seg := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return "", false // path descends through a non-object
		}
		cur, ok = obj[seg]
		if !ok {
			return "", false // key absent
		}
	}
	return scalarString(cur)
}

// scalarString renders a decoded JSON scalar as a string for comparison. It
// returns false for objects and arrays, which are not scalar-comparable.
func scalarString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	case bool:
		return strconv.FormatBool(t), true
	case nil:
		return "null", true
	default:
		return "", false
	}
}
