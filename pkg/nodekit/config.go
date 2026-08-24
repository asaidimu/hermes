package nodekit

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
)

// Delete is the Go-side sentinel mirroring the TS DELETE_SYMBOL: a patch value
// that signals the engine to delete the target path instead of setting it.
type deleteMarker struct{}

// Delete is the sentinel value used in patches returned by node run functions.
var Delete = &deleteMarker{}

func isDelete(v any) bool {
	_, ok := v.(*deleteMarker)
	return ok
}

// GetNested port of utils getNestedValue: walks a dot-separated path over any
// value, returning the value and whether it was found. A nil/empty path returns
// the object itself.
func GetNested(obj any, path string) (any, bool) {
	if path == "" {
		return obj, true
	}
	current := obj
	for _, part := range strings.Split(path, ".") {
		if current == nil {
			return nil, false
		}
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := m[part]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

// ExpandPatch flattens dotted keys into a nested map, mirroring the TS
// setNestedValue expansion applied to step patches before merging.
func ExpandPatch(flat map[string]any) map[string]any {
	expanded := make(map[string]any)
	for k, v := range flat {
		setNested(expanded, k, v)
	}
	return expanded
}

func setNested(obj map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	current := obj
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		next, ok := current[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
}

// ApplyPatch deep-merges a flat (dotted-key) patch into the document, mirroring
// the TS engine: the patch is expanded to a nested object (setNestedValue) and
// merged into state with @core/store merge semantics (arrays replace wholesale,
// objects merge recursively, Delete sentinels remove keys, scalars are replaced).
func ApplyPatch(doc *document.Document, flat map[string]any) error {
	if len(flat) == 0 {
		return nil
	}
	nested := ExpandPatch(flat)
	current := doc.Data()
	merged := MergeMaps(current, nested)
	for k := range nested {
		v, ok := merged[k]
		if !ok {
			if err := doc.Delete(k); err != nil {
				return err
			}
			continue
		}
		if err := doc.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

// MergeMaps port of @core/store merge: deep-merges a patch into an original
// map immutably. Arrays are replaced wholesale; object values merge recursively
// (or replace when the target is not an object); Delete sentinels remove keys.
func MergeMaps(original map[string]any, changes map[string]any) map[string]any {
	result := shallowClone(original)
	type pair struct {
		target map[string]any
		source map[string]any
	}
	stack := []pair{{result, changes}}
	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for key, sv := range p.source {
			if isDelete(sv) {
				delete(p.target, key)
				continue
			}
			if arr, ok := sv.([]any); ok {
				p.target[key] = arr
				continue
			}
			if m, ok := sv.(map[string]any); ok {
				cur, isMap := p.target[key].(map[string]any)
				if !isMap {
					cur = map[string]any{}
				}
				p.target[key] = shallowClone(cur)
				stack = append(stack, pair{p.target[key].(map[string]any), m})
				continue
			}
			p.target[key] = sv
		}
	}
	return result
}

func shallowClone(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Number mirrors JS Number(x): converts values to float64. Returns false when
// the conversion fails (NaN in JS terms).
func Number(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		if err == nil {
			return f, true
		}
		return 0, false
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err == nil {
			return f, true
		}
		return 0, false
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

var (
	anyInterpRe = regexp.MustCompile(`\{\{.+\}\}`)
	tokenRe     = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_$.-]+)\s*\}\}`)
)

// InterpolationContext holds the values available to {{ ... }} templates.
type InterpolationContext struct {
	State     map[string]any
	Resources map[string]any
	Results   map[string]any
}

// InterpolateString port of utils interpolateString: replaces {{ path }} tokens
// with values from state/results/resources. "$res.x" reads resources, "$results.x"
// reads results, "$.x" / "$x" / "x" read state. Missing paths are an error, and
// object values serialize as pretty JSON.
func InterpolateString(template string, ctx InterpolationContext) (string, error) {
	var interpErr error
	out := tokenRe.ReplaceAllStringFunc(template, func(tok string) string {
		m := tokenRe.FindStringSubmatch(tok)
		path := m[1]
		var value any
		var ok bool
		switch {
		case strings.HasPrefix(path, "$res."):
			value, ok = GetNested(ctx.Resources, path[len("$res."):])
		case strings.HasPrefix(path, "$results."):
			value, ok = GetNested(ctx.Results, path[len("$results."):])
		case strings.HasPrefix(path, "$"):
			clean := strings.TrimPrefix(path, "$")
			clean = strings.TrimPrefix(clean, ".")
			if clean == "" {
				value, ok = ctx.State, true
			} else {
				value, ok = GetNested(ctx.State, clean)
			}
		default:
			value, ok = GetNested(ctx.State, path)
		}
		if !ok {
			interpErr = fmt.Errorf("interpolation error: path %q not found in context", path)
			return tok
		}
		strValue := fmt.Sprintf("%v", value)
		if value != nil {
			if _, isObj := value.(map[string]any); isObj {
				b, err := json.MarshalIndent(value, "", "  ")
				if err == nil {
					strValue = string(b)
				}
			} else if _, isArr := value.([]any); isArr {
				b, err := json.MarshalIndent(value, "", "  ")
				if err == nil {
					strValue = string(b)
				}
			}
		}
		return strValue
	})
	if interpErr != nil {
		return "", interpErr
	}
	return out, nil
}

// Interpolate port of utils deepInterpolate: recursively interpolates string
// values containing {{ ... }} while passing all other values through.
func Interpolate(obj any, ctx InterpolationContext) (any, error) {
	switch v := obj.(type) {
	case string:
		if anyInterpRe.MatchString(v) {
			return InterpolateString(v, ctx)
		}
		return v, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			r, err := Interpolate(item, ctx)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			r, err := Interpolate(item, ctx)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	default:
		return obj, nil
	}
}

// CoerceConfig port of the TS coerceConfig: seeds defaults from the compiled
// schema, then type-coerces present values (number -> float, boolean, array
// wrap, record/object passthrough, everything else -> string).
func CoerceConfig(raw map[string]any, rs *definition.ResolvedSchema) map[string]any {
	out := make(map[string]any, len(rs.Fields))
	for _, f := range rs.Fields {
		if def := f.Default.Value(); def != nil {
			out[string(f.Name)] = def
		}
	}
	for _, f := range rs.Fields {
		key := string(f.Name)
		v, ok := raw[key]
		if !ok || v == nil || v == "" {
			continue
		}
		switch f.Type {
		case definition.FieldTypeNumber, definition.FieldTypeInteger, definition.FieldTypeDecimal:
			out[key] = toNumber(v)
		case definition.FieldTypeBoolean:
			if s, isStr := v.(string); isStr && s == "false" {
				out[key] = false
			} else {
				out[key] = toBool(v)
			}
		case definition.FieldTypeArray:
			if arr, isArr := v.([]any); isArr {
				out[key] = arr
			} else {
				out[key] = []any{v}
			}
		case definition.FieldTypeRecord, definition.FieldTypeObject:
			if m, isMap := v.(map[string]any); isMap {
				out[key] = m
			} else {
				out[key] = stringify(v)
			}
		case definition.FieldTypeString, definition.FieldTypeEnum, definition.FieldTypeUnion:
			out[key] = stringify(v)
		default:
			out[key] = v
		}
	}
	return out
}

func toNumber(v any) any {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		f, err := t.Float64()
		if err == nil {
			return f
		}
		return stringify(v)
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err == nil {
			return f
		}
		return t
	default:
		return stringify(v)
	}
}

func toBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		if t == "false" || t == "0" || t == "" {
			return false
		}
		return true
	default:
		return v != nil
	}
}

func stringify(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}
