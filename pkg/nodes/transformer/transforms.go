package transformer

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

// executeTransform ports utils executeTransform. It returns the computed value
// and whether it is "present" — the Go analogue of JS `undefined` (missing /
// explicitly-undefined results must be skipped by the caller). A present nil
// value corresponds to JS `null` and must be kept.
func executeTransform(action string, sourceValue any, sourcePresent bool, actionParam string, workingState map[string]any, targetKey string) (any, bool) {
	switch action {
	case "EXTRACT":
		return sourceValue, sourcePresent

	case "MAP_FIELD":
		arr, ok := sourceValue.([]any)
		if !ok {
			return []any{}, true
		}
		out := make([]any, 0, len(arr))
		for _, item := range arr {
			v, _ := nodekit.GetNested(item, actionParam)
			out = append(out, v)
		}
		return out, true

	case "FILTER_LIST":
		if arr, ok := sourceValue.([]any); ok && strings.Contains(actionParam, "=") {
			parts := strings.SplitN(actionParam, "=", 2)
			filterKey := strings.TrimSpace(parts[0])
			filterVal := strings.TrimSpace(parts[1])
			out := make([]any, 0, len(arr))
			for _, item := range arr {
				val, _ := nodekit.GetNested(item, filterKey)
				if str(val, true) == filterVal {
					out = append(out, item)
				}
			}
			return out, true
		}
		if arr, ok := sourceValue.([]any); ok {
			return arr, true
		}
		return []any{}, true

	case "APPEND_LIST":
		existing, _ := nodekit.GetNested(workingState, targetKey)
		var base []any
		if arr, ok := existing.([]any); ok {
			base = append([]any(nil), arr...)
		} else if existing != nil {
			base = []any{existing}
		} else {
			base = []any{}
		}
		if arr, ok := sourceValue.([]any); ok {
			return append(base, arr...), true
		}
		if sourcePresent {
			return append(base, sourceValue), true
		}
		return base, true

	case "COUNT":
		switch t := sourceValue.(type) {
		case []any:
			return float64(len(t)), true
		case string:
			return float64(len([]rune(t))), true
		case map[string]any:
			return float64(len(t)), true
		default:
			return float64(0), true
		}

	case "CONCAT":
		if arr, ok := sourceValue.([]any); ok {
			parts := make([]string, 0, len(arr))
			for _, item := range arr {
				parts = append(parts, str(item, true))
			}
			return strings.Join(parts, actionParam), true
		}
		if sourceValue != nil {
			return str(sourceValue, true), true
		}
		return "", true

	case "CASE_TRANSFORM":
		if sourceValue == nil {
			return nil, false
		}
		mode := strings.ToLower(strings.TrimSpace(actionParam))
		s := str(sourceValue, true)
		switch mode {
		case "upper":
			return strings.ToUpper(s), true
		case "lower":
			return strings.ToLower(s), true
		default:
			return s, true
		}

	case "COALESCE":
		if sourceValue != nil && sourceValue != "" {
			return sourceValue, true
		}
		fallback, _ := nodekit.GetNested(workingState, actionParam)
		return fallback, fallback != nil

	case "MERGE_OBJECTS":
		secondary, _ := nodekit.GetNested(workingState, actionParam)
		out := map[string]any{}
		if m, ok := sourceValue.(map[string]any); ok {
			for k, v := range m {
				out[k] = v
			}
		}
		if m, ok := secondary.(map[string]any); ok {
			for k, v := range m {
				out[k] = v
			}
		}
		return out, true

	case "FLATTEN_OBJECT":
		if m, ok := sourceValue.(map[string]any); ok {
			return flattenObj(m), true
		}
		return sourceValue, sourcePresent

	case "GROUP_BY":
		if arr, ok := sourceValue.([]any); ok && actionParam != "" {
			out := map[string]any{}
			for _, item := range arr {
				val, present := nodekit.GetNested(item, actionParam)
				groupKey := str(val, present)
				if groupKey == "undefined" {
					groupKey = "unassigned"
				}
				if groupKey == "" {
					groupKey = "unassigned"
				}
				list, _ := out[groupKey].([]any)
				out[groupKey] = append(list, item)
			}
			return out, true
		}
		return map[string]any{}, true

	case "KEY_BY":
		if arr, ok := sourceValue.([]any); ok && actionParam != "" {
			out := map[string]any{}
			for _, item := range arr {
				val, present := nodekit.GetNested(item, actionParam)
				idKey := str(val, present)
				if idKey != "" && idKey != "undefined" {
					out[idKey] = item
				}
			}
			return out, true
		}
		return map[string]any{}, true

	case "CAST_TYPE":
		if sourceValue == nil {
			return sourceValue, sourcePresent
		}
		mode := strings.ToLower(strings.TrimSpace(actionParam))
		switch mode {
		case "number":
			if f, ok := nodekit.Number(sourceValue); ok {
				return f, true
			}
			return sourceValue, true
		case "string":
			return str(sourceValue, true), true
		case "boolean":
			if s, ok := sourceValue.(string); ok && s == "false" {
				return false, true
			}
			return toBool(sourceValue), true
		default:
			return sourceValue, true
		}

	case "DEFAULT_IF_EMPTY":
		isEmpty := sourceValue == nil ||
			sourceValue == "" ||
			func() bool {
				if arr, ok := sourceValue.([]any); ok {
					return len(arr) == 0
				}
				return false
			}() ||
			func() bool {
				if m, ok := sourceValue.(map[string]any); ok {
					return len(m) == 0
				}
				return false
			}()
		if !isEmpty {
			return sourceValue, true
		}
		trimmed := strings.TrimSpace(actionParam)
		switch {
		case strings.ToLower(trimmed) == "true":
			return true, true
		case strings.ToLower(trimmed) == "false":
			return false, true
		default:
			if f, err := strconv.ParseFloat(trimmed, 64); err == nil && trimmed != "" {
				return f, true
			}
			return actionParam, true
		}

	case "SET_VALUE":
		trimmed := strings.TrimSpace(actionParam)
		switch {
		case strings.ToLower(trimmed) == "true":
			return true, true
		case strings.ToLower(trimmed) == "false":
			return false, true
		default:
			if f, err := strconv.ParseFloat(trimmed, 64); err == nil && trimmed != "" {
				return f, true
			}
			return actionParam, true
		}

	case "FORMAT_DATE":
		if sourceValue == nil || str(sourceValue, true) == "" {
			return "", true
		}
		t, err := parseJSDate(str(sourceValue, true))
		if err != nil {
			return sourceValue, true
		}
		format := strings.TrimSpace(actionParam)
		pad := func(n int) string { return fmt.Sprintf("%02d", n) }
		replacements := []struct{ from, to string }{
			{"YYYY", fmt.Sprintf("%d", t.Year())},
			{"MM", pad(int(t.Month()))},
			{"DD", pad(t.Day())},
			{"HH", pad(t.Hour())},
			{"mm", pad(t.Minute())},
			{"ss", pad(t.Second())},
		}
		result := format
		for _, r := range replacements {
			result = strings.Replace(result, r.from, r.to, 1)
		}
		return result, true

	case "DATE_DIFF":
		if sourceValue == nil {
			return 0, true
		}
		parts := strings.SplitN(actionParam, "|", 2)
		comparePath := ""
		unit := "days"
		if len(parts) > 0 {
			comparePath = strings.TrimSpace(parts[0])
		}
		if len(parts) > 1 {
			unit = strings.TrimSpace(parts[1])
		}
		first, err1 := parseJSDate(str(sourceValue, true))
		var second time.Time
		if comparePath == "system.now" {
			second = time.Now()
		} else {
			secondary, _ := nodekit.GetNested(workingState, comparePath)
			if secondary != nil {
				second, _ = parseJSDate(str(secondary, true))
			}
		}
		if err1 != nil {
			return 0, true
		}
		if second.IsZero() {
			return 0, true
		}
		diffMs := durationAbs(second.Sub(first))
		var divisor float64
		switch strings.ToLower(unit) {
		case "seconds":
			divisor = 1000
		case "minutes":
			divisor = 1000 * 60
		case "hours":
			divisor = 1000 * 60 * 60
		default:
			divisor = 1000 * 60 * 60 * 24
		}
		return mathFloor(diffMs / divisor), true

	case "SORT_LIST":
		arr, ok := sourceValue.([]any)
		if !ok {
			return []any{}, true
		}
		parts := strings.SplitN(actionParam, ":", 2)
		sortKey := strings.TrimSpace(parts[0])
		isDesc := len(parts) > 1 && strings.ToLower(strings.TrimSpace(parts[1])) == "desc"
		out := append([]any(nil), arr...)
		sort.SliceStable(out, func(i, j int) bool {
			valA, _ := getForSort(out[i], sortKey)
			valB, _ := getForSort(out[j], sortKey)
			if isNullLike(valA) {
				return isDesc
			}
			if isNullLike(valB) {
				return !isDesc
			}
			cmp := compareAny(valA, valB)
			if isDesc {
				return cmp > 0
			}
			return cmp < 0
		})
		return out, true

	case "UNIQUE_LIST":
		arr, ok := sourceValue.([]any)
		if !ok {
			return []any{}, true
		}
		propPath := strings.TrimSpace(actionParam)
		if propPath == "" {
			seen := map[string]bool{}
			out := make([]any, 0, len(arr))
			for _, item := range arr {
				key := uniqKey(item)
				if seen[key] {
					continue
				}
				seen[key] = true
				out = append(out, item)
			}
			return out, true
		}
		seen := map[string]bool{}
		out := make([]any, 0, len(arr))
		for _, item := range arr {
			val, _ := nodekit.GetNested(item, propPath)
			key := uniqKey(val)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, item)
		}
		return out, true

	case "SLICE_LIST":
		arr, ok := sourceValue.([]any)
		if !ok {
			return []any{}, true
		}
		parts := strings.SplitN(actionParam, ":", 2)
		start := 0
		if parts[0] != "" {
			if n, err := strconv.Atoi(parts[0]); err == nil {
				start = n
			}
		}
		end := len(arr)
		if len(parts) > 1 && parts[1] != "" {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				end = n
			}
		}
		if start < 0 {
			start = 0
		}
		if start > len(arr) {
			start = len(arr)
		}
		if end < start {
			end = start
		}
		if end > len(arr) {
			end = len(arr)
		}
		return arr[start:end], true

	case "DATE_ADD":
		if sourceValue == nil {
			return nil, false
		}
		parts := strings.SplitN(actionParam, "|", 2)
		amount, errA := strconv.Atoi(strings.TrimSpace(parts[0]))
		unit := "days"
		if len(parts) > 1 {
			unit = strings.ToLower(strings.TrimSpace(parts[1]))
		}
		t, errD := parseJSDate(str(sourceValue, true))
		if errA != nil || errD != nil {
			return nil, false
		}
		switch unit {
		case "hours":
			t = t.Add(time.Duration(amount) * time.Hour)
		case "minutes":
			t = t.Add(time.Duration(amount) * time.Minute)
		default:
			t = t.AddDate(0, 0, amount)
		}
		return t.UTC().Format(time.RFC3339), true

	case "START_OF_UNIT":
		if sourceValue == nil {
			return nil, false
		}
		t, err := parseJSDate(str(sourceValue, true))
		if err != nil {
			return nil, false
		}
		unit := strings.ToLower(strings.TrimSpace(actionParam))
		switch unit {
		case "year":
			t = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
		case "month":
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		case "day":
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		case "week":
			offset := int(t.Weekday())
			t = time.Date(t.Year(), t.Month(), t.Day()-offset, 0, 0, 0, 0, t.Location())
		}
		return t.UTC().Format(time.RFC3339), true

	case "REDUCE_SUM":
		arr, ok := sourceValue.([]any)
		if !ok {
			return 0, true
		}
		prop := strings.TrimSpace(actionParam)
		sum := 0.0
		for _, item := range arr {
			var val any
			if prop != "" {
				val, _ = nodekit.GetNested(item, prop)
			} else {
				val = item
			}
			if f, ok := nodekit.Number(val); ok {
				sum += f
			}
		}
		return sum, true

	case "FIND_MATCH":
		arr, ok := sourceValue.([]any)
		if !ok || !strings.Contains(actionParam, "=") {
			return nil, true
		}
		parts := strings.SplitN(actionParam, "=", 2)
		fKey := strings.TrimSpace(parts[0])
		fVal := strings.TrimSpace(parts[1])
		for _, item := range arr {
			val, _ := nodekit.GetNested(item, fKey)
			if str(val, true) == fVal {
				return item, true
			}
		}
		return nil, true

	case "ARRAY_TO_OBJECT":
		arr, ok := sourceValue.([]any)
		if !ok {
			return map[string]any{}, true
		}
		parts := strings.SplitN(actionParam, ":", 2)
		if len(parts) != 2 {
			return map[string]any{}, true
		}
		keyProp := strings.TrimSpace(parts[0])
		valProp := strings.TrimSpace(parts[1])
		out := map[string]any{}
		for _, item := range arr {
			k, kp := nodekit.GetNested(item, keyProp)
			v, _ := nodekit.GetNested(item, valProp)
			ks := str(k, kp)
			if ks != "" && ks != "undefined" {
				out[ks] = v
			}
		}
		return out, true

	default:
		return nil, false
	}
}

// flattenObj ports the TS flattenObj helper.
func flattenObj(obj map[string]any) map[string]any {
	out := map[string]any{}
	var walk func(m map[string]any, prefix string)
	walk = func(m map[string]any, prefix string) {
		for k, v := range m {
			pre := k
			if prefix != "" {
				pre = prefix + "." + k
			}
			if sub, ok := v.(map[string]any); ok {
				walk(sub, pre)
			} else {
				out[pre] = v
			}
		}
	}
	walk(obj, "")
	return out
}

// str mirrors JS String(x) combined with presence: missing values stringify as
// "undefined", present nils as "null".
func str(v any, present bool) string {
	if !present {
		return "undefined"
	}
	if v == nil {
		return "null"
	}
	return stringifyJS(v)
}

func stringifyJS(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func toBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t != "" && t != "0" && strings.ToLower(t) != "false"
	case float64:
		return t != 0
	case nil:
		return false
	default:
		return true
	}
}

func parseJSDate(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid date %q", s)
}

func durationAbs(d time.Duration) float64 {
	if d < 0 {
		return float64(-d)
	}
	return float64(d)
}

func mathFloor(f float64) float64 {
	trunc := float64(int64(f))
	if f < trunc {
		return trunc - 1
	}
	return trunc
}

func isNullLike(v any) bool {
	if v == nil {
		return true
	}
	if s, ok := v.(string); ok {
		return s == "undefined" || s == "null"
	}
	return false
}

func compareAny(a, b any) int {
	af, aOK := nodekit.Number(a)
	bf, bOK := nodekit.Number(b)
	if aOK && bOK {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	as := stringifyJS(a)
	bs := stringifyJS(b)
	return strings.Compare(as, bs)
}

func getForSort(item any, key string) (any, bool) {
	if key == "" {
		return item, true
	}
	return nodekit.GetNested(item, key)
}

func uniqKey(v any) string {
	if v == nil {
		return "<nil>"
	}
	if s, ok := v.(string); ok {
		return s
	}
	if f, ok := nodekit.Number(v); ok {
		return fmt.Sprintf("%v", f)
	}
	return fmt.Sprintf("%T:%p", v, &v)
}