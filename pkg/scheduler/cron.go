package scheduler

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CronDelay computes the duration from now until the next cron fire.
// Supports:
//   - "@every <duration>"  → fixed interval
//   - "@daily", "@hourly", "@weekly" → named intervals
//   - "min hour day month weekday" → standard 5-field cron
func CronDelay(expr string) time.Duration {
	expr = strings.TrimSpace(expr)

	// Handle @every, @daily, @hourly, @weekly
	if strings.HasPrefix(expr, "@every ") {
		d, err := time.ParseDuration(expr[7:])
		if err != nil {
			return time.Hour
		}
		return d
	}
	switch expr {
	case "@daily", "@midnight":
		return untilNext(time.Hour * 24)
	case "@hourly":
		return untilNext(time.Hour)
	case "@weekly":
		return untilNext(time.Hour * 24 * 7)
	}

	// Standard 5-field cron
	c, err := parseCron(expr)
	if err != nil {
		return time.Hour // fallback
	}
	next := c.Next(time.Now())
	d := time.Until(next)
	if d <= 0 {
		d = time.Second
	}
	return d
}

func untilNext(interval time.Duration) time.Duration {
	now := time.Now()
	next := now.Truncate(interval).Add(interval)
	return time.Until(next)
}

// Cron represents a parsed 5-field cron expression.
type Cron struct {
	Minute, Hour, Day, Month, Weekday []int
}

func parseCron(expr string) (*Cron, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d", len(fields))
	}

	c := &Cron{}
	var err error

	if c.Minute, err = parseField(fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("cron: minute: %w", err)
	}
	if c.Hour, err = parseField(fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("cron: hour: %w", err)
	}
	if c.Day, err = parseField(fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("cron: day: %w", err)
	}
	if c.Month, err = parseField(fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("cron: month: %w", err)
	}
	if c.Weekday, err = parseField(fields[4], 0, 6); err != nil {
		return nil, fmt.Errorf("cron: weekday: %w", err)
	}
	return c, nil
}

// Next returns the next time the cron matches after t.
func (c *Cron) Next(t time.Time) time.Time {
	t = t.Add(time.Minute).Truncate(time.Minute)

	for y := t.Year(); y <= t.Year()+1; y++ {
		for m := 1; m <= 12; m++ {
			if !contains(c.Month, m) {
				continue
			}
			daysInMonth := time.Date(y, time.Month(m+1), 0, 0, 0, 0, 0, t.Location()).Day()
			for d := 1; d <= daysInMonth; d++ {
				if !contains(c.Day, d) {
					continue
				}
				wd := int(time.Date(y, time.Month(m), d, 0, 0, 0, 0, t.Location()).Weekday())
				if !contains(c.Weekday, wd) {
					continue
				}
				for h := 0; h < 24; h++ {
					if !contains(c.Hour, h) {
						continue
					}
					for min := 0; min < 60; min++ {
						if !contains(c.Minute, min) {
							continue
						}
						candidate := time.Date(y, time.Month(m), d, h, min, 0, 0, t.Location())
						if candidate.After(t) {
							return candidate
						}
					}
				}
			}
		}
	}
	return t.Add(time.Hour)
}

func contains(vals []int, v int) bool {
	for _, x := range vals {
		if x == v {
			return true
		}
	}
	return false
}

func parseField(field string, min, max int) ([]int, error) {
	var result []int
	parts := strings.Split(field, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "*" {
			for i := min; i <= max; i++ {
				result = append(result, i)
			}
			continue
		}
		if strings.Contains(part, "-") {
			bounds := strings.SplitN(part, "-", 2)
			lo, err := strconv.Atoi(bounds[0])
			if err != nil {
				return nil, err
			}
			hi, err := strconv.Atoi(bounds[1])
			if err != nil {
				return nil, err
			}
			for i := lo; i <= hi; i++ {
				result = append(result, i)
			}
			continue
		}
		if strings.HasPrefix(part, "*/") {
			step, err := strconv.Atoi(part[2:])
			if err != nil {
				return nil, err
			}
			for i := min; i <= max; i += step {
				result = append(result, i)
			}
			continue
		}
		v, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		if v < min || v > max {
			return nil, fmt.Errorf("value %d out of range [%d, %d]", v, min, max)
		}
		result = append(result, v)
	}
	return result, nil
}
