package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/watch"
)

// watchRegistration tracks a single (runId, eventType) watch pair.
type watchRegistration struct {
	runID      string
	descriptor watch.WatchDescriptor
	queue      []watch.WatchEvent
	parked     bool
	timer      *time.Timer
}

// WatchService manages watch registrations for all active runs.
type WatchService struct {
	mu             sync.Mutex
	registrations  map[string]map[string]*watchRegistration
	byEventType    map[string]map[string]bool
	busSubs        map[string]func()
	bus            events.ScopedEventBus
	resumeCallback func(runID string, patch map[string]any)
}

// NewWatchService creates a new WatchService.
func NewWatchService(bus events.ScopedEventBus, resumeCallback func(runID string, patch map[string]any)) *WatchService {
	return &WatchService{
		registrations:  make(map[string]map[string]*watchRegistration),
		byEventType:    make(map[string]map[string]bool),
		busSubs:        make(map[string]func()),
		bus:            bus,
		resumeCallback: resumeCallback,
	}
}

// Register creates a watch registration for the given run.
func (s *WatchService) Register(runID string, desc watch.WatchDescriptor) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runMap, ok := s.registrations[runID]
	if !ok {
		runMap = make(map[string]*watchRegistration)
		s.registrations[runID] = runMap
	}

	for _, eventType := range desc.EventTypes {
		reg := &watchRegistration{
			runID:      runID,
			descriptor: desc,
			queue:      make([]watch.WatchEvent, 0),
			parked:     false,
		}
		runMap[eventType] = reg

		runSet, ok := s.byEventType[eventType]
		if !ok {
			runSet = make(map[string]bool)
			s.byEventType[eventType] = runSet
		}
		runSet[runID] = true

		s.acquireBusSubscriptionLocked(eventType)
	}
}

// OnRunPaused returns buffered event or nil if run should wait.
func (s *WatchService) OnRunPaused(runID string) *watch.WatchEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	runMap, ok := s.registrations[runID]
	if !ok {
		return nil
	}

	for _, reg := range runMap {
		if len(reg.queue) > 0 {
			event := &reg.queue[0]
			reg.queue = reg.queue[1:]
			return event
		}
	}

	for _, reg := range runMap {
		reg.parked = true
	}

	for _, reg := range runMap {
		if reg.descriptor.Timeout > 0 {
			s.startTimeoutLocked(runID, reg.descriptor.Timeout)
			break
		}
	}

	return nil
}

// OnRunEnded cleans up all watches for a run.
func (s *WatchService) OnRunEnded(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runMap, ok := s.registrations[runID]
	if !ok {
		return
	}

	for eventType, reg := range runMap {
		if reg.timer != nil {
			reg.timer.Stop()
		}
		s.removeFromReverseIndexLocked(runID, eventType)
		s.releaseBusSubscriptionLocked(eventType)
	}

	delete(s.registrations, runID)
}

// PeekBufferedEvent checks if there's a buffered event without marking the run as parked.
// This is used by bounded pause nodes to check for events before deciding to pause.
func (s *WatchService) PeekBufferedEvent(runID string) *watch.WatchEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	runMap, ok := s.registrations[runID]
	if !ok {
		return nil
	}

	for _, reg := range runMap {
		if len(reg.queue) > 0 {
			event := &reg.queue[0]
			return event
		}
	}

	return nil
}

// onEvent is called when a bus event arrives for a watched event type.
func (s *WatchService) onEvent(eventType string, payload map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	runSet, ok := s.byEventType[eventType]
	if !ok || len(runSet) == 0 {
		return
	}

	type resumeEntry struct {
		runID string
		event watch.WatchEvent
	}
	var toResume []resumeEntry

	for runID := range runSet {
		runMap, ok := s.registrations[runID]
		if !ok {
			continue
		}
		reg, ok := runMap[eventType]
		if !ok {
			continue
		}

		if !s.evaluateConditions(reg.descriptor.Conditions, payload) {
			continue
		}

		patch := make(map[string]any)
		for k, v := range reg.descriptor.Patch {
			patch[k] = v
		}
		for k, v := range payload {
			patch[k] = v
		}

		resolved := watch.WatchEvent{
			EventType: eventType,
			Payload:   payload,
			Patch:     patch,
		}

		if reg.parked {
			reg.parked = false
			if reg.timer != nil {
				reg.timer.Stop()
				reg.timer = nil
			}
			toResume = append(toResume, resumeEntry{runID: runID, event: resolved})
		} else {
			reg.queue = append(reg.queue, resolved)
		}
	}

	s.mu.Unlock()
	for _, entry := range toResume {
		s.resumeCallback(entry.runID, entry.event.Patch)
	}
	s.mu.Lock()
}

func (s *WatchService) evaluateConditions(conditions []watch.WatchCondition, payload map[string]any) bool {
	for _, cond := range conditions {
		value := getField(payload, cond.Field)
		if cond.Op == "exists" {
			if value == nil {
				return false
			}
			continue
		}
		if value == nil {
			return false
		}
		switch cond.Op {
		case "==":
			if value != cond.Value {
				return false
			}
		case "!=":
			if value == cond.Value {
				return false
			}
		}
	}
	return true
}

func (s *WatchService) startTimeoutLocked(runID string, timeoutMs int64) {
	runMap, ok := s.registrations[runID]
	if !ok {
		return
	}

	for _, reg := range runMap {
		if reg.timer != nil {
			reg.timer.Stop()
		}
	}

	timer := time.AfterFunc(time.Duration(timeoutMs)*time.Millisecond, func() {
		s.mu.Lock()
		regMap, ok := s.registrations[runID]
		if !ok {
			s.mu.Unlock()
			return
		}
		parked := false
		for _, reg := range regMap {
			if reg.parked {
				parked = true
				reg.parked = false
				break
			}
		}
		if !parked {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		s.resumeCallback(runID, map[string]any{"__resume_reason__": "timeout"})
	})

	for _, reg := range runMap {
		reg.timer = timer
		break
	}
}

func (s *WatchService) acquireBusSubscriptionLocked(eventType string) {
	if _, ok := s.busSubs[eventType]; ok {
		return
	}
	unsubscribe := s.bus.Subscribe(eventType, func(ctx context.Context, evt events.PipelineEvent) error {
		s.onEvent(eventType, evt.Payload)
		return nil
	})
	s.busSubs[eventType] = unsubscribe
}

func (s *WatchService) releaseBusSubscriptionLocked(eventType string) {
	runSet, ok := s.byEventType[eventType]
	if ok && len(runSet) > 0 {
		return
	}
	unsubscribe, ok := s.busSubs[eventType]
	if ok {
		unsubscribe()
		delete(s.busSubs, eventType)
	}
}

func (s *WatchService) removeFromReverseIndexLocked(runID string, eventType string) {
	runSet, ok := s.byEventType[eventType]
	if !ok {
		return
	}
	delete(runSet, runID)
	if len(runSet) == 0 {
		delete(s.byEventType, eventType)
	}
}

func getField(obj any, path string) any {
	if path == "" {
		return obj
	}
	m, ok := obj.(map[string]any)
	if !ok {
		return nil
	}
	parts := splitPath(path)
	var current any = m
	for _, part := range parts {
		cm, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = cm[part]
	}
	return current
}

func splitPath(path string) []string {
	if path == "" {
		return nil
	}
	result := []string{}
	current := ""
	for _, ch := range path {
		if ch == '.' {
			if current != "" {
				result = append(result, current)
			}
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}
