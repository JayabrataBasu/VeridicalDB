package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TriggerTiming specifies when a trigger fires.
type TriggerTiming int

const (
	TriggerBefore TriggerTiming = iota
	TriggerAfter
	TriggerInsteadOf
)

// String returns the string representation of TriggerTiming.
func (t TriggerTiming) String() string {
	switch t {
	case TriggerBefore:
		return "BEFORE"
	case TriggerAfter:
		return "AFTER"
	case TriggerInsteadOf:
		return "INSTEAD OF"
	default:
		return "UNKNOWN"
	}
}

// TriggerEvent specifies the event that fires a trigger.
type TriggerEvent int

const (
	TriggerInsert TriggerEvent = iota
	TriggerUpdate
	TriggerDelete
)

// String returns the string representation of TriggerEvent.
func (e TriggerEvent) String() string {
	switch e {
	case TriggerInsert:
		return "INSERT"
	case TriggerUpdate:
		return "UPDATE"
	case TriggerDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// TriggerMeta holds metadata for a trigger.
type TriggerMeta struct {
	Name         string        `json:"name"`
	TableName    string        `json:"table_name"`
	Timing       TriggerTiming `json:"timing"`
	Event        TriggerEvent  `json:"event"`
	ForEachRow   bool          `json:"for_each_row"`
	FunctionName string        `json:"function_name"`
	Enabled      bool          `json:"enabled"`
}

// TriggerCatalog manages trigger metadata.
type TriggerCatalog struct {
	mu       sync.RWMutex
	dataDir  string
	triggers map[string]map[string]*TriggerMeta // tableName -> triggerName -> trigger
}

// NewTriggerCatalog creates or loads a trigger catalog from dataDir.
func NewTriggerCatalog(dataDir string) (*TriggerCatalog, error) {
	tc := &TriggerCatalog{
		dataDir:  dataDir,
		triggers: make(map[string]map[string]*TriggerMeta),
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	if err := tc.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return tc, nil
}

func (tc *TriggerCatalog) catalogPath() string {
	return filepath.Join(tc.dataDir, "triggers.json")
}

// load reads trigger catalog from disk.
func (tc *TriggerCatalog) load() error {
	data, err := os.ReadFile(tc.catalogPath())
	if err != nil {
		return err
	}
	var state struct {
		Triggers []*TriggerMeta `json:"triggers"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	for _, t := range state.Triggers {
		if tc.triggers[t.TableName] == nil {
			tc.triggers[t.TableName] = make(map[string]*TriggerMeta)
		}
		tc.triggers[t.TableName][t.Name] = t
	}
	return nil
}

// save writes trigger catalog to disk.
func (tc *TriggerCatalog) save() error {
	var triggers []*TriggerMeta
	for _, tableTriggers := range tc.triggers {
		for _, t := range tableTriggers {
			triggers = append(triggers, t)
		}
	}
	state := struct {
		Triggers []*TriggerMeta `json:"triggers"`
	}{
		Triggers: triggers,
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(tc.catalogPath(), data, 0o644)
}

// CreateTrigger registers a new trigger.
func (tc *TriggerCatalog) CreateTrigger(trigger *TriggerMeta) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.triggers[trigger.TableName] == nil {
		tc.triggers[trigger.TableName] = make(map[string]*TriggerMeta)
	}

	if _, exists := tc.triggers[trigger.TableName][trigger.Name]; exists {
		return fmt.Errorf("trigger %q already exists on table %q", trigger.Name, trigger.TableName)
	}

	trigger.Enabled = true
	tc.triggers[trigger.TableName][trigger.Name] = trigger

	return tc.save()
}

// DropTrigger removes a trigger.
func (tc *TriggerCatalog) DropTrigger(name, tableName string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tableTriggers, exists := tc.triggers[tableName]
	if !exists {
		return fmt.Errorf("table %q has no triggers", tableName)
	}

	if _, exists := tableTriggers[name]; !exists {
		return fmt.Errorf("trigger %q does not exist on table %q", name, tableName)
	}

	delete(tableTriggers, name)
	if len(tableTriggers) == 0 {
		delete(tc.triggers, tableName)
	}

	return tc.save()
}

// GetTrigger retrieves a trigger by name and table.
func (tc *TriggerCatalog) GetTrigger(name, tableName string) (*TriggerMeta, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tableTriggers, exists := tc.triggers[tableName]
	if !exists {
		return nil, false
	}
	trigger, exists := tableTriggers[name]
	return trigger, exists
}

// GetTriggersForTable retrieves all triggers for a table.
func (tc *TriggerCatalog) GetTriggersForTable(tableName string) []*TriggerMeta {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tableTriggers, exists := tc.triggers[tableName]
	if !exists {
		return nil
	}

	result := make([]*TriggerMeta, 0, len(tableTriggers))
	for _, t := range tableTriggers {
		result = append(result, t)
	}
	return result
}

// GetTriggersForTableEvent retrieves triggers for a specific table and event.
func (tc *TriggerCatalog) GetTriggersForTableEvent(tableName string, timing TriggerTiming, event TriggerEvent) []*TriggerMeta {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tableTriggers, exists := tc.triggers[tableName]
	if !exists {
		return nil
	}

	var result []*TriggerMeta
	for _, t := range tableTriggers {
		if t.Timing == timing && t.Event == event && t.Enabled {
			result = append(result, t)
		}
	}
	return result
}

// ListAllTriggers returns all triggers.
func (tc *TriggerCatalog) ListAllTriggers() []*TriggerMeta {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	var result []*TriggerMeta
	for _, tableTriggers := range tc.triggers {
		for _, t := range tableTriggers {
			result = append(result, t)
		}
	}
	return result
}

// TriggerExists checks if a trigger exists.
func (tc *TriggerCatalog) TriggerExists(name, tableName string) bool {
	_, exists := tc.GetTrigger(name, tableName)
	return exists
}

// EnableTrigger enables a trigger.
func (tc *TriggerCatalog) EnableTrigger(name, tableName string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tableTriggers, exists := tc.triggers[tableName]
	if !exists {
		return fmt.Errorf("table %q has no triggers", tableName)
	}

	trigger, exists := tableTriggers[name]
	if !exists {
		return fmt.Errorf("trigger %q does not exist on table %q", name, tableName)
	}

	trigger.Enabled = true
	return tc.save()
}

// DisableTrigger disables a trigger.
func (tc *TriggerCatalog) DisableTrigger(name, tableName string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tableTriggers, exists := tc.triggers[tableName]
	if !exists {
		return fmt.Errorf("table %q has no triggers", tableName)
	}

	trigger, exists := tableTriggers[name]
	if !exists {
		return fmt.Errorf("trigger %q does not exist on table %q", name, tableName)
	}

	trigger.Enabled = false
	return tc.save()
}

// DropTriggersForTable removes all triggers for a table.
// Called when a table is dropped.
func (tc *TriggerCatalog) DropTriggersForTable(tableName string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	delete(tc.triggers, tableName)
	return tc.save()
}
