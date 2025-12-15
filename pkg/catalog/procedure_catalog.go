package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ProcedureType distinguishes procedures from functions.
type ProcedureType int

const (
	ProcTypeProcedure ProcedureType = iota
	ProcTypeFunction
)

// String returns the string representation of ProcedureType.
func (t ProcedureType) String() string {
	switch t {
	case ProcTypeProcedure:
		return "PROCEDURE"
	case ProcTypeFunction:
		return "FUNCTION"
	default:
		return "UNKNOWN"
	}
}

// ParamMode represents the mode of a parameter.
type ParamMode int

const (
	ParamModeIn    ParamMode = iota // IN (default, input only)
	ParamModeOut                    // OUT (output only)
	ParamModeInOut                  // INOUT (input and output)
)

// String returns the string representation of ParamMode.
func (m ParamMode) String() string {
	switch m {
	case ParamModeIn:
		return "IN"
	case ParamModeOut:
		return "OUT"
	case ParamModeInOut:
		return "INOUT"
	default:
		return "IN"
	}
}

// ProcParamMeta holds metadata for a procedure/function parameter.
type ProcParamMeta struct {
	Name        string    `json:"name"`
	Type        DataType  `json:"type"`
	Mode        ParamMode `json:"mode"`
	HasDefault  bool      `json:"has_default,omitempty"`
	DefaultExpr string    `json:"default_expr,omitempty"` // stored as text for simplicity
}

// ProcedureMeta holds metadata for a stored procedure or function.
type ProcedureMeta struct {
	Name       string          `json:"name"`
	Type       ProcedureType   `json:"type"` // PROCEDURE or FUNCTION
	Params     []ProcParamMeta `json:"params"`
	ReturnType DataType        `json:"return_type,omitempty"` // only for functions
	Language   string          `json:"language"`              // e.g., "plpgsql", "sql"
	Body       string          `json:"body"`                  // original body text
	Owner      string          `json:"owner,omitempty"`
	CreatedAt  int64           `json:"created_at"`
}

// ProcedureCatalog manages stored procedure and function metadata.
type ProcedureCatalog struct {
	mu         sync.RWMutex
	dataDir    string
	procedures map[string]*ProcedureMeta // name -> procedure
	functions  map[string]*ProcedureMeta // name -> function
}

// NewProcedureCatalog creates or loads a procedure catalog from dataDir.
func NewProcedureCatalog(dataDir string) (*ProcedureCatalog, error) {
	pc := &ProcedureCatalog{
		dataDir:    dataDir,
		procedures: make(map[string]*ProcedureMeta),
		functions:  make(map[string]*ProcedureMeta),
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	if err := pc.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return pc, nil
}

func (pc *ProcedureCatalog) catalogPath() string {
	return filepath.Join(pc.dataDir, "procedures.json")
}

// load reads procedure catalog from disk.
func (pc *ProcedureCatalog) load() error {
	data, err := os.ReadFile(pc.catalogPath())
	if err != nil {
		return err
	}
	var state struct {
		Procedures []*ProcedureMeta `json:"procedures"`
		Functions  []*ProcedureMeta `json:"functions"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	for _, p := range state.Procedures {
		pc.procedures[p.Name] = p
	}
	for _, f := range state.Functions {
		pc.functions[f.Name] = f
	}
	return nil
}

// save writes procedure catalog to disk.
func (pc *ProcedureCatalog) save() error {
	var state struct {
		Procedures []*ProcedureMeta `json:"procedures"`
		Functions  []*ProcedureMeta `json:"functions"`
	}
	for _, p := range pc.procedures {
		state.Procedures = append(state.Procedures, p)
	}
	for _, f := range pc.functions {
		state.Functions = append(state.Functions, f)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(pc.catalogPath(), data, 0o644)
}

// CreateProcedure creates a new stored procedure.
func (pc *ProcedureCatalog) CreateProcedure(meta *ProcedureMeta) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if _, exists := pc.procedures[meta.Name]; exists {
		return fmt.Errorf("procedure %q already exists", meta.Name)
	}

	meta.Type = ProcTypeProcedure
	pc.procedures[meta.Name] = meta
	return pc.save()
}

// CreateOrReplaceProcedure creates or replaces a stored procedure.
func (pc *ProcedureCatalog) CreateOrReplaceProcedure(meta *ProcedureMeta) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	meta.Type = ProcTypeProcedure
	pc.procedures[meta.Name] = meta
	return pc.save()
}

// CreateFunction creates a new stored function.
func (pc *ProcedureCatalog) CreateFunction(meta *ProcedureMeta) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if _, exists := pc.functions[meta.Name]; exists {
		return fmt.Errorf("function %q already exists", meta.Name)
	}

	meta.Type = ProcTypeFunction
	pc.functions[meta.Name] = meta
	return pc.save()
}

// CreateOrReplaceFunction creates or replaces a stored function.
func (pc *ProcedureCatalog) CreateOrReplaceFunction(meta *ProcedureMeta) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	meta.Type = ProcTypeFunction
	pc.functions[meta.Name] = meta
	return pc.save()
}

// GetProcedure retrieves a stored procedure by name.
func (pc *ProcedureCatalog) GetProcedure(name string) (*ProcedureMeta, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	p, ok := pc.procedures[name]
	return p, ok
}

// GetFunction retrieves a stored function by name.
func (pc *ProcedureCatalog) GetFunction(name string) (*ProcedureMeta, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	f, ok := pc.functions[name]
	return f, ok
}

// DropProcedure removes a stored procedure.
func (pc *ProcedureCatalog) DropProcedure(name string, ifExists bool) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if _, exists := pc.procedures[name]; !exists {
		if ifExists {
			return nil
		}
		return fmt.Errorf("procedure %q does not exist", name)
	}

	delete(pc.procedures, name)
	return pc.save()
}

// DropFunction removes a stored function.
func (pc *ProcedureCatalog) DropFunction(name string, ifExists bool) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if _, exists := pc.functions[name]; !exists {
		if ifExists {
			return nil
		}
		return fmt.Errorf("function %q does not exist", name)
	}

	delete(pc.functions, name)
	return pc.save()
}

// ListProcedures returns all stored procedures.
func (pc *ProcedureCatalog) ListProcedures() []*ProcedureMeta {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	result := make([]*ProcedureMeta, 0, len(pc.procedures))
	for _, p := range pc.procedures {
		result = append(result, p)
	}
	return result
}

// ListFunctions returns all stored functions.
func (pc *ProcedureCatalog) ListFunctions() []*ProcedureMeta {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	result := make([]*ProcedureMeta, 0, len(pc.functions))
	for _, f := range pc.functions {
		result = append(result, f)
	}
	return result
}

// ProcedureExists checks if a procedure exists.
func (pc *ProcedureCatalog) ProcedureExists(name string) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	_, exists := pc.procedures[name]
	return exists
}

// FunctionExists checks if a function exists.
func (pc *ProcedureCatalog) FunctionExists(name string) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	_, exists := pc.functions[name]
	return exists
}
