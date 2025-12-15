package fts

import (
	"fmt"
	"sync"
)

// Manager manages all full-text search indexes for a database.
type Manager struct {
	dataDir string
	indexes map[string]*InvertedIndex
	mu      sync.RWMutex
}

// NewManager creates a new FTS manager.
func NewManager(dataDir string) (*Manager, error) {
	mgr := &Manager{
		dataDir: dataDir,
		indexes: make(map[string]*InvertedIndex),
	}

	// Load existing indexes
	names, err := ListIndexes(dataDir)
	if err != nil {
		return nil, fmt.Errorf("list indexes: %w", err)
	}

	for _, name := range names {
		idx, err := LoadInvertedIndex(dataDir, name)
		if err != nil {
			// Log warning but continue loading other indexes
			continue
		}
		mgr.indexes[name] = idx
	}

	return mgr, nil
}

// CreateIndex creates a new full-text search index.
func (m *Manager) CreateIndex(name, tableName string, columns []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[name]; exists {
		return fmt.Errorf("index %q already exists", name)
	}

	idx := NewInvertedIndex(name, tableName, columns)
	m.indexes[name] = idx

	// Save to disk immediately
	if err := idx.Save(m.dataDir); err != nil {
		delete(m.indexes, name)
		return fmt.Errorf("save index: %w", err)
	}

	return nil
}

// DropIndex removes a full-text search index.
func (m *Manager) DropIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[name]; !exists {
		return fmt.Errorf("index %q not found", name)
	}

	// Remove from disk
	if err := DeleteIndex(m.dataDir, name); err != nil {
		return fmt.Errorf("delete index file: %w", err)
	}

	delete(m.indexes, name)
	return nil
}

// GetIndex returns an index by name.
func (m *Manager) GetIndex(name string) (*InvertedIndex, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	idx, exists := m.indexes[name]
	if !exists {
		return nil, fmt.Errorf("index %q not found", name)
	}

	return idx, nil
}

// GetIndexForTable returns the FTS index for a table, if any.
func (m *Manager) GetIndexForTable(tableName string) *InvertedIndex {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, idx := range m.indexes {
		if idx.TableName == tableName {
			return idx
		}
	}

	return nil
}

// ListIndexes returns information about all indexes.
func (m *Manager) ListIndexes() []IndexMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metas := make([]IndexMeta, 0, len(m.indexes))
	for _, idx := range m.indexes {
		metas = append(metas, IndexMeta{
			Name:      idx.Name,
			TableName: idx.TableName,
			Columns:   idx.Columns,
			TotalDocs: idx.TotalDocs,
			AvgDocLen: idx.AvgDocLen,
		})
	}

	return metas
}

// IndexDocument indexes a document in the appropriate index.
func (m *Manager) IndexDocument(indexName string, docID uint64, text string) error {
	idx, err := m.GetIndex(indexName)
	if err != nil {
		return err
	}

	return idx.IndexDocument(docID, text)
}

// RemoveDocument removes a document from an index.
func (m *Manager) RemoveDocument(indexName string, docID uint64) error {
	idx, err := m.GetIndex(indexName)
	if err != nil {
		return err
	}

	idx.RemoveDocument(docID)
	return nil
}

// Search performs a search on the specified index.
func (m *Manager) Search(indexName, query string, limit int) ([]SearchResult, error) {
	idx, err := m.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	return idx.Search(query, limit)
}

// SaveAll saves all dirty indexes to disk.
func (m *Manager) SaveAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, idx := range m.indexes {
		if err := idx.Save(m.dataDir); err != nil {
			return fmt.Errorf("save index %q: %w", idx.Name, err)
		}
	}

	return nil
}

// Close saves all indexes and releases resources.
func (m *Manager) Close() error {
	return m.SaveAll()
}

// ReindexTable rebuilds the FTS index for a table.
// The caller must provide a function to iterate over all rows.
func (m *Manager) ReindexTable(indexName string, iterateRows func(fn func(docID uint64, text string) error) error) error {
	idx, err := m.GetIndex(indexName)
	if err != nil {
		return err
	}

	// Clear existing index data
	idx.mu.Lock()
	idx.Terms = make(map[string]*TermEntry)
	idx.DocLengths = make(map[uint64]int)
	idx.TotalDocs = 0
	idx.AvgDocLen = 0
	idx.dirty = true
	idx.mu.Unlock()

	// Reindex all rows
	err = iterateRows(func(docID uint64, text string) error {
		return idx.IndexDocument(docID, text)
	})

	if err != nil {
		return fmt.Errorf("reindex rows: %w", err)
	}

	// Save the updated index
	return idx.Save(m.dataDir)
}

// TSVector represents a text search vector (analyzed document).
type TSVector struct {
	Lexemes map[string][]int // term -> positions
}

// NewTSVector creates a TSVector from text.
func NewTSVector(text string) *TSVector {
	analyzer := NewAnalyzer()
	tokens := analyzer.Analyze(text)

	lexemes := make(map[string][]int)
	for _, token := range tokens {
		lexemes[token.Term] = append(lexemes[token.Term], token.Position)
	}

	return &TSVector{Lexemes: lexemes}
}

// String returns a string representation of the TSVector.
func (v *TSVector) String() string {
	if v == nil || len(v.Lexemes) == 0 {
		return ""
	}

	result := ""
	for term, positions := range v.Lexemes {
		if result != "" {
			result += " "
		}
		result += fmt.Sprintf("'%s'", term)
		if len(positions) > 0 {
			result += ":"
			for i, pos := range positions {
				if i > 0 {
					result += ","
				}
				result += fmt.Sprintf("%d", pos+1) // 1-indexed
			}
		}
	}

	return result
}

// TSQuery represents a text search query.
type TSQuery struct {
	Tokens []QueryToken
}

// NewTSQuery creates a TSQuery from a query string.
func NewTSQuery(query string) *TSQuery {
	analyzer := NewAnalyzer()
	tokens := analyzer.AnalyzeQuery(query)
	return &TSQuery{Tokens: tokens}
}

// Match checks if a TSVector matches a TSQuery.
func (q *TSQuery) Match(v *TSVector) bool {
	if q == nil || v == nil || len(q.Tokens) == 0 {
		return false
	}

	var hasRequired bool
	var allRequiredMatch = true
	var anyOptionalMatch bool

	for _, token := range q.Tokens {
		if token.Excluded {
			// If excluded term exists, no match
			if _, exists := v.Lexemes[token.Term]; exists {
				return false
			}
			continue
		}

		exists := false
		if token.Prefix {
			// Check prefix match
			for term := range v.Lexemes {
				if len(term) >= len(token.Term) && term[:len(token.Term)] == token.Term {
					exists = true
					break
				}
			}
		} else {
			_, exists = v.Lexemes[token.Term]
		}

		if token.Required {
			hasRequired = true
			if !exists {
				allRequiredMatch = false
			}
		} else {
			if exists {
				anyOptionalMatch = true
			}
		}
	}

	if hasRequired {
		return allRequiredMatch
	}
	return anyOptionalMatch
}

// String returns a string representation of the TSQuery.
func (q *TSQuery) String() string {
	if q == nil || len(q.Tokens) == 0 {
		return ""
	}

	result := ""
	for i, token := range q.Tokens {
		if i > 0 {
			result += " & "
		}
		prefix := ""
		if token.Required {
			prefix = "+"
		} else if token.Excluded {
			prefix = "!"
		}
		suffix := ""
		if token.Prefix {
			suffix = ":*"
		}
		result += fmt.Sprintf("%s'%s'%s", prefix, token.Term, suffix)
	}

	return result
}
