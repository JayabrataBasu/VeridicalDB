package fts

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Posting represents a single occurrence of a term in a document.
type Posting struct {
	DocID     uint64 // Document/row identifier
	Frequency int    // Number of times term appears in document
	Positions []int  // Positions where term appears
}

// TermEntry holds all postings for a single term.
type TermEntry struct {
	Term           string    // The term
	DocumentFreq   int       // Number of documents containing this term
	Postings       []Posting // Sorted by DocID
	CollectionFreq int       // Total occurrences across all documents
}

// InvertedIndex is the main full-text search index structure.
type InvertedIndex struct {
	Name       string                // Index name
	TableName  string                // Associated table
	Columns    []string              // Indexed columns
	Terms      map[string]*TermEntry // Term -> postings
	DocLengths map[uint64]int        // DocID -> document length (for BM25)
	TotalDocs  int                   // Total number of indexed documents
	AvgDocLen  float64               // Average document length
	analyzer   *Analyzer             // Text analyzer
	mu         sync.RWMutex          // Thread-safe access
	dirty      bool                  // Whether index needs saving
}

// IndexMeta stores index metadata for persistence.
type IndexMeta struct {
	Name      string   `json:"name"`
	TableName string   `json:"table_name"`
	Columns   []string `json:"columns"`
	TotalDocs int      `json:"total_docs"`
	AvgDocLen float64  `json:"avg_doc_len"`
}

// NewInvertedIndex creates a new inverted index.
func NewInvertedIndex(name, tableName string, columns []string) *InvertedIndex {
	return &InvertedIndex{
		Name:       name,
		TableName:  tableName,
		Columns:    columns,
		Terms:      make(map[string]*TermEntry),
		DocLengths: make(map[uint64]int),
		analyzer:   NewAnalyzer(),
		dirty:      true, // New indexes are dirty by default
	}
}

// SetAnalyzer sets a custom analyzer for the index.
func (idx *InvertedIndex) SetAnalyzer(analyzer *Analyzer) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.analyzer = analyzer
}

// IndexDocument adds or updates a document in the index.
func (idx *InvertedIndex) IndexDocument(docID uint64, text string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// First, remove any existing document with this ID
	idx.removeDocumentLocked(docID)

	// Analyze the text
	tokens := idx.analyzer.Analyze(text)
	if len(tokens) == 0 {
		return nil // Nothing to index
	}

	// Count term frequencies and positions
	termFreqs := make(map[string][]int)
	for _, token := range tokens {
		termFreqs[token.Term] = append(termFreqs[token.Term], token.Position)
	}

	// Add to index
	for term, positions := range termFreqs {
		entry, exists := idx.Terms[term]
		if !exists {
			entry = &TermEntry{
				Term:     term,
				Postings: make([]Posting, 0),
			}
			idx.Terms[term] = entry
		}

		posting := Posting{
			DocID:     docID,
			Frequency: len(positions),
			Positions: positions,
		}

		// Insert posting in sorted order by DocID
		insertIdx := sort.Search(len(entry.Postings), func(i int) bool {
			return entry.Postings[i].DocID >= docID
		})

		entry.Postings = append(entry.Postings, Posting{})
		copy(entry.Postings[insertIdx+1:], entry.Postings[insertIdx:])
		entry.Postings[insertIdx] = posting

		entry.DocumentFreq++
		entry.CollectionFreq += len(positions)
	}

	// Update document length
	idx.DocLengths[docID] = len(tokens)
	idx.TotalDocs++

	// Recalculate average document length
	totalLen := 0
	for _, l := range idx.DocLengths {
		totalLen += l
	}
	idx.AvgDocLen = float64(totalLen) / float64(idx.TotalDocs)

	idx.dirty = true
	return nil
}

// RemoveDocument removes a document from the index.
func (idx *InvertedIndex) RemoveDocument(docID uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.removeDocumentLocked(docID)
}

func (idx *InvertedIndex) removeDocumentLocked(docID uint64) {
	if _, exists := idx.DocLengths[docID]; !exists {
		return // Document not in index
	}

	// Remove from all term entries
	for term, entry := range idx.Terms {
		newPostings := make([]Posting, 0, len(entry.Postings))
		for _, p := range entry.Postings {
			if p.DocID == docID {
				entry.DocumentFreq--
				entry.CollectionFreq -= p.Frequency
			} else {
				newPostings = append(newPostings, p)
			}
		}
		entry.Postings = newPostings

		// Remove term if no postings left
		if len(entry.Postings) == 0 {
			delete(idx.Terms, term)
		}
	}

	delete(idx.DocLengths, docID)
	idx.TotalDocs--

	// Recalculate average document length
	if idx.TotalDocs > 0 {
		totalLen := 0
		for _, l := range idx.DocLengths {
			totalLen += l
		}
		idx.AvgDocLen = float64(totalLen) / float64(idx.TotalDocs)
	} else {
		idx.AvgDocLen = 0
	}

	idx.dirty = true
}

// SearchResult represents a single search result with ranking.
type SearchResult struct {
	DocID     uint64
	Score     float64
	Positions []int // Combined positions of all matched terms
}

// Search performs a full-text search and returns ranked results.
func (idx *InvertedIndex) Search(query string, limit int) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	queryTokens := idx.analyzer.AnalyzeQuery(query)
	if len(queryTokens) == 0 {
		return nil, nil
	}

	// Separate required, excluded, and optional terms
	var required, excluded, optional []QueryToken
	for _, qt := range queryTokens {
		if qt.Excluded {
			excluded = append(excluded, qt)
		} else if qt.Required {
			required = append(required, qt)
		} else {
			optional = append(optional, qt)
		}
	}

	// If no required terms, treat all as optional
	if len(required) == 0 {
		optional = append(optional, required...)
	}

	// Collect candidate documents
	candidates := make(map[uint64]*SearchResult)

	// Process required terms (all must match)
	if len(required) > 0 {
		for i, qt := range required {
			matchingDocs := idx.findMatchingDocs(qt)
			if i == 0 {
				// Initialize candidates with first required term
				for docID, positions := range matchingDocs {
					candidates[docID] = &SearchResult{
						DocID:     docID,
						Positions: positions,
					}
				}
			} else {
				// Intersect with existing candidates
				for docID := range candidates {
					if _, exists := matchingDocs[docID]; !exists {
						delete(candidates, docID)
					} else {
						candidates[docID].Positions = append(candidates[docID].Positions, matchingDocs[docID]...)
					}
				}
			}
		}
	}

	// Process optional terms
	for _, qt := range optional {
		matchingDocs := idx.findMatchingDocs(qt)
		for docID, positions := range matchingDocs {
			if result, exists := candidates[docID]; exists {
				result.Positions = append(result.Positions, positions...)
			} else if len(required) == 0 {
				// Only add new docs if no required terms
				candidates[docID] = &SearchResult{
					DocID:     docID,
					Positions: positions,
				}
			}
		}
	}

	// Remove excluded documents
	for _, qt := range excluded {
		matchingDocs := idx.findMatchingDocs(qt)
		for docID := range matchingDocs {
			delete(candidates, docID)
		}
	}

	// Calculate scores using BM25
	results := make([]SearchResult, 0, len(candidates))
	allTerms := append(append(required, optional...), excluded...)

	for docID, result := range candidates {
		score := idx.calculateBM25(docID, allTerms)
		result.Score = score
		results = append(results, *result)
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Apply limit
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// findMatchingDocs finds documents matching a query token.
func (idx *InvertedIndex) findMatchingDocs(qt QueryToken) map[uint64][]int {
	result := make(map[uint64][]int)

	if qt.IsPhrase {
		// Handle phrase search
		return idx.searchPhrase(qt.Term)
	}

	if qt.Prefix {
		// Handle prefix search
		for term, entry := range idx.Terms {
			if strings.HasPrefix(term, qt.Term) {
				for _, posting := range entry.Postings {
					result[posting.DocID] = append(result[posting.DocID], posting.Positions...)
				}
			}
		}
		return result
	}

	if qt.Fuzzy {
		// Handle fuzzy search (Levenshtein distance <= 2)
		for term, entry := range idx.Terms {
			if levenshteinDistance(term, qt.Term) <= 2 {
				for _, posting := range entry.Postings {
					result[posting.DocID] = append(result[posting.DocID], posting.Positions...)
				}
			}
		}
		return result
	}

	// Exact term match
	if entry, exists := idx.Terms[qt.Term]; exists {
		for _, posting := range entry.Postings {
			result[posting.DocID] = append(result[posting.DocID], posting.Positions...)
		}
	}

	return result
}

// searchPhrase searches for a phrase (consecutive words).
func (idx *InvertedIndex) searchPhrase(phrase string) map[uint64][]int {
	result := make(map[uint64][]int)

	// Analyze phrase to get terms
	tokens := idx.analyzer.Analyze(phrase)
	if len(tokens) == 0 {
		return result
	}

	// Get postings for all terms in phrase
	termPostings := make([]map[uint64][]int, len(tokens))
	for i, token := range tokens {
		termPostings[i] = make(map[uint64][]int)
		if entry, exists := idx.Terms[token.Term]; exists {
			for _, posting := range entry.Postings {
				termPostings[i][posting.DocID] = posting.Positions
			}
		}
	}

	// Find documents that contain all terms
	if len(termPostings) == 0 || len(termPostings[0]) == 0 {
		return result
	}

	// Check each document that contains the first term
	for docID, firstPositions := range termPostings[0] {
		// Check if all other terms exist and are in sequence
		for _, startPos := range firstPositions {
			valid := true
			for termIdx := 1; termIdx < len(tokens); termIdx++ {
				otherPositions, exists := termPostings[termIdx][docID]
				if !exists {
					valid = false
					break
				}
				// Check if there's a position at startPos + termIdx
				expectedPos := startPos + termIdx
				found := false
				for _, pos := range otherPositions {
					if pos == expectedPos {
						found = true
						break
					}
				}
				if !found {
					valid = false
					break
				}
			}
			if valid {
				result[docID] = append(result[docID], startPos)
			}
		}
	}

	return result
}

// calculateBM25 calculates the BM25 score for a document.
// BM25 is a ranking function used by search engines.
func (idx *InvertedIndex) calculateBM25(docID uint64, queryTokens []QueryToken) float64 {
	const k1 = 1.2 // Term frequency saturation parameter
	const b = 0.75 // Length normalization parameter

	if idx.TotalDocs == 0 || idx.AvgDocLen == 0 {
		return 0
	}

	docLen := float64(idx.DocLengths[docID])
	score := 0.0

	for _, qt := range queryTokens {
		if qt.Excluded {
			continue
		}

		var entry *TermEntry
		if qt.Prefix {
			// For prefix searches, combine all matching terms
			for term, e := range idx.Terms {
				if strings.HasPrefix(term, qt.Term) {
					if entry == nil {
						entry = &TermEntry{
							DocumentFreq: e.DocumentFreq,
						}
					} else {
						entry.DocumentFreq += e.DocumentFreq
					}
				}
			}
		} else if qt.Fuzzy {
			// For fuzzy searches, combine all matching terms
			for term, e := range idx.Terms {
				if levenshteinDistance(term, qt.Term) <= 2 {
					if entry == nil {
						entry = &TermEntry{
							DocumentFreq: e.DocumentFreq,
						}
					} else {
						entry.DocumentFreq += e.DocumentFreq
					}
				}
			}
		} else {
			entry = idx.Terms[qt.Term]
		}

		if entry == nil {
			continue
		}

		// Find term frequency in this document
		tf := 0
		for _, posting := range entry.Postings {
			if posting.DocID == docID {
				tf = posting.Frequency
				break
			}
		}

		if tf == 0 {
			continue
		}

		// Calculate IDF
		idf := math.Log((float64(idx.TotalDocs) - float64(entry.DocumentFreq) + 0.5) /
			(float64(entry.DocumentFreq) + 0.5))
		if idf < 0 {
			idf = 0
		}

		// Calculate BM25 term score
		tfNorm := (float64(tf) * (k1 + 1)) /
			(float64(tf) + k1*(1-b+b*(docLen/idx.AvgDocLen)))

		score += idf * tfNorm
	}

	return score
}

// levenshteinDistance calculates the edit distance between two strings.
func levenshteinDistance(s1, s2 string) int {
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	// Create matrix
	matrix := make([][]int, len(s1)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(s2)+1)
		matrix[i][0] = i
	}
	for j := range matrix[0] {
		matrix[0][j] = j
	}

	// Fill matrix
	for i := 1; i <= len(s1); i++ {
		for j := 1; j <= len(s2); j++ {
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
			}
			matrix[i][j] = min(
				matrix[i-1][j]+1,      // Deletion
				matrix[i][j-1]+1,      // Insertion
				matrix[i-1][j-1]+cost, // Substitution
			)
		}
	}

	return matrix[len(s1)][len(s2)]
}

func min(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// GetStats returns statistics about the index.
type IndexStats struct {
	TotalTerms     int
	TotalDocs      int
	AvgDocLength   float64
	TotalPostings  int
	MemoryEstimate int64 // Estimated memory usage in bytes
}

func (idx *InvertedIndex) GetStats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := IndexStats{
		TotalTerms:   len(idx.Terms),
		TotalDocs:    idx.TotalDocs,
		AvgDocLength: idx.AvgDocLen,
	}

	for _, entry := range idx.Terms {
		stats.TotalPostings += len(entry.Postings)
	}

	// Rough memory estimate
	stats.MemoryEstimate = int64(len(idx.Terms)*64 + stats.TotalPostings*32 + idx.TotalDocs*16)

	return stats
}

// Persistence methods

// PersistenceData is the serializable form of the index.
type PersistenceData struct {
	Meta       IndexMeta             `json:"meta"`
	Terms      map[string]*TermEntry `json:"terms"`
	DocLengths map[uint64]int        `json:"doc_lengths"`
}

// Save persists the index to disk.
func (idx *InvertedIndex) Save(dataDir string) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.dirty {
		return nil // No changes to save
	}

	// Ensure directory exists
	ftsDir := filepath.Join(dataDir, "fts")
	if err := os.MkdirAll(ftsDir, 0755); err != nil {
		return fmt.Errorf("create fts directory: %w", err)
	}

	data := PersistenceData{
		Meta: IndexMeta{
			Name:      idx.Name,
			TableName: idx.TableName,
			Columns:   idx.Columns,
			TotalDocs: idx.TotalDocs,
			AvgDocLen: idx.AvgDocLen,
		},
		Terms:      idx.Terms,
		DocLengths: idx.DocLengths,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}

	indexPath := filepath.Join(ftsDir, fmt.Sprintf("%s.json", idx.Name))
	if err := os.WriteFile(indexPath, jsonData, 0644); err != nil {
		return fmt.Errorf("write index file: %w", err)
	}

	idx.dirty = false
	return nil
}

// Load reads an index from disk.
func LoadInvertedIndex(dataDir, name string) (*InvertedIndex, error) {
	ftsDir := filepath.Join(dataDir, "fts")
	indexPath := filepath.Join(ftsDir, fmt.Sprintf("%s.json", name))

	data, err := os.ReadFile(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("index %q not found", name)
		}
		return nil, fmt.Errorf("read index file: %w", err)
	}

	var pd PersistenceData
	if err := json.Unmarshal(data, &pd); err != nil {
		return nil, fmt.Errorf("unmarshal index: %w", err)
	}

	idx := &InvertedIndex{
		Name:       pd.Meta.Name,
		TableName:  pd.Meta.TableName,
		Columns:    pd.Meta.Columns,
		TotalDocs:  pd.Meta.TotalDocs,
		AvgDocLen:  pd.Meta.AvgDocLen,
		Terms:      pd.Terms,
		DocLengths: pd.DocLengths,
		analyzer:   NewAnalyzer(),
		dirty:      false,
	}

	return idx, nil
}

// ListIndexes returns the names of all FTS indexes in the data directory.
func ListIndexes(dataDir string) ([]string, error) {
	ftsDir := filepath.Join(dataDir, "fts")

	entries, err := os.ReadDir(ftsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			name := strings.TrimSuffix(entry.Name(), ".json")
			names = append(names, name)
		}
	}

	return names, nil
}

// DeleteIndex removes an index from disk.
func DeleteIndex(dataDir, name string) error {
	ftsDir := filepath.Join(dataDir, "fts")
	indexPath := filepath.Join(ftsDir, fmt.Sprintf("%s.json", name))
	return os.Remove(indexPath)
}
