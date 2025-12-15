// Package fts provides full-text search functionality tests.
package fts

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

// ==================== Analyzer Tests ====================

func TestAnalyzerBasic(t *testing.T) {
	analyzer := NewAnalyzer()

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple words",
			input:    "hello world",
			expected: []string{"hello", "world"},
		},
		{
			name:     "with stopwords",
			input:    "the quick brown fox",
			expected: []string{"quick", "brown", "fox"},
		},
		{
			name:     "with punctuation",
			input:    "hello, world! how are you?",
			expected: []string{"hello", "world"},
		},
		{
			name:     "mixed case",
			input:    "Hello WORLD HoW ArE YoU",
			expected: []string{"hello", "world"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{},
		},
		{
			name:     "only stopwords",
			input:    "the a an is are",
			expected: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tokens := analyzer.Analyze(tc.input)
			terms := make([]string, len(tokens))
			for i, tok := range tokens {
				terms[i] = tok.Term
			}

			if len(terms) != len(tc.expected) {
				t.Errorf("expected %d tokens, got %d: %v", len(tc.expected), len(terms), terms)
				return
			}

			for i, expected := range tc.expected {
				if terms[i] != expected {
					t.Errorf("token %d: expected %q, got %q", i, expected, terms[i])
				}
			}
		})
	}
}

func TestAnalyzerPositions(t *testing.T) {
	analyzer := NewAnalyzer()
	tokens := analyzer.Analyze("hello world hello")

	// Should have tokens at positions 0, 1, 2
	// "hello" at 0, "world" at 1, "hello" at 2
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}

	expectedPositions := []int{0, 1, 2}
	for i, tok := range tokens {
		if tok.Position != expectedPositions[i] {
			t.Errorf("token %d: expected position %d, got %d", i, expectedPositions[i], tok.Position)
		}
	}
}

func TestPorterStemmer(t *testing.T) {
	stemmer := &PorterStemmer{}

	tests := []struct {
		input    string
		expected string
	}{
		{"running", "run"},
		{"runs", "run"},
		{"ran", "ran"},
		{"easily", "easili"},
		{"caresses", "caress"},
		{"cats", "cat"},
		{"ponies", "poni"},
		{"troubles", "troubl"},
		{"happiness", "happi"},
		{"relational", "relat"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := stemmer.Stem(tc.input)
			if result != tc.expected {
				t.Errorf("Stem(%q) = %q, want %q", tc.input, result, tc.expected)
			}
		})
	}
}

func TestAnalyzeQuery(t *testing.T) {
	analyzer := NewAnalyzer()

	tests := []struct {
		name     string
		query    string
		checkLen int
	}{
		{
			name:     "simple term",
			query:    "hello",
			checkLen: 1,
		},
		{
			name:     "required term",
			query:    "+hello",
			checkLen: 1,
		},
		{
			name:     "excluded term",
			query:    "-hello",
			checkLen: 1,
		},
		{
			name:     "prefix search",
			query:    "hel*",
			checkLen: 1,
		},
		{
			name:     "phrase search",
			query:    "\"hello world\"",
			checkLen: 1,
		},
		{
			name:     "mixed operators",
			query:    "+hello -world test*",
			checkLen: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := analyzer.AnalyzeQuery(tc.query)
			if len(result) != tc.checkLen {
				t.Errorf("expected %d tokens, got %d: %+v", tc.checkLen, len(result), result)
			}
		})
	}
}

func TestQueryTokenModifiers(t *testing.T) {
	analyzer := NewAnalyzer()

	// Test required modifier
	tokens := analyzer.AnalyzeQuery("+hello")
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if !tokens[0].Required {
		t.Error("expected token to be required")
	}

	// Test excluded modifier
	tokens = analyzer.AnalyzeQuery("-world")
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if !tokens[0].Excluded {
		t.Error("expected token to be excluded")
	}

	// Test prefix modifier
	tokens = analyzer.AnalyzeQuery("test*")
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if !tokens[0].Prefix {
		t.Error("expected token to be prefix")
	}

	// Test phrase
	tokens = analyzer.AnalyzeQuery("\"hello world\"")
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if !tokens[0].IsPhrase {
		t.Error("expected token to be phrase")
	}
}

// ==================== Inverted Index Tests ====================

func TestInvertedIndexBasic(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})

	// Index some documents
	idx.IndexDocument(1, "the quick brown fox")
	idx.IndexDocument(2, "the lazy dog")
	idx.IndexDocument(3, "quick quick quick fox")

	// Search for "quick"
	results, err := idx.Search("quick", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Check that results are sorted by score
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted by score: %v", results)
		}
	}
}

func TestInvertedIndexRemove(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})

	idx.IndexDocument(1, "hello world")
	idx.IndexDocument(2, "hello universe")

	results, _ := idx.Search("hello", 10)
	if len(results) != 2 {
		t.Fatalf("expected 2 results before removal, got %d", len(results))
	}

	idx.RemoveDocument(1)

	results, _ = idx.Search("hello", 10)
	if len(results) != 1 {
		t.Errorf("expected 1 result after removal, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != 2 {
		t.Errorf("expected doc2, got %d", results[0].DocID)
	}
}

func TestInvertedIndexPhraseSearch(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})

	idx.IndexDocument(1, "the quick brown fox jumps")
	idx.IndexDocument(2, "brown quick fox")
	idx.IndexDocument(3, "quick fox brown")

	// Search for phrase "quick brown"
	results, _ := idx.Search("\"quick brown\"", 10)

	// Only doc1 should match (has "quick brown" in order)
	if len(results) != 1 {
		t.Errorf("expected 1 result for phrase search, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != 1 {
		t.Errorf("expected doc1, got %d", results[0].DocID)
	}
}

func TestInvertedIndexPrefixSearch(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})

	idx.IndexDocument(1, "testing tests tested")
	idx.IndexDocument(2, "temporary template")
	idx.IndexDocument(3, "hello world")

	// Search with prefix "test*"
	results, _ := idx.Search("test*", 10)

	if len(results) != 1 {
		t.Errorf("expected 1 result for prefix search, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != 1 {
		t.Errorf("expected doc1, got %d", results[0].DocID)
	}
}

func TestInvertedIndexBooleanSearch(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})

	idx.IndexDocument(1, "quick brown fox")
	idx.IndexDocument(2, "slow brown dog")
	idx.IndexDocument(3, "quick yellow cat")

	// Search for "+brown -fox"
	results, _ := idx.Search("+brown -fox", 10)

	// Only doc2 should match (has brown, doesn't have fox)
	if len(results) != 1 {
		t.Errorf("expected 1 result for boolean search, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != 2 {
		t.Errorf("expected doc2, got %d", results[0].DocID)
	}
}

func TestInvertedIndexFuzzySearch(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})

	idx.IndexDocument(1, "hello world")
	idx.IndexDocument(2, "testing framework")

	// Search with slight typo using fuzzy modifier
	results, _ := idx.Search("helo~", 10)

	// Due to fuzzy matching (edit distance <= 2)
	if len(results) != 1 {
		t.Errorf("expected 1 result for fuzzy search, got %d", len(results))
	}
}

func TestBM25Ranking(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})

	// Index documents with varying term frequencies
	idx.IndexDocument(1, "fox")                 // 1 occurrence
	idx.IndexDocument(2, "fox fox fox fox fox") // 5 occurrences
	idx.IndexDocument(3, "fox fox")             // 2 occurrences

	results, _ := idx.Search("fox", 10)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Scores should be in descending order
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted by score: %v", results)
		}
	}
}

// ==================== Manager Tests ====================

func TestManagerCreateIndex(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "fts_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	err = mgr.CreateIndex("articles_content_idx", "articles", []string{"content"})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Creating duplicate should return error
	err = mgr.CreateIndex("articles_content_idx", "articles", []string{"content"})
	if err == nil {
		t.Error("expected error for duplicate index, got nil")
	}
}

func TestManagerDropIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	err = mgr.CreateIndex("articles_content_idx", "articles", []string{"content"})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	err = mgr.DropIndex("articles_content_idx")
	if err != nil {
		t.Errorf("DropIndex failed: %v", err)
	}

	// Dropping non-existent should return error
	err = mgr.DropIndex("articles_content_idx")
	if err == nil {
		t.Error("expected error for non-existent index, got nil")
	}
}

func TestManagerIndexDocument(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	err = mgr.CreateIndex("articles_content_idx", "articles", []string{"content"})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Index documents
	err = mgr.IndexDocument("articles_content_idx", 1, "the quick brown fox")
	if err != nil {
		t.Errorf("IndexDocument failed: %v", err)
	}

	err = mgr.IndexDocument("articles_content_idx", 2, "lazy dog sleeps")
	if err != nil {
		t.Errorf("IndexDocument failed: %v", err)
	}

	// Search
	results, err := mgr.Search("articles_content_idx", "fox", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestTSVectorCreation(t *testing.T) {
	text := "The quick brown fox jumps over the lazy dog"
	tsv := NewTSVector(text)

	// Should have terms with positions
	if len(tsv.Lexemes) == 0 {
		t.Error("TSVector should have lexemes")
	}

	// Check that stopwords are removed
	for term := range tsv.Lexemes {
		if term == "the" || term == "over" {
			t.Errorf("stopword %q should be removed", term)
		}
	}
}

func TestTSQueryCreation(t *testing.T) {
	tests := []struct {
		name  string
		query string
		terms int
	}{
		{"simple", "hello", 1},
		{"boolean", "+hello -world", 2},
		{"phrase", "\"hello world\"", 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tsq := NewTSQuery(tc.query)
			if len(tsq.Tokens) != tc.terms {
				t.Errorf("expected %d terms, got %d", tc.terms, len(tsq.Tokens))
			}
		})
	}
}

func TestTSQueryMatch(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		query    string
		expected bool
	}{
		{"simple match", "quick brown fox", "fox", true},
		{"no match", "quick brown fox", "dog", false},
		{"multiple words", "quick brown fox", "quick fox", true},
		{"required term missing", "quick brown fox", "+dog", false},
		{"excluded term present", "quick brown fox", "-fox", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tsv := NewTSVector(tc.text)
			tsq := NewTSQuery(tc.query)
			result := tsq.Match(tsv)
			if result != tc.expected {
				t.Errorf("Match(%q, %q) = %v, want %v", tc.text, tc.query, result, tc.expected)
			}
		})
	}
}

// ==================== Integration Tests ====================

func TestFullTextSearchWorkflow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	// Create index
	err = mgr.CreateIndex("products_idx", "products", []string{"description"})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Index some products
	products := []struct {
		id   uint64
		desc string
	}{
		{1, "Professional DSLR camera with advanced features"},
		{2, "Compact digital camera for beginners"},
		{3, "Camera lens for professional photography"},
		{4, "Smartphone with excellent camera capabilities"},
		{5, "Laptop computer with high performance"},
	}

	for _, p := range products {
		err := mgr.IndexDocument("products_idx", p.id, p.desc)
		if err != nil {
			t.Errorf("IndexDocument failed for %d: %v", p.id, err)
		}
	}

	// Search for "camera"
	results, err := mgr.Search("products_idx", "camera", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 4 {
		t.Errorf("expected 4 results for 'camera', got %d", len(results))
	}

	// Search for "+professional -smartphone"
	results, err = mgr.Search("products_idx", "+professional -smartphone", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	for _, r := range results {
		if r.DocID == 4 {
			t.Error("smartphone document should be excluded")
		}
	}

	// Search for "\"digital camera\""
	results, err = mgr.Search("products_idx", "\"digital camera\"", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 || results[0].DocID != 2 {
		t.Errorf("phrase search should return only doc2, got %v", results)
	}
}

func TestEmptyQueries(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})
	idx.IndexDocument(1, "hello world")

	// Empty query
	results, _ := idx.Search("", 10)
	if len(results) != 0 {
		t.Errorf("empty query should return no results, got %d", len(results))
	}

	// Only stopwords
	results, _ = idx.Search("the a an", 10)
	if len(results) != 0 {
		t.Errorf("stopwords-only query should return no results, got %d", len(results))
	}
}

func TestSpecialCharacters(t *testing.T) {
	idx := NewInvertedIndex("test_idx", "test_table", []string{"content"})

	// Index document with special characters
	idx.IndexDocument(1, "hello! world? foo-bar test_case")

	// Should still find "hello"
	results, _ := idx.Search("hello", 10)
	if len(results) != 1 {
		t.Errorf("expected 1 result for 'hello', got %d", len(results))
	}

	// Should find "world"
	results, _ = idx.Search("world", 10)
	if len(results) != 1 {
		t.Errorf("expected 1 result for 'world', got %d", len(results))
	}
}

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		a, b     string
		expected int
	}{
		{"", "", 0},
		{"hello", "", 5},
		{"", "world", 5},
		{"hello", "hello", 0},
		{"hello", "helo", 1},
		{"hello", "jello", 1},
		{"sitting", "kitten", 3},
	}

	for _, tc := range tests {
		t.Run(tc.a+"_"+tc.b, func(t *testing.T) {
			result := levenshteinDistance(tc.a, tc.b)
			if result != tc.expected {
				t.Errorf("levenshteinDistance(%q, %q) = %d, want %d", tc.a, tc.b, result, tc.expected)
			}
		})
	}
}

func TestBM25Score(t *testing.T) {
	// Test BM25 scoring properties
	k1 := 1.2
	b := 0.75
	avgdl := 100.0
	N := float64(1000)
	n := float64(10) // 10 documents contain the term

	// IDF calculation
	idf := math.Log((N - n + 0.5) / (n + 0.5))

	// Term frequency affects score positively
	score1 := calculateBM25Score(1, 100, k1, b, avgdl, idf)
	score2 := calculateBM25Score(5, 100, k1, b, avgdl, idf)
	if score2 <= score1 {
		t.Errorf("higher tf should give higher score: tf=1 score=%f, tf=5 score=%f", score1, score2)
	}

	// Longer documents get lower scores (same tf)
	score3 := calculateBM25Score(5, 200, k1, b, avgdl, idf)
	if score3 >= score2 {
		t.Errorf("longer document should get lower score: len=100 score=%f, len=200 score=%f", score2, score3)
	}
}

// Helper function for BM25 testing
func calculateBM25Score(tf int, docLen int, k1, b, avgdl, idf float64) float64 {
	tfFloat := float64(tf)
	dlFloat := float64(docLen)
	return idf * ((tfFloat * (k1 + 1)) / (tfFloat + k1*(1-b+b*dlFloat/avgdl)))
}

func TestIndexPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fts_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create and populate index
	idx := NewInvertedIndex("persist_test", "test_table", []string{"content"})
	idx.IndexDocument(1, "hello world")
	idx.IndexDocument(2, "testing persistence")

	// Save to disk
	err = idx.Save(tmpDir)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists in fts subdirectory
	indexPath := filepath.Join(tmpDir, "fts", "persist_test.json")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Errorf("Index file should exist at %s", indexPath)
	}

	// Load from disk
	loadedIdx, err := LoadInvertedIndex(tmpDir, "persist_test")
	if err != nil {
		t.Fatalf("LoadInvertedIndex failed: %v", err)
	}

	// Verify loaded index works
	results, err := loadedIdx.Search("hello", 10)
	if err != nil {
		t.Fatalf("Search on loaded index failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result from loaded index, got %d", len(results))
	}
}
