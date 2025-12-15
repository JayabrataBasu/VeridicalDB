// Package fts provides full-text search functionality for VeridicalDB.
// It includes text analysis, inverted indexing, and query execution.
package fts

import (
	"regexp"
	"strings"
	"unicode"
)

// Analyzer performs text analysis for full-text search.
// It handles tokenization, normalization, stop word removal, and stemming.
type Analyzer struct {
	stopWords     map[string]bool
	stemmer       Stemmer
	minTokenLen   int
	maxTokenLen   int
	caseSensitive bool
}

// Stemmer defines the interface for word stemming algorithms.
type Stemmer interface {
	Stem(word string) string
}

// PorterStemmer implements a simplified Porter Stemmer algorithm.
type PorterStemmer struct{}

// Stem applies Porter stemming rules to reduce a word to its root form.
func (s *PorterStemmer) Stem(word string) string {
	if len(word) < 3 {
		return word
	}

	// Step 1a: SSES -> SS, IES -> I, SS -> SS, S -> (remove)
	if strings.HasSuffix(word, "sses") {
		word = word[:len(word)-2]
	} else if strings.HasSuffix(word, "ies") {
		word = word[:len(word)-2]
	} else if strings.HasSuffix(word, "ss") {
		// Keep as is
	} else if strings.HasSuffix(word, "s") && len(word) > 3 {
		word = word[:len(word)-1]
	}

	// Step 1b: Handle -EED, -ED, -ING endings
	if strings.HasSuffix(word, "eed") && len(word) > 4 {
		word = word[:len(word)-1]
	} else if strings.HasSuffix(word, "ed") && len(word) > 4 && containsVowel(word[:len(word)-2]) {
		word = word[:len(word)-2]
		word = step1bHelper(word)
	} else if strings.HasSuffix(word, "ing") && len(word) > 5 && containsVowel(word[:len(word)-3]) {
		word = word[:len(word)-3]
		word = step1bHelper(word)
	}

	// Step 1c: Y -> I if stem contains vowel
	if strings.HasSuffix(word, "y") && len(word) > 2 && containsVowel(word[:len(word)-1]) {
		word = word[:len(word)-1] + "i"
	}

	// Step 2: Handle common suffixes
	suffixReplacements := map[string]string{
		"ational": "ate",
		"tional":  "tion",
		"enci":    "ence",
		"anci":    "ance",
		"izer":    "ize",
		"abli":    "able",
		"alli":    "al",
		"entli":   "ent",
		"eli":     "e",
		"ousli":   "ous",
		"ization": "ize",
		"ation":   "ate",
		"ator":    "ate",
		"alism":   "al",
		"iveness": "ive",
		"fulness": "ful",
		"ousness": "ous",
		"aliti":   "al",
		"iviti":   "ive",
		"biliti":  "ble",
	}

	for suffix, replacement := range suffixReplacements {
		if strings.HasSuffix(word, suffix) && len(word)-len(suffix) > 2 {
			word = word[:len(word)-len(suffix)] + replacement
			break
		}
	}

	// Step 3: Handle more suffixes
	step3Replacements := map[string]string{
		"icate": "ic",
		"ative": "",
		"alize": "al",
		"iciti": "ic",
		"ical":  "ic",
		"ful":   "",
		"ness":  "",
	}

	for suffix, replacement := range step3Replacements {
		if strings.HasSuffix(word, suffix) && len(word)-len(suffix) > 2 {
			word = word[:len(word)-len(suffix)] + replacement
			break
		}
	}

	// Step 4: Remove suffixes
	step4Suffixes := []string{
		"al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement",
		"ment", "ent", "ion", "ou", "ism", "ate", "iti", "ous", "ive", "ize",
	}

	for _, suffix := range step4Suffixes {
		if strings.HasSuffix(word, suffix) && len(word)-len(suffix) > 3 {
			if suffix == "ion" {
				stem := word[:len(word)-3]
				if strings.HasSuffix(stem, "s") || strings.HasSuffix(stem, "t") {
					word = stem
				}
			} else {
				word = word[:len(word)-len(suffix)]
			}
			break
		}
	}

	// Step 5a: Remove trailing E
	if strings.HasSuffix(word, "e") && len(word) > 3 {
		word = word[:len(word)-1]
	}

	// Step 5b: Remove trailing double consonant -> single
	if len(word) > 2 && word[len(word)-1] == word[len(word)-2] && !isVowel(rune(word[len(word)-1])) {
		if word[len(word)-1] != 'l' && word[len(word)-1] != 's' && word[len(word)-1] != 'z' {
			word = word[:len(word)-1]
		}
	}

	return word
}

func step1bHelper(word string) string {
	if strings.HasSuffix(word, "at") || strings.HasSuffix(word, "bl") || strings.HasSuffix(word, "iz") {
		return word + "e"
	}
	if len(word) > 1 && word[len(word)-1] == word[len(word)-2] && !isVowel(rune(word[len(word)-1])) {
		if word[len(word)-1] != 'l' && word[len(word)-1] != 's' && word[len(word)-1] != 'z' {
			return word[:len(word)-1]
		}
	}
	return word
}

func containsVowel(s string) bool {
	for _, r := range s {
		if isVowel(r) {
			return true
		}
	}
	return false
}

func isVowel(r rune) bool {
	switch r {
	case 'a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U':
		return true
	}
	return false
}

// DefaultStopWords returns the default English stop words list.
func DefaultStopWords() map[string]bool {
	words := []string{
		"a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
		"has", "he", "in", "is", "it", "its", "of", "on", "or", "that",
		"the", "to", "was", "were", "will", "with", "the", "this", "but",
		"they", "have", "had", "what", "when", "where", "who", "which",
		"why", "how", "all", "each", "every", "both", "few", "more",
		"most", "other", "some", "such", "no", "nor", "not", "only",
		"own", "same", "so", "than", "too", "very", "can", "just", "should",
		"now", "i", "me", "my", "we", "our", "you", "your", "do", "does",
		"did", "doing", "would", "could", "might", "must", "shall",
		"about", "above", "after", "again", "against", "am", "any",
		"because", "been", "before", "being", "below", "between", "down",
		"during", "further", "here", "herself", "himself", "if", "into",
		"itself", "myself", "off", "once", "out", "over", "ourselves",
		"she", "them", "themselves", "then", "there", "these", "those",
		"through", "under", "until", "up", "us", "while", "whom", "yours",
		"yourself", "yourselves", "her", "him", "his",
	}
	m := make(map[string]bool, len(words))
	for _, w := range words {
		m[w] = true
	}
	return m
}

// NewAnalyzer creates a new text analyzer with default settings.
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		stopWords:     DefaultStopWords(),
		stemmer:       &PorterStemmer{},
		minTokenLen:   2,
		maxTokenLen:   64,
		caseSensitive: false,
	}
}

// NewAnalyzerWithOptions creates a new analyzer with custom settings.
func NewAnalyzerWithOptions(stopWords map[string]bool, stemmer Stemmer, minLen, maxLen int, caseSensitive bool) *Analyzer {
	return &Analyzer{
		stopWords:     stopWords,
		stemmer:       stemmer,
		minTokenLen:   minLen,
		maxTokenLen:   maxLen,
		caseSensitive: caseSensitive,
	}
}

// Token represents a single token extracted from text.
type Token struct {
	Term     string // The normalized/stemmed term
	Original string // The original text
	Position int    // Position in the original text (word position, not byte)
	Start    int    // Start byte offset in original text
	End      int    // End byte offset in original text
}

// wordPattern matches word characters
var wordPattern = regexp.MustCompile(`[\p{L}\p{N}]+`)

// Analyze tokenizes and processes the input text.
func (a *Analyzer) Analyze(text string) []Token {
	if text == "" {
		return nil
	}

	matches := wordPattern.FindAllStringIndex(text, -1)
	tokens := make([]Token, 0, len(matches))

	position := 0
	for _, match := range matches {
		original := text[match[0]:match[1]]
		term := original

		// Normalize case
		if !a.caseSensitive {
			term = strings.ToLower(term)
		}

		// Check length constraints
		if len(term) < a.minTokenLen || len(term) > a.maxTokenLen {
			continue
		}

		// Skip stop words
		if a.stopWords != nil && a.stopWords[term] {
			position++
			continue
		}

		// Apply stemming
		if a.stemmer != nil {
			term = a.stemmer.Stem(term)
		}

		tokens = append(tokens, Token{
			Term:     term,
			Original: original,
			Position: position,
			Start:    match[0],
			End:      match[1],
		})
		position++
	}

	return tokens
}

// AnalyzeQuery tokenizes a search query, handling special query syntax.
func (a *Analyzer) AnalyzeQuery(query string) []QueryToken {
	if query == "" {
		return nil
	}

	tokens := make([]QueryToken, 0)
	query = strings.TrimSpace(query)

	// Handle phrase queries (quoted strings)
	phrasePattern := regexp.MustCompile(`"([^"]+)"`)
	phraseMatches := phrasePattern.FindAllStringSubmatchIndex(query, -1)

	lastEnd := 0
	for _, match := range phraseMatches {
		// Process text before the phrase
		if match[0] > lastEnd {
			beforePhrase := query[lastEnd:match[0]]
			tokens = append(tokens, a.parseQueryTerms(beforePhrase)...)
		}

		// Add the phrase as a single token
		phrase := query[match[2]:match[3]]
		tokens = append(tokens, QueryToken{
			Term:     phrase,
			IsPhrase: true,
			Required: true,
		})
		lastEnd = match[1]
	}

	// Process remaining text after last phrase
	if lastEnd < len(query) {
		remaining := query[lastEnd:]
		tokens = append(tokens, a.parseQueryTerms(remaining)...)
	}

	return tokens
}

// QueryToken represents a token in a search query.
type QueryToken struct {
	Term     string
	IsPhrase bool // Whether this is a phrase (multiple words)
	Required bool // + prefix means required
	Excluded bool // - prefix means excluded
	Prefix   bool // * suffix means prefix match
	Fuzzy    bool // ~ suffix means fuzzy match
}

func (a *Analyzer) parseQueryTerms(text string) []QueryToken {
	words := strings.Fields(text)
	tokens := make([]QueryToken, 0, len(words))

	for _, word := range words {
		word = strings.TrimSpace(word)
		if word == "" {
			continue
		}

		token := QueryToken{}

		// Check for modifiers
		if strings.HasPrefix(word, "+") {
			token.Required = true
			word = word[1:]
		} else if strings.HasPrefix(word, "-") {
			token.Excluded = true
			word = word[1:]
		}

		if strings.HasSuffix(word, "*") {
			token.Prefix = true
			word = word[:len(word)-1]
		} else if strings.HasSuffix(word, "~") {
			token.Fuzzy = true
			word = word[:len(word)-1]
		}

		// Normalize the term
		if !a.caseSensitive {
			word = strings.ToLower(word)
		}

		// Skip empty terms and stop words (unless required)
		if word == "" {
			continue
		}
		if a.stopWords != nil && a.stopWords[word] && !token.Required {
			continue
		}

		// Apply stemming (unless it's a prefix search)
		if !token.Prefix && a.stemmer != nil {
			word = a.stemmer.Stem(word)
		}

		token.Term = word
		tokens = append(tokens, token)
	}

	return tokens
}

// NormalizeText performs basic text normalization without tokenization.
func NormalizeText(text string) string {
	// Convert to lowercase
	text = strings.ToLower(text)
	// Remove extra whitespace
	text = strings.Join(strings.Fields(text), " ")
	return text
}

// ExtractHighlights returns text snippets containing the search terms.
func ExtractHighlights(text string, terms []string, contextWords int) []string {
	if contextWords < 1 {
		contextWords = 5
	}

	words := strings.Fields(text)
	highlights := make([]string, 0)
	termSet := make(map[string]bool)

	for _, t := range terms {
		termSet[strings.ToLower(t)] = true
	}

	for i, word := range words {
		normalized := strings.ToLower(word)
		// Remove punctuation for matching
		normalized = strings.TrimFunc(normalized, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsNumber(r)
		})

		if termSet[normalized] {
			start := i - contextWords
			if start < 0 {
				start = 0
			}
			end := i + contextWords + 1
			if end > len(words) {
				end = len(words)
			}

			snippet := strings.Join(words[start:end], " ")
			if start > 0 {
				snippet = "..." + snippet
			}
			if end < len(words) {
				snippet = snippet + "..."
			}
			highlights = append(highlights, snippet)
		}
	}

	return highlights
}
