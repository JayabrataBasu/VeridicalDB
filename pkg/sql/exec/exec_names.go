package exec

// splitQualifiedName splits "table.column" into ["table", "column"]
func splitQualifiedName(name string) []string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return []string{name[:i], name[i+1:]}
		}
	}
	return []string{name}
}
