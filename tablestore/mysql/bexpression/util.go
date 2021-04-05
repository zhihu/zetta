package expression

// ExtractColumns extracts all columns from an expression.
func ExtractColumns(expr Expression) []*Column {
	// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
	result := make([]*Column, 0, 8)
	return extractColumns(result, expr, nil)
}

func extractColumns(result []*Column, expr Expression, filter func(*Column) bool) []*Column {
	switch v := expr.(type) {
	case *Column:
		if filter == nil || filter(v) {
			result = append(result, v)
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractColumns(result, arg, filter)
		}
	}
	return result
}
