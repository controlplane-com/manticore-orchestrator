package generator

import "strings"

// EscapeSQL escapes single quotes in a string for Manticore SQL insertion.
// Manticore uses backslash escaping (\') rather than SQL-standard double quotes ('').
func EscapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// QuoteString wraps a string in single quotes with proper escaping.
func QuoteString(s string) string {
	return "'" + EscapeSQL(s) + "'"
}
