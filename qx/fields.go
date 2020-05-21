package qx

import (
	"strings"
)

// FieldLiteral is a Field where its underlying string is literally plugged
// into the SQL query.
type FieldLiteral string

// FieldLiterals
const (
	_NULL FieldLiteral = "NULL"
)

// ToSQL returns the underlying string of the FieldLiteral.
func (f FieldLiteral) ToSQL([]string) (string, []interface{}) {
	return string(f), nil
}

// GetAlias implements the Field interface. It always returns an empty string
// because FieldLiterals do not have aliases.
func (f FieldLiteral) GetAlias() string {
	return ""
}

// GetName implements the Field interface. It returns the FieldLiteral's
// underlying string as the name.
func (f FieldLiteral) GetName() string {
	return string(f)
}

// Fields represents the "field1, field2, etc..." SQL construct.
type Fields []Field

// WriteSQL will write the a slice of Fields into the buffer and args as
// described in the Fields description. The result is prepended and appended
// with the prependWith and appendWith arguments. The list of table qualifiers
// to be excluded is propagated down to the individual Fields.
//
// If there are no Fields present, nothing will be written into the buffer.
// WriteSQL returns a flag indicating whether anything was written into the
// buffer.
func (fs Fields) WriteSQL(buf *strings.Builder, args *[]interface{}, prependWith, appendWith string, excludeTableQualifiers []string) (written bool) {
	var fieldsQueries []string
	var fieldsArgs []interface{}
	for i := range fs {
		if fs[i] == nil {
			fs[i] = _NULL
		}
		subquery, subargs := fs[i].ToSQL(excludeTableQualifiers)
		if subquery == "" {
			continue
		}
		fieldsQueries = append(fieldsQueries, subquery)
		fieldsArgs = append(fieldsArgs, subargs...)
	}
	if len(fieldsQueries) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(prependWith + strings.Join(fieldsQueries, ", ") + appendWith)
		*args = append(*args, fieldsArgs...)
		return true
	}
	return false
}

// WriteSQLWithAlias is exactly like WriteSQL, but appends each field (i.e.
// field1 AS alias1, field2 AS alias2, ...) with its alias if it has one.
func (fs Fields) WriteSQLWithAlias(buf *strings.Builder, args *[]interface{}, prependWith, appendWith string, excludeTableQualifiers []string) (written bool) {
	var fieldsQueries []string
	var fieldsArgs []interface{}
	for i := range fs {
		if fs[i] == nil {
			fs[i] = _NULL
		}
		subquery, subargs := fs[i].ToSQL(excludeTableQualifiers)
		if subquery == "" {
			continue
		}
		if fs[i].GetAlias() != "" {
			subquery = subquery + " AS " + fs[i].GetAlias()
		}
		fieldsQueries = append(fieldsQueries, subquery)
		fieldsArgs = append(fieldsArgs, subargs...)
	}
	if len(fieldsQueries) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(prependWith + strings.Join(fieldsQueries, ", ") + appendWith)
		*args = append(*args, fieldsArgs...)
		return true
	}
	return false
}

// FieldValueSet represents a Field and Value set. Its usage appears in both
// the UPDATE and INSERT queries whenever values are assigned to columns e.g.
// 'SET field = value'.
type FieldValueSet struct {
	Field Field
	Value interface{}
}

// FieldValueSets is a list of FieldValueSets, when translated to SQL it looks
// something like "SET field1 = value1, field2 = value2, etc...".
type FieldValueSets []FieldValueSet

// WriteSQL will write the SET clause into the buffer and args as described in
// the FieldValueSets description. If there are no FieldValueSets, it simply
// writes nothing to the buffer. It returns a flag indicating whether anything
// was written into the buffer.
func (sets FieldValueSets) WriteSQL(buf *strings.Builder, args *[]interface{}, prependWith, appendWith string, excludeTableQualifiers []string) bool {
	setsQueries, setsArgs := []string{}, []interface{}{}
	for i := range sets {
		if sets[i].Field == nil {
			// can't convert a nil Field to NULL here, the left hand field of a SET X = Y must actually be a column
			continue
		}
		subquery, subargs := sets[i].Field.ToSQL(excludeTableQualifiers)
		if subquery == "" {
			continue
		}
		if field, ok := sets[i].Value.(Field); ok && field != nil {
			q, a := field.ToSQL(excludeTableQualifiers)
			subquery = subquery + " = " + q
			subargs = append(subargs, a...)
		} else {
			subquery = subquery + " = ?"
			subargs = append(subargs, sets[i].Value)
		}
		setsQueries = append(setsQueries, subquery)
		setsArgs = append(setsArgs, subargs...)
	}
	if len(setsQueries) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(prependWith + strings.Join(setsQueries, ", ") + appendWith)
		*args = append(*args, setsArgs...)
		return true
	}
	return false
}
