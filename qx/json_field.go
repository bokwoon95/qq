package qx

import (
	"encoding/json"
)

// JSONField either represents a JSON column or a literal value that can be
// marshalled into a JSON string.
type JSONField struct {
	// JSONField will be one of the following:

	// 1) Literal JSONable value (almost all structs can be converted to JSON)
	value interface{}

	// 2) JSON column
	alias      string
	table      *TableInfo
	name       string
	descending *bool
	nullsfirst *bool
}

// ToSQL marshals a JSONField into an SQL query and args (as described in the
// JSONField internal struct comments). If the JSONField's table name appears
// in the excludeTableQualifiers list, the output column name will not be table
// qualified.
func (f JSONField) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	// 1) Literal JSONable value
	if f.value != nil {
		return "?", []interface{}{f.value}
	}

	// 2) JSON column
	var tableQualifier string
	if f.table != nil {
		if f.table.GetAlias() != "" {
			tableQualifier = f.table.GetAlias() + "."
		} else if f.table.GetName() != "" {
			tableQualifier = f.table.GetName() + "."
		}
	}
	for i := range excludeTableQualifiers {
		if tableQualifier == excludeTableQualifiers[i]+"." {
			tableQualifier = ""
			break
		}
	}
	columnName := tableQualifier + f.name
	if f.descending != nil {
		if *f.descending {
			columnName = columnName + " DESC"
		} else {
			columnName = columnName + " ASC"
		}
	}
	if f.nullsfirst != nil {
		if *f.nullsfirst {
			columnName = columnName + " NULLS FIRST"
		} else {
			columnName = columnName + " NULLS LAST"
		}
	}
	return columnName, nil
}

// NewJSONField returns a new JSONField representing a JSON column.
func NewJSONField(name string, tbl *TableInfo) JSONField {
	f := JSONField{
		name:  name,
		table: tbl,
	}
	f.table.Fields = append(f.table.Fields, &f)
	return f
}

// JSON returns a new JSONField representing a literal JSONable value. It
// returns an error indicating if the value can be marshalled into JSON.
func JSON(val interface{}) (JSONField, error) {
	f := JSONField{value: val}
	_, err := json.Marshal(val)
	if err != nil {
		return f, err
	}
	return f, nil
}

// MustJSON is like JSON but it panics on error.
func MustJSON(val interface{}) JSONField {
	f, err := JSON(val)
	if err != nil {
		panic(err)
	}
	return f
}

// Set returns a FieldValueSet associating the JSONField to the value i.e.
// 'SET field = value'.
func (f JSONField) Set(value interface{}) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: value,
	}
}

// SetJSON returns a FieldValueSet associating the JSONField to the JSONable
// value i.e. 'SET field = value'. Internally it uses MustJSON, which means it
// will panic if the value cannot be marshalled into JSON.
func (f JSONField) SetJSON(value interface{}) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: MustJSON(value).value,
	}
}

// As returns a new JSONField with the new field Alias i.e. 'field AS Alias'.
func (f JSONField) As(alias string) JSONField {
	f.alias = alias
	return f
}

// Asc returns a new JSONField indicating that it should be ordered in
// ascending order i.e. 'ORDER BY field ASC'.
func (f JSONField) Asc() JSONField {
	desc := false
	f.descending = &desc
	return f
}

// Desc returns a new JSONField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (f JSONField) Desc() JSONField {
	desc := true
	f.descending = &desc
	return f
}

// NullsFirst returns a new JSONField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f JSONField) NullsFirst() JSONField {
	nullsfirst := true
	f.nullsfirst = &nullsfirst
	return f
}

// NullsLast returns a new JSONField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f JSONField) NullsLast() JSONField {
	nullsfirst := false
	f.nullsfirst = &nullsfirst
	return f
}

// IsNull returns an 'A IS NULL' Predicate.
func (f JSONField) IsNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNull,
		Field:    f,
	}
}

// IsNotNull returns an 'A IS NOT NULL' Predicate.
func (f JSONField) IsNotNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNotNull,
		Field:    f,
	}
}

// String implements the fmt.Stringer interface. It returns the string
// representation of a JSONField.
func (f JSONField) String() string {
	query, args := f.ToSQL(nil)
	return MySQLInterpolateSQL(query, args...)
}

// GetAlias implements the Field interface. It returns the Alias of the
// JSONField.
func (f JSONField) GetAlias() string {
	return f.alias
}

// GetName implements the Field interface. It returns the Name of the
// JSONField.
func (f JSONField) GetName() string {
	return f.name
}
