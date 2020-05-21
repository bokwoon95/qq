package qx

// BooleanField either represents a boolean column or a literal bool value.
type BooleanField struct {
	// BooleanField will be one of the following:

	// 1) Literal bool value
	// Examples of literal bool values:
	// | query | args |
	// |-------|------|
	// | ?     | true |
	value *bool

	// 2) Boolean column
	// Examples of boolean columns:
	// | query            | args |
	// |------------------|------|
	// | users.is_created |      |
	// | is_created       |      |
	alias      string
	table      *TableInfo
	name       string
	descending *bool
	nullsfirst *bool
}

// ToSQL marshals a BooleanField into an SQL query and args (as described in
// the BooleanField internal struct comments). If the BooleanField's table name
// appears in the excludeTableQualifiers list, the output column name will not
// be table qualified.
func (f BooleanField) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	// 1) Literal bool value
	if f.value != nil {
		return "?", []interface{}{*f.value}
	}

	// 2) Boolean column
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

// NewBooleanField returns a new BooleanField representing a boolean column.
func NewBooleanField(name string, table *TableInfo) BooleanField {
	f := BooleanField{
		name:  name,
		table: table,
	}
	f.table.Fields = append(f.table.Fields, f)
	return f
}

// Bool returns a new Boolean Field representing a literal bool value.
func Bool(b bool) BooleanField {
	return BooleanField{
		value: &b,
	}
}

// Set returns a FieldValueSet associating the BooleanField to the value i.e.
// 'SET field = value'.
func (f BooleanField) Set(val interface{}) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: val,
	}
}

// SetBool returns a FieldValueSet associating the BooleanField to the bool
// value i.e. 'SET field = value'.
func (f BooleanField) SetBool(val bool) FieldValueSet {
	return f.Set(val)
}

// As returns a new BooleanField with the new field Alias i.e. 'field AS
// Alias'.
func (f BooleanField) As(alias string) BooleanField {
	f.alias = alias
	return f
}

// Asc returns a new BooleanField indicating that it should be ordered in
// ascending order i.e. 'ORDER BY field ASC'.
func (f BooleanField) Asc() BooleanField {
	desc := false
	f.descending = &desc
	return f
}

// Desc returns a new BooleanField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (f BooleanField) Desc() BooleanField {
	desc := true
	f.descending = &desc
	return f
}

// NullsFirst returns a new BooleanField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f BooleanField) NullsFirst() BooleanField {
	nullsfirst := true
	f.nullsfirst = &nullsfirst
	return f
}

// NullsLast returns a new BooleanField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f BooleanField) NullsLast() BooleanField {
	nullsfirst := false
	f.nullsfirst = &nullsfirst
	return f
}

// Not returns a 'NOT X' Predicate.
func (f BooleanField) Not() Predicate {
	return CustomPredicate{
		Format: "NOT ?",
		Values: []interface{}{f},
	}
}

// IsNull returns an 'A IS NULL' Predicate.
func (f BooleanField) IsNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNull,
		Field:    f,
	}
}

// IsNotNull returns an 'A IS NOT NULL' Predicate.
func (f BooleanField) IsNotNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNotNull,
		Field:    f,
	}
}

// Eq returns an 'A = B' Predicate. It only accepts BooleanField.
func (f BooleanField) Eq(field BooleanField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: field,
	}
}

// Ne returns an 'A <> B' Predicate. It only accepts BooleanField.
func (f BooleanField) Ne(field BooleanField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: field,
	}
}

// String implements the fmt.Stringer interface. It returns the string
// representation of a BooleanField.
func (f BooleanField) String() string {
	query, args := f.ToSQL(nil)
	return MySQLInterpolateSQL(query, args...)
}

// GetAlias implements the Field interface. It returns the Alias of the
// BooleanField.
func (f BooleanField) GetAlias() string {
	return f.alias
}

// GetName implements the Field interface. It returns the Name of the
// BooleanField.
func (f BooleanField) GetName() string {
	return f.name
}

// AssertPredicate implements the Predicate interface.
func (f BooleanField) AssertPredicate() {}
