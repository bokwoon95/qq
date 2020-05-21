package qx

// StringField either represents a string column or a literal string value.
type StringField struct {
	// StringField will be one of the following:

	// 1) Literal string value
	// Examples of literal string values:
	// | query | args |
	// |-------|------|
	// | ?     | abcd |
	value *string

	// 2) String column
	// Examples of boolean columns:
	// | query       | args |
	// |-------------|------|
	// | users.name  |      |
	// | name        |      |
	// | users.email |      |
	alias      string
	table      *TableInfo
	name       string
	descending *bool
	nullsfirst *bool
}

// ToSQL marshals a StringField into an SQL query and args (as described in the
// StringField internal struct comments). If the BooleanField's table name
// appears in the excludeTableQualifiers list, the output column name will not
// be table qualified.
func (f StringField) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	// 1) Literal string value
	if f.value != nil {
		return "?", []interface{}{*f.value}
	}

	// 2) String column
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

// NewStringField returns a new StringField representing a boolean column.
func NewStringField(name string, table *TableInfo) StringField {
	f := StringField{
		name:  name,
		table: table,
	}
	f.table.Fields = append(f.table.Fields, &f)
	return f
}

// String returns a new StringField representing a literal string value.
func String(s string) StringField {
	return StringField{
		value: &s,
	}
}

// Set returns a FieldValueSet associating the StringField to the value i.e.
// 'SET field = value'.
func (f StringField) Set(value interface{}) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: value,
	}
}

// SetString returns a FieldValueSet associating the StringField to the string
// value i.e. 'SET field = value'.
func (f StringField) SetString(s string) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: s,
	}
}

// As returns a new StringField with the new field Alias i.e. 'field AS Alias'.
func (f StringField) As(alias string) StringField {
	f.alias = alias
	return f
}

// Asc returns a new StringField indicating that it should be ordered in
// ascending order i.e. 'ORDER BY field ASC'.
func (f StringField) Asc() StringField {
	desc := false
	f.descending = &desc
	return f
}

// Desc returns a new StringField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (f StringField) Desc() StringField {
	desc := true
	f.descending = &desc
	return f
}

// NullsFirst returns a new StringField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f StringField) NullsFirst() StringField {
	nullsfirst := true
	f.nullsfirst = &nullsfirst
	return f
}

// NullsLast returns a new StringField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f StringField) NullsLast() StringField {
	nullsfirst := false
	f.nullsfirst = &nullsfirst
	return f
}

// IsNull returns an 'A IS NULL' Predicate.
func (f StringField) IsNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNull,
		Field:    f,
	}
}

// IsNotNull returns an 'A IS NOT NULL' Predicate.
func (f StringField) IsNotNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNotNull,
		Field:    f,
	}
}

// Eq returns an 'A = B' Predicate. It only accepts StringField.
func (f StringField) Eq(field StringField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: field,
	}
}

// Ne returns an 'A <> B' Predicate. It only accepts StringField.
func (f StringField) Ne(field StringField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: field,
	}
}

// Gt returns an 'A > B' Predicate. It only accepts StringField.
func (f StringField) Gt(field StringField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGt,
		LeftField:  f,
		RightField: field,
	}
}

// Ge returns an 'A >= B' Predicate. It only accepts StringField.
func (f StringField) Ge(field StringField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGe,
		LeftField:  f,
		RightField: field,
	}
}

// Lt returns an 'A < B' Predicate. It only accepts StringField.
func (f StringField) Lt(field StringField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLt,
		LeftField:  f,
		RightField: field,
	}
}

// Le returns an 'A <= B' Predicate. It only accepts StringField.
func (f StringField) Le(field StringField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLe,
		LeftField:  f,
		RightField: field,
	}
}

// EqString returns an 'A = B' Predicate. It only accepts string.
func (f StringField) EqString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: String(s),
	}
}

// NeString returns an 'A <> B' Predicate. It only accepts string.
func (f StringField) NeString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: String(s),
	}
}

// GtString returns an 'A > B' Predicate. It only accepts string.
func (f StringField) GtString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGt,
		LeftField:  f,
		RightField: String(s),
	}
}

// GeString returns an 'A >= B' Predicate. It only accepts string.
func (f StringField) GeString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGe,
		LeftField:  f,
		RightField: String(s),
	}
}

// LtString returns an 'A < B' Predicate. It only accepts string.
func (f StringField) LtString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLt,
		LeftField:  f,
		RightField: String(s),
	}
}

// LeString returns an 'A <= B' Predicate. It only accepts string.
func (f StringField) LeString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLe,
		LeftField:  f,
		RightField: String(s),
	}
}

// LikeString returns an 'A LIKE B' Predicate. It only accepts string.
func (f StringField) LikeString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLike,
		LeftField:  f,
		RightField: String(s),
	}
}

// NotLikeString returns an 'A NOT LIKE B' Predicate. It only accepts string.
func (f StringField) NotLikeString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNotLike,
		LeftField:  f,
		RightField: String(s),
	}
}

// ILikeString returns an 'A ILIKE B' Predicate. It only accepts string.
func (f StringField) ILikeString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateILike,
		LeftField:  f,
		RightField: String(s),
	}
}

// NotILikeString returns an 'A NOT ILIKE B' Predicate. It only accepts string.
func (f StringField) NotILikeString(s string) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNotILike,
		LeftField:  f,
		RightField: String(s),
	}
}

// String implements the fmt.Stringer interface. It returns the string
// representation of a StringField.
func (f StringField) String() string {
	query, args := f.ToSQL(nil)
	return MySQLInterpolateSQL(query, args...)
}

// GetAlias implements the Field interface. It returns the Alias of the
// StringField.
func (f StringField) GetAlias() string {
	return f.alias
}

// GetName implements the Field interface. It returns the Name of the
// StringField.
func (f StringField) GetName() string {
	return f.name
}
