package qx

import (
	"time"
)

// TimeField either represents a time column or a literal time.Time value.
type TimeField struct {
	// TimeField will be one of the following:

	// 1) Literal time.Time value
	// Examples of literal string values:
	// | query | args       |
	// |-------|------------|
	// | ?     | time.Now() |
	value *time.Time

	// 2) Time column
	// Examples of time columns:
	// | query            | args |
	// |------------------|------|
	// | users.created_at |      |
	// | created_at       |      |
	// | events.start_at  |      |
	alias      string
	table      *TableInfo
	name       string
	descending *bool
	nullsfirst *bool
}

// ToSQL marshals a TimeField into an SQL query and args (as described in the
// TimeField internal struct comments). If the TimeFields's table name
// appears in the excludeTableQualifiers list, the output column name will not
// be table qualified.
func (f TimeField) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	// 1) Literal time.Time value
	if f.value != nil {
		return "?", []interface{}{*f.value}
	}

	// 2) Time column
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

// NewTimeField returns a new TimeField representing a time column.
func NewTimeField(name string, tbl *TableInfo) TimeField {
	f := TimeField{
		name:  name,
		table: tbl,
	}
	f.table.Fields = append(f.table.Fields, &f)
	return f
}

// Time returns a new TimeField representing a literal time.Time value.
func Time(t time.Time) TimeField {
	return TimeField{
		value: &t,
	}
}

// Set returns a FieldValueSet associating the TimeField to the value i.e.
// 'SET field = value'.
func (f TimeField) Set(value interface{}) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: value,
	}
}

// Set returns a FieldValueSet associating the TimeField to the time.Time value
// i.e.  'SET field = value'.
func (f TimeField) SetTime(value time.Time) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: value,
	}
}

// As returns a new TimeField with the new field Alias i.e. 'field AS Alias'.
func (f TimeField) As(alias string) TimeField {
	f.alias = alias
	return f
}

// Asc returns a new TimeField indicating that it should be ordered in
// ascending order i.e. 'ORDER BY field ASC'.
func (f TimeField) Asc() TimeField {
	desc := false
	f.descending = &desc
	return f
}

// Desc returns a new TimeField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (f TimeField) Desc() TimeField {
	desc := true
	f.descending = &desc
	return f
}

// NullsFirst returns a new TimeField indicating that it should be ordered with
// nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f TimeField) NullsFirst() TimeField {
	nullsfirst := true
	f.nullsfirst = &nullsfirst
	return f
}

// NullsLast returns a new TimeField indicating that it should be ordered with
// nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f TimeField) NullsLast() TimeField {
	nullsfirst := false
	f.nullsfirst = &nullsfirst
	return f
}

// IsNull returns an 'A IS NULL' Predicate.
func (f TimeField) IsNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNull,
		Field:    f,
	}
}

// IsNotNull returns an 'A IS NOT NULL' Predicate.
func (f TimeField) IsNotNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNotNull,
		Field:    f,
	}
}

// Eq returns an 'A = B' Predicate. It only accepts TimeField.
func (f TimeField) Eq(field TimeField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: field,
	}
}

// EqTime returns an 'A = B' Predicate. It only accepts time.Time.
func (f TimeField) EqTime(t time.Time) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: Time(t),
	}
}

// Ne returns an 'A <> B' Predicate. It only accepts TimeField.
func (f TimeField) Ne(field TimeField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: field,
	}
}

// NeTime returns an 'A <> B' Predicate. It only accepts time.Time.
func (f TimeField) NeTime(t time.Time) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: Time(t),
	}
}

// Gt returns an 'A > B' Predicate. It only accepts TimeField.
func (f TimeField) Gt(field TimeField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGt,
		LeftField:  f,
		RightField: field,
	}
}

// GtTime returns an 'A > B' Predicate. It only accepts time.Time.
func (f TimeField) GtTime(t time.Time) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGt,
		LeftField:  f,
		RightField: Time(t),
	}
}

// Ge returns an 'A >= B' Predicate. It only accepts TimeField.
func (f TimeField) Ge(field TimeField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGe,
		LeftField:  f,
		RightField: field,
	}
}

// GeTime returns an 'A >= B' Predicate. It only accepts time.Time.
func (f TimeField) GeTime(t time.Time) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGe,
		LeftField:  f,
		RightField: Time(t),
	}
}

// Lt returns an 'A < B' Predicate. It only accepts TimeField.
func (f TimeField) Lt(field TimeField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLt,
		LeftField:  f,
		RightField: field,
	}
}

// LtTime returns an 'A < B' Predicate. It only accepts time.Time.
func (f TimeField) LtTime(t time.Time) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLt,
		LeftField:  f,
		RightField: Time(t),
	}
}

// Le returns an 'A <= B' Predicate. It only accepts TimeField.
func (f TimeField) Le(field TimeField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLe,
		LeftField:  f,
		RightField: field,
	}
}

// LeTime returns an 'A <= B' Predicate. It only accepts time.Time.
func (f TimeField) LeTime(t time.Time) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLe,
		LeftField:  f,
		RightField: Time(t),
	}
}

// Between returns an 'A BETWEEN X AND Y' Predicate. It only accepts TimeField.
func (f TimeField) Between(start, end TimeField) Predicate {
	return TernaryPredicate{
		Operator: PredicateBetween,
		Field:    f,
		FieldX:   start,
		FieldY:   end,
	}
}

// BetweenTime returns an 'A BETWEEN X AND Y' Predicate. It only accepts
// time.Time.
func (f TimeField) BetweenTime(start, end time.Time) Predicate {
	return TernaryPredicate{
		Operator: PredicateBetween,
		Field:    f,
		FieldX:   Time(start),
		FieldY:   Time(end),
	}
}

// NotBetween returns an 'A NOT BETWEEN X AND Y' Predicate. It only accepts
// TimeField.
func (f TimeField) NotBetween(start, end TimeField) Predicate {
	return TernaryPredicate{
		Operator: PredicateNotBetween,
		Field:    f,
		FieldX:   start,
		FieldY:   end,
	}
}

// NotBetweenTime returns an 'A NOT BETWEEN X AND Y' Predicate. It only accepts
// time.Time.
func (f TimeField) NotBetweenTime(start, end time.Time) Predicate {
	return TernaryPredicate{
		Operator: PredicateNotBetween,
		Field:    f,
		FieldX:   Time(start),
		FieldY:   Time(end),
	}
}

// BetweenSymmetric returns an 'A BETWEEN SYMMETRIC X AND Y' Predicate. It only
// accepts TimeField.
func (f TimeField) BetweenSymmetric(start, end TimeField) Predicate {
	return TernaryPredicate{
		Operator: PredicateBetweenSymmetric,
		Field:    f,
		FieldX:   start,
		FieldY:   end,
	}
}

// BetweenSymmetricTime returns an 'A BETWEEN SYMMETRIC X AND Y' Predicate.
// It only accepts time.Time.
func (f TimeField) BetweenSymmetricTime(start, end time.Time) Predicate {
	return TernaryPredicate{
		Operator: PredicateBetweenSymmetric,
		Field:    f,
		FieldX:   Time(start),
		FieldY:   Time(end),
	}
}

// NotBetweenSymmetric returns an 'A NOT BETWEEN SYMMETRIC X AND Y' Predicate.
// It only accepts TimeField.
func (f TimeField) NotBetweenSymmetric(start, end TimeField) Predicate {
	return TernaryPredicate{
		Operator: PredicateNotBetweenSymmetric,
		Field:    f,
		FieldX:   start,
		FieldY:   end,
	}
}

// NotBetweenSymmetricTime returns an 'A NOT BETWEEN SYMMETRIC X AND Y'
// Predicate. It only accepts time.Time.
func (f TimeField) NotBetweenSymmetricTime(start, end time.Time) Predicate {
	return TernaryPredicate{
		Operator: PredicateNotBetweenSymmetric,
		Field:    f,
		FieldX:   Time(start),
		FieldY:   Time(end),
	}
}

// String implements the fmt.Stringer interface. It returns the string
// representation of a TimeField.
func (f TimeField) String() string {
	query, args := f.ToSQL(nil)
	return MySQLInterpolateSQL(query, args...)
}

// GetAlias implements the Field interface. It returns the Alias of the
// TimeField.
func (f TimeField) GetAlias() string {
	return f.alias
}

// GetName implements the Field interface. It returns the Name of the
// TimeField.
func (f TimeField) GetName() string {
	return f.name
}
