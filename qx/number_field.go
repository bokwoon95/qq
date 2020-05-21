package qx

// NumberField either represents a number column, a number expression or a
// literal number value.
type NumberField struct {
	// NumberField will be one of the following:

	// 1) Number expression
	// Examples of number expressions:
	// | query                  | args        |
	// |------------------------|-------------|
	// | ? / ?                  | 22, 7       |
	// | FLOOR(? + tbl.column)  | 5           |
	// | (ABS(?) + (? % ?)) - ? | -3, 5, 4, 8 |
	format *string
	fields []Field

	// 2) Literal number value
	// Examples of literal number values:
	// | query | args    |
	// |-------|---------|
	// | ?     | 5       |
	// | ?     | 3.14159 |
	value interface{}

	// 3) Number column
	// Examples of number columns:
	// | query                    | args   |
	// |--------------------------|--------|
	// | users.uid                |        |
	// | uid                      |        |
	// | users.uid ASC NULLS LAST |        |
	alias      string
	table      *TableInfo
	name       string
	descending *bool
	nullsfirst *bool
}

// ToSQL marshals a NumberField into an SQL query and args (as described in the
// NumberField internal struct comments). If the BooleanField's table name
// appears in the excludeTableQualifiers list, the output column name will not
// be table qualified.
func (f NumberField) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	// 1) Number expression
	if f.format != nil {
		values := make([]interface{}, len(f.fields))
		for i := range f.fields {
			values[i] = f.fields[i]
		}
		return CustomField{
			Alias:  f.alias,
			Format: *f.format,
			Values: values,
		}.ToSQL(excludeTableQualifiers)
	}

	// 2) Literal number value
	if f.value != nil {
		return "?", []interface{}{f.value}
	}

	// 3) Number column
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

// NewNumberField returns a new NumberField representing a number TableInfo column.
func NewNumberField(name string, tbl *TableInfo) NumberField {
	f := NumberField{
		name:  name,
		table: tbl,
	}
	tbl.Fields = append(tbl.Fields, &f)
	return f
}

// Int returns a new NumberField representing a literal int value.
func Int(num int) NumberField {
	return NumberField{
		value: num,
	}
}

// Int64 returns a new NumberField representing a literal int64 value.
func Int64(num int64) NumberField {
	return NumberField{
		value: num,
	}
}

// Float64 returns a new NumberField representing a literal float64 value.
func Float64(num float64) NumberField {
	return NumberField{
		value: num,
	}
}

// Set returns a FieldValueSet associating the NumberField to the value i.e.
// 'SET field = value'.
func (f NumberField) Set(val interface{}) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: val,
	}
}

// SetInt returns a FieldValueSet associating the NumberField to the int value
// i.e. 'SET field = value'.
func (f NumberField) SetInt(num int) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: num,
	}
}

// SetInt64 returns a FieldValueSet associating the NumberField to the int64
// value i.e. 'SET field = value'.
func (f NumberField) SetInt64(num int64) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: num,
	}
}

// SetFloat64 returns a FieldValueSet associating the NumberField to the float64
// value i.e. 'SET field = value'.
func (f NumberField) SetFloat64(num float64) FieldValueSet {
	return FieldValueSet{
		Field: f,
		Value: num,
	}
}

// As returns a new NumberField with the new field Alias i.e. 'field AS Alias'.
func (f NumberField) As(alias string) NumberField {
	f.alias = alias
	return f
}

// Asc returns a new NumberField indicating that it should be ordered in
// ascending order i.e. 'ORDER BY field ASC'.
func (f NumberField) Asc() NumberField {
	desc := false
	f.descending = &desc
	return f
}

// Desc returns a new NumberField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (f NumberField) Desc() NumberField {
	desc := true
	f.descending = &desc
	return f
}

// NullsFirst returns a new NumberField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f NumberField) NullsFirst() NumberField {
	nullsfirst := true
	f.nullsfirst = &nullsfirst
	return f
}

// NullsLast returns a new NumberField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f NumberField) NullsLast() NumberField {
	nullsfirst := false
	f.nullsfirst = &nullsfirst
	return f
}

// IsNull returns an 'A IS NULL' Predicate.
func (f NumberField) IsNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNull,
		Field:    f,
	}
}

// IsNotNull returns an 'A IS NOT NULL' Predicate.
func (f NumberField) IsNotNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNotNull,
		Field:    f,
	}
}

// Eq returns an 'A = B' Predicate. It only accepts NumberField.
func (f NumberField) Eq(field NumberField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: field,
	}
}

// EqFloat64 returns an 'A = B' Predicate. It only accepts float64.
func (f NumberField) EqFloat64(num float64) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: Float64(num),
	}
}

// EqInt returns an 'A = B' Predicate. It only accepts int.
func (f NumberField) EqInt(num int) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: Int(num),
	}
}

// Ne returns an 'A <> B' Predicate. It only accepts NumberField.
func (f NumberField) Ne(field NumberField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: field,
	}
}

// NeFloat64 returns an 'A <> B' Predicate. It only accepts float64.
func (f NumberField) NeFloat64(num float64) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: Float64(num),
	}
}

// NeInt returns an 'A <> B' Predicate. It only accepts int.
func (f NumberField) NeInt(num int) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: Int(num),
	}
}

// Gt returns an 'A > B' Predicate. It only accepts NumberField.
func (f NumberField) Gt(field NumberField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGt,
		LeftField:  f,
		RightField: field,
	}
}

// GtFloat64 returns an 'A > B' Predicate. It only accepts float64.
func (f NumberField) GtFloat64(num float64) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGt,
		LeftField:  f,
		RightField: Float64(num),
	}
}

// GtInt returns an 'A > B' Predicate. It only accepts int.
func (f NumberField) GtInt(num int) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGt,
		LeftField:  f,
		RightField: Int(num),
	}
}

// Ge returns an 'A >= B' Predicate. It only accepts NumberField.
func (f NumberField) Ge(field NumberField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGe,
		LeftField:  f,
		RightField: field,
	}
}

// GeFloat64 returns an 'A >= B' Predicate. It only accepts float64.
func (f NumberField) GeFloat64(num float64) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGe,
		LeftField:  f,
		RightField: Float64(num),
	}
}

// GeInt returns an 'A >= B' Predicate. It only accepts int.
func (f NumberField) GeInt(num int) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGe,
		LeftField:  f,
		RightField: Int(num),
	}
}

// Lt returns an 'A < B' Predicate. It only accepts NumberField.
func (f NumberField) Lt(field NumberField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLt,
		LeftField:  f,
		RightField: field,
	}
}

// LtFloat64 returns an 'A < B' Predicate. It only accepts float64.
func (f NumberField) LtFloat64(num float64) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLt,
		LeftField:  f,
		RightField: Float64(num),
	}
}

// LtInt returns an 'A < B' Predicate. It only accepts int.
func (f NumberField) LtInt(num int) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLt,
		LeftField:  f,
		RightField: Int(num),
	}
}

// Le returns an 'A <= B' Predicate. It only accepts NumberField.
func (f NumberField) Le(field NumberField) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLe,
		LeftField:  f,
		RightField: field,
	}
}

// LeFloat64 returns an 'A <= B' Predicate. It only accepts float64.
func (f NumberField) LeFloat64(num float64) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLe,
		LeftField:  f,
		RightField: Float64(num),
	}
}

// LeInt returns an 'A <= B' Predicate. It only accepts int.
func (f NumberField) LeInt(num int) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLe,
		LeftField:  f,
		RightField: Int(num),
	}
}

// String implements the fmt.Stringer interface. It returns the string
// representation of a NumberField.
func (f NumberField) String() string {
	query, args := f.ToSQL(nil)
	return MySQLInterpolateSQL(query, args...)
}

// GetAlias implements the Field interface. It returns the Alias of the
// NumberField.
func (f NumberField) GetAlias() string {
	return f.alias
}

// GetName implements the Field interface. It returns the Name of the
// NumberField.
func (f NumberField) GetName() string {
	return f.name
}

// // NumberFieldf follows a printf-like syntax that takes in multiple
// // NumberFields and returns a NumberField formatted according to an arbitrary
// // format string.  The only recognized format specifier is the ? question mark.
// // E.g.  NumberFieldf("(? + 5 - 10) / ?", numfield1, numfield2), where the
// // first and second question marks will be replaced with numfield1 and
// // numfield2 respectively.
// func NumberFieldf(format string, fields ...NumberField) NumberField {
// 	expression := NumberField{
// 		format: &format,
// 		fields: make([]Field, len(fields)),
// 	}
// 	for i := range fields {
// 		expression.fields[i] = fields[i]
// 	}
// 	return expression
// }
//
// // Add will add a NumberField to a NumberField.
// func (f NumberField) Add(field NumberField) NumberField {
// 	return NumberFieldf("(? + ?)", f, field)
// }
//
// // Sub will subtract a NumberField from a NumberField.
// func (f NumberField) Sub(field NumberField) NumberField {
// 	return NumberFieldf("(? - ?)", f, field)
// }
//
// // Mul will mulitply a NumberField with a NumberField.
// func (f NumberField) Mul(field NumberField) NumberField {
// 	return NumberFieldf("(? * ?)", f, field)
// }
//
// // Div will divide a NumberField by a NumberField.
// func (f NumberField) Div(field NumberField) NumberField {
// 	return NumberFieldf("(? / ?)", f, field)
// }
//
// // Mod will modulo a NumberField by a NumberField. Note that modulo is an
// // operation that is only defined for integers.
// func (f NumberField) Mod(field NumberField) NumberField {
// 	return NumberFieldf("(? % ?)", f, field)
// }
//
// // Abs will return the absolute value of a NumberField.
// func (f NumberField) Abs() NumberField {
// 	return NumberFieldf("abs(?)", f)
// }
//
// // Ceil will round a NumberField up to the nearest integer value.
// func (f NumberField) Ceil() NumberField {
// 	return NumberFieldf("ceil(?)", f)
// }
//
// // Ceil will round a NumberField down to the nearest integer value.
// func (f NumberField) Floor() NumberField {
// 	return NumberFieldf("floor(?)", f)
// }
//
// // Pow will return a NumberField raised to the power of another NumberField.
// func (f NumberField) Pow(field NumberField) NumberField {
// 	return NumberFieldf("power(?, ?)", f, field)
// }
//
// // Add will add an int to a NumberField.
// func (f NumberField) AddInt(num int) NumberField {
// 	return NumberFieldf("(? + ?)", f, Int(num))
// }
//
// // Sub will subtract an int from a NumberField.
// func (f NumberField) SubInt(num int) NumberField {
// 	return NumberFieldf("(? - ?)", f, Int(num))
// }
//
// // Mul will multiple a NumberField by an int.
// func (f NumberField) MulInt(num int) NumberField {
// 	return NumberFieldf("(? * ?)", f, Int(num))
// }
//
// // Div will divide a NumberField by an int.
// func (f NumberField) DivInt(num int) NumberField {
// 	return NumberFieldf("(? / ?)", f, Int(num))
// }
//
// // Mod will modulo a NumberField by an int.
// func (f NumberField) ModInt(num int) NumberField {
// 	return NumberFieldf("(? % ?)", f, Int(num))
// }
//
// // PowInt will raise a NumberField to the power of an int.
// func (f NumberField) PowInt(num int) NumberField {
// 	return NumberFieldf("power(?, ?)", f, Int(num))
// }
