package qx

// CustomField is a Field that can render itself in an arbitrary way as defined
// by its Format string. Values are interpolated into the Format string as
// described in the (CustomField).CustomSprintf function.
type CustomField struct {
	Alias        string
	Format       string
	Values       []interface{}
	IsDesc       *bool
	IsNullsFirst *bool

	// Each dialect-specific qy package (postgres, mysql, sqlite3) is expected to
	// provide their dialect-specific CustomSprintf function to CustomField. If
	// none is provided, it will fall back on using the the defaultSprintf function
	// in this package.
	CustomSprintf func(format string, values []interface{}, excludeTableQualifiers []string) (string, []interface{})
}

// ToSQL marshals a CustomField into an SQL query and args as described in the
// CustomField struct description.
func (f CustomField) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	var query string
	var args []interface{}
	if f.CustomSprintf != nil {
		query, args = f.CustomSprintf(f.Format, f.Values, excludeTableQualifiers)
	} else {
		query, args = defaultSprintf(f.Format, f.Values, excludeTableQualifiers)
	}
	if f.IsDesc != nil {
		if *f.IsDesc {
			query = query + " DESC"
		} else {
			query = query + " ASC"
		}
	}
	if f.IsNullsFirst != nil {
		if *f.IsNullsFirst {
			query = query + " NULLS FIRST"
		} else {
			query = query + " NULLS LAST"
		}
	}
	return query, args
}

// As returns a new CustomField with the new alias i.e. 'field AS Alias'.
func (f CustomField) As(alias string) CustomField {
	f.Alias = alias
	return f
}

// Asc returns a new CustomField indicating that it should be ordered in
// ascending order i.e. 'ORDER BY field ASC'.
func (f CustomField) Asc() CustomField {
	isDesc := false
	f.IsDesc = &isDesc
	return f
}

// Desc returns a new CustomField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (f CustomField) Desc() CustomField {
	isDesc := true
	f.IsDesc = &isDesc
	return f
}

// NullsFirst returns a new CustomField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f CustomField) NullsFirst() CustomField {
	isNullsFirst := true
	f.IsNullsFirst = &isNullsFirst
	return f
}

// NullsLast returns a new CustomField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f CustomField) NullsLast() CustomField {
	isNullsFirst := false
	f.IsNullsFirst = &isNullsFirst
	return f
}

// IsNull returns an 'A IS NULL' Predicate.
func (f CustomField) IsNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNull,
		Field:    f,
	}
}

// IsNotNull returns an 'A IS NOT NULL' Predicate.
func (f CustomField) IsNotNull() Predicate {
	return UnaryPredicate{
		Operator: PredicateIsNotNull,
		Field:    f,
	}
}

// Eq returns an 'A = B' Predicate. It accepts any Field.
func (f CustomField) Eq(field Field) Predicate {
	return BinaryPredicate{
		Operator:   PredicateEq,
		LeftField:  f,
		RightField: field,
	}
}

// Ne returns an 'A <> B' Predicate. It accepts any Field.
func (f CustomField) Ne(field Field) Predicate {
	return BinaryPredicate{
		Operator:   PredicateNe,
		LeftField:  f,
		RightField: field,
	}
}

// Gt returns an 'A > B' Predicate. It accepts any Field.
func (f CustomField) Gt(field Field) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGt,
		LeftField:  f,
		RightField: field,
	}
}

// Ge returns an 'A >= B' Predicate. It accepts any Field.
func (f CustomField) Ge(field Field) Predicate {
	return BinaryPredicate{
		Operator:   PredicateGe,
		LeftField:  f,
		RightField: field,
	}
}

// Lt returns an 'A < B' Predicate. It accepts any Field.
func (f CustomField) Lt(field Field) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLt,
		LeftField:  f,
		RightField: field,
	}
}

// Le returns an 'A <= B' Predicate. It accepts any Field.
func (f CustomField) Le(field Field) Predicate {
	return BinaryPredicate{
		Operator:   PredicateLe,
		LeftField:  f,
		RightField: field,
	}
}

// GetAlias implements the Field interface. It returns the alias of thee
// CustomField.
func (f CustomField) GetAlias() string {
	return f.Alias
}

// GetName implements the Field interface. It returns the name of the
// CustomField.
func (f CustomField) GetName() string {
	name, _ := f.ToSQL(nil)
	return name
}
