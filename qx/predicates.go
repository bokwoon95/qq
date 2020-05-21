package qx

import (
	"fmt"
	"strings"
)

// CustomPredicate is a Query that can render itself in an arbitrary way as defined
// by its Format string. Values are interpolated into the Format string as
// described in the (CustomPredicate).CustomSprintf function.
type CustomPredicate struct {
	Format string
	Values []interface{}

	// Each dialect-specific qy package (postgres, mysql, sqlite3) is expected
	// to provide their dialect-specific CustomSprintf function to CustomPredicate.
	// If none is provided, it will fall back on using the the defaultSprintf
	// function in this package.
	CustomSprintf func(format string, values []interface{}, excludeTableQualifiers []string) (string, []interface{})
}

// ToSQL marshals a CustomPredicate into an SQL query.
func (p CustomPredicate) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	var query string
	var args []interface{}
	if p.CustomSprintf != nil {
		query, args = p.CustomSprintf(p.Format, p.Values, excludeTableQualifiers)
	} else {
		query, args = defaultSprintf(p.Format, p.Values, excludeTableQualifiers)
	}
	return query, args
}

// AssertPredicate implements the Predicate interface.
func (p CustomPredicate) AssertPredicate() {}

// VariadicPredicateOperator is an operator that can join a variadic number of
// Predicates together.
type VariadicPredicateOperator string

// Possible VariadicOperators
const (
	PredicateOr  VariadicPredicateOperator = "OR"
	PredicateAnd VariadicPredicateOperator = "AND"
)

// VariadicPredicate represents the "x AND y AND z..." or "x OR y OR z..." SQL
// construct.
type VariadicPredicate struct {
	// Toplevel indicates if the variadic predicate is the top level predicate
	// i.e. it does not need enclosing brackets
	Toplevel   bool
	Operator   VariadicPredicateOperator
	Predicates []Predicate
}

// ToSQL marshals a VariadicPredicate into an SQL query and args as described
// in the VariadicPredicate struct description.
func (p VariadicPredicate) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	var predicateStrs []string
	var args []interface{}
	if p.Operator == "" {
		p.Operator = PredicateAnd
	}
	for i := range p.Predicates {
		if p.Predicates[i] == nil {
			continue
		}
		subquery, subargs := p.Predicates[i].ToSQL(excludeTableQualifiers)
		predicateStrs = append(predicateStrs, subquery)
		args = append(args, subargs...)
	}
	switch len(predicateStrs) {
	case 0:
		return "", nil
	case 1:
		return predicateStrs[0], args
	default:
		query := strings.Join(predicateStrs, " "+string(p.Operator)+" ")
		if !p.Toplevel {
			query = "(" + query + ")"
		}
		return query, args
	}
}

// AssertPredicate implements the Predicate interface.
func (p VariadicPredicate) AssertPredicate() {}

func And(predicates ...Predicate) VariadicPredicate {
	return VariadicPredicate{
		Operator:   PredicateAnd,
		Predicates: predicates,
	}
}

func Or(predicates ...Predicate) VariadicPredicate {
	return VariadicPredicate{
		Operator:   PredicateOr,
		Predicates: predicates,
	}
}

// WriteSQL will write the VariadicPredicate into the buffer and args as
// described in the VariadicPredicate struct description. The result is
// prepended and appended with the prependwith and appendwith arguments. The
// list of table qualifiers to be excluded is propagated down to the individual
// fields.
//
// If there are no Predicates present, nothing will be written into the buffer.
// WriteSQL returns a flag indicating whether anything was written into the
// buffer.
func (p VariadicPredicate) WriteSQL(buf *strings.Builder, args *[]interface{}, prependWith, appendWith string, excludeTableQualifiers []string) bool {
	if len(p.Predicates) == 1 {
		if pred, ok := p.Predicates[0].(VariadicPredicate); ok {
			pred.Toplevel = true
			p.Predicates[0] = pred
		}
	}
	predicateQuery, predicateArgs := p.ToSQL(excludeTableQualifiers)
	if predicateQuery != "" {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(prependWith + predicateQuery + appendWith)
		*args = append(*args, predicateArgs...)
		return true
	}
	return false
}

type UnaryPredicateOperator string

const (
	PredicateIsNull    UnaryPredicateOperator = "IS NULL"
	PredicateIsNotNull UnaryPredicateOperator = "IS NOT NULL"
)

// UnaryPredicate represents the 'X [IS NULL | IS NOT NULL]' SQL construct.
type UnaryPredicate struct {
	Operator UnaryPredicateOperator
	Field    Field
}

func (p UnaryPredicate) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	if p.Operator == "" {
		p.Operator = PredicateIsNull
	}
	if p.Field == nil {
		p.Field = _NULL
	}
	query, args := p.Field.ToSQL(excludeTableQualifiers)
	switch p.Operator {
	case PredicateIsNull:
		return query + " IS NULL", args
	case PredicateIsNotNull:
		return query + " IS NOT NULL", args
	default:
		return "", nil
	}
}

func (p UnaryPredicate) AssertPredicate() {}

func (p UnaryPredicate) String() string {
	return fmt.Sprint(p.ToSQL(nil))
}

type BinaryPredicateOperator string

const (
	PredicateEq                BinaryPredicateOperator = "="
	PredicateNe                BinaryPredicateOperator = "<>"
	PredicateGt                BinaryPredicateOperator = ">"
	PredicateGe                BinaryPredicateOperator = ">="
	PredicateLt                BinaryPredicateOperator = "<"
	PredicateLe                BinaryPredicateOperator = "<="
	PredicateLike              BinaryPredicateOperator = "LIKE"
	PredicateNotLike           BinaryPredicateOperator = "NOT LIKE"
	PredicateILike             BinaryPredicateOperator = "ILIKE"
	PredicateNotILike          BinaryPredicateOperator = "NOT ILIKE"
	PredicateIsDistinctFrom    BinaryPredicateOperator = "IS DISTINCT FROM"
	PredicateIsNotDistinctFrom BinaryPredicateOperator = "IS NOT DISTINCT FROM"
)

// BinaryPredicate represents the 'A [operator] B' SQL construct, where
// operator is something like =, <>, >, >= ... etc.
type BinaryPredicate struct {
	Operator   BinaryPredicateOperator
	LeftField  Field
	RightField Field
}

func (p BinaryPredicate) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	if p.Operator == "" {
		p.Operator = PredicateEq
	}
	if p.LeftField == nil {
		p.LeftField = _NULL
	}
	if p.RightField == nil {
		p.RightField = _NULL
	}
	lquery, largs := p.LeftField.ToSQL(excludeTableQualifiers)
	rquery, rargs := p.RightField.ToSQL(excludeTableQualifiers)
	query := lquery + " " + string(p.Operator) + " " + rquery
	args := append(largs, rargs...)
	return query, args
}

func (p BinaryPredicate) AssertPredicate() {}

func (p BinaryPredicate) String() string {
	return fmt.Sprint(p.ToSQL(nil))
}

type TernaryPredicateOperator string

const (
	PredicateBetween             TernaryPredicateOperator = "BETWEEN"
	PredicateNotBetween          TernaryPredicateOperator = "NOT BETWEEN"
	PredicateBetweenSymmetric    TernaryPredicateOperator = "BETWEEN SYMMETRIC"
	PredicateNotBetweenSymmetric TernaryPredicateOperator = "NOT BETWEEN SYMMETRIC"
)

// TernaryPredicate represents the 'A [operator] X AND Y' SQL construct, where
// operator is something like BETWEEN, NOT BETWEEN, BETWEEN SYMMETRIC... etc.
type TernaryPredicate struct {
	Operator TernaryPredicateOperator
	Field    Field
	FieldX   Field
	FieldY   Field
}

func (p TernaryPredicate) ToSQL(excludeTableQualifiers []string) (string, []interface{}) {
	if p.Operator == "" {
		p.Operator = PredicateBetween
	}
	if p.Field == nil {
		p.Field = _NULL
	}
	if p.FieldX == nil {
		p.FieldX = _NULL
	}
	if p.FieldY == nil {
		p.FieldY = _NULL
	}
	query, args := p.Field.ToSQL(excludeTableQualifiers)
	queryX, argsX := p.FieldX.ToSQL(excludeTableQualifiers)
	queryY, argsY := p.FieldY.ToSQL(excludeTableQualifiers)
	query = query + " " + string(p.Operator) + " " + queryX + " AND " + queryY
	args = append(args, argsX...)
	args = append(args, argsY...)
	return query, args
}

func (p TernaryPredicate) AssertPredicate() {}

func (p TernaryPredicate) String() string {
	return fmt.Sprint(p.ToSQL(nil))
}
