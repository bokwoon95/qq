package qx

import "strings"

type VariadicQueryOperator string

const (
	QueryUnion        VariadicQueryOperator = "UNION"
	QueryUnionAll     VariadicQueryOperator = "UNION ALL"
	QueryIntersect    VariadicQueryOperator = "INTERSECT"
	QueryIntersectAll VariadicQueryOperator = "INTERSECT ALL"
	QueryExcept       VariadicQueryOperator = "EXCEPT"
	QueryExceptAll    VariadicQueryOperator = "EXCEPT ALL"
)

type VariadicQuery struct {
	Nested   bool
	Alias    string
	Operator VariadicQueryOperator
	Queries  []Query
}

func (q VariadicQuery) ToSQL() (string, []interface{}) {
	allQueries, allArgs := []string{}, []interface{}{}
	for i := range q.Queries {
		if q.Queries[i] == nil {
			continue
		}
		subquery, subargs := q.Queries[i].NestThis().ToSQL()
		if subquery == "" {
			continue
		}
		allQueries = append(allQueries, subquery)
		allArgs = append(allArgs, subargs...)
	}
	if len(allQueries) == 0 {
		return "", nil
	}
	if q.Operator == "" {
		q.Operator = QueryUnion
	}
	query := strings.Join(allQueries, " "+string(q.Operator)+" ")
	if q.Nested {
		query = "(" + query + ")"
	}
	return query, allArgs
}

func (q VariadicQuery) GetAlias() string {
	return q.Alias
}

func (q VariadicQuery) GetName() string {
	return ""
}

func (q VariadicQuery) NestThis() Query {
	q.Nested = true
	return q
}

func (q VariadicQuery) As(alias string) VariadicQuery {
	q.Alias = alias
	return q
}

func (q VariadicQuery) Get(fieldName string) CustomField {
	return CustomField{Format: q.Alias + "." + fieldName}
}
