package qy

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/bokwoon95/qx-postgres/qx"
)

type SelectQuery struct {
	Nested bool
	Alias  string
	// WITH
	CTEs qx.CTEs
	// SELECT
	SelectType   qx.SelectType
	DistinctOn   qx.Fields
	SelectFields qx.Fields
	// FROM
	FromTable  qx.Table
	JoinGroups qx.JoinGroups
	// WHERE
	WherePredicates qx.VariadicPredicate
	// GROUP BY
	GroupByFields qx.Fields
	// HAVING
	HavingPredicates qx.VariadicPredicate
	// ORDER BY
	OrderByFields qx.Fields
	// LIMIT
	LimitValue *uint64
	// OFFSET
	OffsetValue *uint64
	// Exec
	Mapper      func(Row)
	Accumulator func()
	// Logging
	Log qx.Logger
}

func (q SelectQuery) ToSQL() (string, []interface{}) {
	var buf = &strings.Builder{}
	var args []interface{}
	// WITH
	q.CTEs.WriteSQL(buf, &args)
	{ // SELECT
		// We need to check if the user provided any valid selected in the SELECT
		// clause (i.e. aggregated ToSQL()s evaluate to a non-empty string). If
		// there is nothing valid in the SELECT clause, we don't bother with
		// writing anything SELECT related into the buffer at all. We cannot simply
		// check len(q.selected) > 0 to validate this, because a non-zero number of
		// selected may still end up evaluating to an empty string e.g. if the user
		// passes in all nil. So we write to a temporary buffer first and if
		// anything was successfully written into the buffer, then we know that
		// there are valid selected present in the SELECT
		tempBuf, tempArgs := &strings.Builder{}, []interface{}{}
		if q.SelectFields.WriteSQLWithAlias(tempBuf, &tempArgs, "", "", nil) {
			if q.SelectType == "" {
				q.SelectType = qx.SelectTypeDefault
			}
			if buf.Len() > 0 {
				buf.WriteString(" ")
			}
			switch q.SelectType {
			case qx.SelectTypeDistinctOn:
				// For SELECT DISTINCT ON, we need to write the distinct selected
				// first before writing the rest of the selected selected
				q.DistinctOn.WriteSQL(buf, &args, string(q.SelectType)+" (", ") "+tempBuf.String(), nil)
			default:
				// otherwise for SELECT and SELECT DISTINCT we can just write the
				// selected selected in directly
				buf.WriteString(string(q.SelectType) + " " + tempBuf.String())
			}
			args = append(args, tempArgs...)
		}
	}
	{ // FROM
		fromQuery, fromArgs := "", []interface{}{}
		if q.FromTable != nil {
			fromQuery, fromArgs = q.FromTable.ToSQL()
		}
		if fromQuery != "" {
			if buf.Len() > 0 {
				buf.WriteString(" ")
			}
			if _, ok := q.FromTable.(qx.Query); ok {
				fromQuery = "(" + fromQuery + ")"
			}
			if q.FromTable.GetAlias() != "" {
				buf.WriteString("FROM " + fromQuery + " AS " + q.FromTable.GetAlias())
			} else {
				buf.WriteString("FROM " + fromQuery)
			}
			args = append(args, fromArgs...)
		}
	}
	// JOIN
	q.JoinGroups.WriteSQL(buf, &args)
	// WHERE
	q.WherePredicates.Toplevel = true
	q.WherePredicates.WriteSQL(buf, &args, "WHERE ", "", nil)
	// GROUP BY
	q.GroupByFields.WriteSQL(buf, &args, "GROUP BY ", "", nil)
	// HAVING
	q.HavingPredicates.Toplevel = true
	q.HavingPredicates.WriteSQL(buf, &args, "HAVING ", "", nil)
	// ORDER BY
	q.OrderByFields.WriteSQL(buf, &args, "ORDER BY ", "", nil)
	// LIMIT
	if q.LimitValue != nil {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString("LIMIT ?")
		args = append(args, *q.LimitValue)
	}
	// OFFSET
	if q.OffsetValue != nil {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString("OFFSET ?")
		args = append(args, *q.OffsetValue)
	}
	query := buf.String()
	if !q.Nested {
		query = qx.MySQLToPostgresPlaceholders(query)
		if q.Log != nil {
			q.Log.Println(qx.PostgresInterpolateSQL(query, args...))
		}
	}
	return query, args
}

func NewSelectQuery() SelectQuery {
	q := SelectQuery{Alias: qx.RandomString(8)}
	return q
}

func From(table qx.Table) SelectQuery {
	return NewSelectQuery().From(table)
}

func Select(fields ...qx.Field) SelectQuery {
	return NewSelectQuery().Select(fields...)
}

func SelectDistinct(fields ...qx.Field) SelectQuery {
	return NewSelectQuery().SelectDistinct(fields...)
}

func SelectDistinctOn(distinctFields ...qx.Field) func(...qx.Field) SelectQuery {
	return func(fields ...qx.Field) SelectQuery {
		return NewSelectQuery().SelectDistinctOn(distinctFields...)(fields...)
	}
}

func Selectx(mapper func(Row), accumulator func()) SelectQuery {
	return NewSelectQuery().Selectx(mapper, accumulator)
}

func SelectRowx(mapper func(Row)) SelectQuery {
	return NewSelectQuery().SelectRowx(mapper)
}

func (q SelectQuery) With(ctes ...qx.CTE) SelectQuery {
	q.CTEs = append(q.CTEs, ctes...)
	return q
}

func (q SelectQuery) Select(fields ...qx.Field) SelectQuery {
	q.SelectFields = append(q.SelectFields, fields...)
	return q
}

func (q SelectQuery) SelectDistinct(fields ...qx.Field) SelectQuery {
	q.SelectType = qx.SelectTypeDistinct
	return q.Select(fields...)
}

func (q SelectQuery) SelectDistinctOn(distinctFields ...qx.Field) func(...qx.Field) SelectQuery {
	return func(fields ...qx.Field) SelectQuery {
		q.SelectType = qx.SelectTypeDistinctOn
		q.DistinctOn = append(q.DistinctOn, distinctFields...)
		return q.Select(fields...)
	}
}

func (q SelectQuery) From(table qx.Table) SelectQuery {
	q.FromTable = table
	return q
}

func (q SelectQuery) Join(table qx.Table, predicate qx.Predicate, predicates ...qx.Predicate) SelectQuery {
	predicates = append([]qx.Predicate{predicate}, predicates...)
	q.JoinGroups = append(q.JoinGroups, qx.JoinGroup{
		JoinType:     qx.JoinTypeDefault,
		Table:        table,
		OnPredicates: qx.VariadicPredicate{Predicates: predicates},
	})
	return q
}

func (q SelectQuery) LeftJoin(table qx.Table, predicate qx.Predicate, predicates ...qx.Predicate) SelectQuery {
	predicates = append([]qx.Predicate{predicate}, predicates...)
	q.JoinGroups = append(q.JoinGroups, qx.JoinGroup{
		JoinType:     qx.JoinTypeLeft,
		Table:        table,
		OnPredicates: qx.VariadicPredicate{Predicates: predicates},
	})
	return q
}

func (q SelectQuery) RightJoin(table qx.Table, predicate qx.Predicate, predicates ...qx.Predicate) SelectQuery {
	predicates = append([]qx.Predicate{predicate}, predicates...)
	q.JoinGroups = append(q.JoinGroups, qx.JoinGroup{
		JoinType:     qx.JoinTypeRight,
		Table:        table,
		OnPredicates: qx.VariadicPredicate{Predicates: predicates},
	})
	return q
}

func (q SelectQuery) FullJoin(table qx.Table, predicate qx.Predicate, predicates ...qx.Predicate) SelectQuery {
	predicates = append([]qx.Predicate{predicate}, predicates...)
	q.JoinGroups = append(q.JoinGroups, qx.JoinGroup{
		JoinType:     qx.JoinTypeFull,
		Table:        table,
		OnPredicates: qx.VariadicPredicate{Predicates: predicates},
	})
	return q
}

func (q SelectQuery) CrossJoin(table qx.Table) SelectQuery {
	q.JoinGroups = append(q.JoinGroups, qx.JoinGroup{
		JoinType: qx.JoinTypeCross,
		Table:    table,
	})
	return q
}

func (q SelectQuery) Where(predicates ...qx.Predicate) SelectQuery {
	q.WherePredicates.Predicates = append(q.WherePredicates.Predicates, predicates...)
	return q
}

func (q SelectQuery) GroupBy(fields ...qx.Field) SelectQuery {
	q.GroupByFields = append(q.GroupByFields, fields...)
	return q
}

func (q SelectQuery) Having(predicates ...qx.Predicate) SelectQuery {
	q.HavingPredicates.Predicates = append(q.HavingPredicates.Predicates, predicates...)
	return q
}

func (q SelectQuery) OrderBy(fields ...qx.Field) SelectQuery {
	q.OrderByFields = append(q.OrderByFields, fields...)
	return q
}

func (q SelectQuery) Limit(limit int) SelectQuery {
	if limit < 0 {
		limit = -limit
	}
	num := uint64(limit)
	q.LimitValue = &num
	return q
}

func (q SelectQuery) Offset(offset int) SelectQuery {
	if offset < 0 {
		offset = -offset
	}
	num := uint64(offset)
	q.OffsetValue = &num
	return q
}

func (q SelectQuery) Selectx(mapper func(Row), accumulator func()) SelectQuery {
	q.Mapper = mapper
	q.Accumulator = accumulator
	return q
}

func (q SelectQuery) SelectRowx(mapper func(Row)) SelectQuery {
	q.Mapper = mapper
	return q
}

func (q SelectQuery) Exec(db qx.Queryer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				err = v
			case string:
				err = errors.New(v)
			}
		}
	}()
	if q.Mapper == nil {
		return errors.New("you can't call Exec without a mapper")
	}
	r := &QyRow{QxRow: &qx.QxRow{}}
	q.Mapper(r)                     // call the mapper once on the *Row to get all the selected that the user is interested in
	q.SelectFields = r.QxRow.Fields // then, transfer the selected collected by *Row to the SelectQuery
	r.QxRow.Active = true           // mark Row as active i.e.
	noFieldsSpecified := len(r.QxRow.Fields) == 0
	if noFieldsSpecified {
		q.SelectFields = append(q.SelectFields, Fieldf("1"))
	}
	query, args := q.ToSQL()
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	var rowcount int
	if noFieldsSpecified {
		// if user didn't specify any selected, don't bother scanning anything and return early
		return nil
	}
	for rows.Next() {
		rowcount++
		err = rows.Scan(r.QxRow.Dest...)
		if err != nil {
			return err
		}
		r.QxRow.Index = 0 // index must always be reset back to 0 before mapper is called
		q.Mapper(r)
		if q.Accumulator == nil {
			break
		}
		q.Accumulator()
	}
	if rowcount == 0 && q.Accumulator == nil {
		return sql.ErrNoRows
	}
	return rows.Err()
}

func (q SelectQuery) As(alias string) SelectQuery {
	q.Alias = alias
	return q
}

func (q SelectQuery) Get(fieldName string) qx.CustomField {
	return Fieldf(q.Alias + "." + fieldName)
}

func (q SelectQuery) GetAlias() string {
	return q.Alias
}

func (q SelectQuery) GetName() string {
	return ""
}

func (q SelectQuery) NestThis() qx.Query {
	q.Nested = true
	return q
}
