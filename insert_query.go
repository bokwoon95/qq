package qy

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/bokwoon95/qx-postgres/qx"
)

type InsertQuery struct {
	Nested bool
	Alias  string
	// WITH
	CTEs qx.CTEs
	// INSERT INTO
	IntoTable    qx.BaseTable
	InsertFields qx.Fields
	// VALUES
	ValuesList qx.ValuesList
	// SELECT
	SelectQuery *SelectQuery
	// ON CONFLICT
	HandleConflict       bool
	ConflictFields       qx.Fields
	ConflictPredicates   qx.VariadicPredicate
	ConflictConstraint   string
	Resolution           qx.FieldValueSets
	ResolutionPredicates qx.VariadicPredicate
	// RETURNING
	ReturningFields qx.Fields
	Mapper          func(Row)
	Accumulator     func()
	// Logging
	Log qx.Logger
}

func (q InsertQuery) ToSQL() (string, []interface{}) {
	var buf = &strings.Builder{}
	var args []interface{}
	var excludeTableQualifiers []string
	// WITH
	q.CTEs.WriteSQL(buf, &args)
	{ // INSERT INTO
		intoQuery, intoArgs := "", []interface{}{}
		if q.IntoTable != nil {
			intoQuery, intoArgs = q.IntoTable.ToSQL()
			if q.IntoTable.GetAlias() != "" {
				excludeTableQualifiers = append(excludeTableQualifiers, q.IntoTable.GetAlias())
			} else if q.IntoTable.GetName() != "" {
				excludeTableQualifiers = append(excludeTableQualifiers, q.IntoTable.GetName())
			}
		}
		if intoQuery != "" {
			if buf.Len() > 0 {
				buf.WriteString(" ")
			}
			if q.IntoTable.GetAlias() != "" {
				buf.WriteString("INSERT INTO " + intoQuery + " AS " + q.IntoTable.GetAlias())
			} else {
				buf.WriteString("INSERT INTO " + intoQuery)
			}
			args = append(args, intoArgs...)
			q.InsertFields.WriteSQL(buf, &args, "(", ")", excludeTableQualifiers)
		}
	}
	// VALUES/SELECT
	switch {
	case len(q.ValuesList) > 0:
		q.ValuesList.WriteSQL(buf, &args, "VALUES ",  "")
	case q.SelectQuery != nil:
		q.SelectQuery.Nested = true
		selectQuery, selectArgs := q.SelectQuery.ToSQL()
		if selectQuery != "" {
			if buf.Len() > 0 {
				buf.WriteString(" ")
			}
			buf.WriteString(selectQuery)
			args = append(args, selectArgs...)
		}
	}
	// ON CONFLICT
	var noConflict bool
	switch {
	case q.HandleConflict:
		switch {
		case q.ConflictConstraint != "":
			if buf.Len() > 0 {
				buf.WriteString(" ")
			}
			buf.WriteString("ON CONFLICT ON CONSTRAINT " + q.ConflictConstraint)
		case q.ConflictFields.WriteSQL(buf, &args, "ON CONFLICT (", ")", excludeTableQualifiers):
			q.ConflictPredicates.Toplevel = true
			q.ConflictPredicates.WriteSQL(buf, &args, "WHERE ", "", excludeTableQualifiers)
		default:
			if buf.Len() > 0 {
				buf.WriteString(" ")
			}
			buf.WriteString("ON CONFLICT")
		}
	default:
		noConflict = true
	}
	switch {
	case noConflict:
		// no-op
	case q.Resolution.WriteSQL(buf, &args, "DO UPDATE SET ", "", excludeTableQualifiers):
		q.ResolutionPredicates.Toplevel = true
		q.ResolutionPredicates.WriteSQL(buf, &args, "WHERE ", "", nil)
	default:
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString("DO NOTHING")
	}
	// RETURNING
	q.ReturningFields.WriteSQLWithAlias(buf, &args, "RETURNING ", "", nil)
	query := buf.String()
	if !q.Nested {
		query = qx.MySQLToPostgresPlaceholders(query)
		if q.Log != nil {
			q.Log.Println(qx.PostgresInterpolateSQL(query, args...))
		}
	}
	return query, args
}

func NewInsertQuery() InsertQuery {
	return InsertQuery{Alias: qx.RandomString(8)}
}

func InsertInto(table qx.BaseTable) InsertQuery {
	return NewInsertQuery().InsertInto(table)
}

func (q InsertQuery) With(ctes ...qx.CTE) InsertQuery {
	q.CTEs = append(q.CTEs, ctes...)
	return q
}

func (q InsertQuery) InsertInto(table qx.BaseTable) InsertQuery {
	q.IntoTable = table
	return q
}

func (q InsertQuery) Columns(fields ...qx.Field) InsertQuery {
	q.InsertFields = append(q.InsertFields, fields...)
	return q
}

func (q InsertQuery) Values(values ...interface{}) InsertQuery {
	q.ValuesList = append(q.ValuesList, values)
	return q
}

func (q InsertQuery) InsertRow(sets ...qx.FieldValueSet) InsertQuery {
	fields, values := make([]qx.Field, len(sets)), make([]interface{}, len(sets))
	for i := range sets {
		fields[i] = sets[i].Field
		values[i] = sets[i].Value
	}
	if len(q.InsertFields) == 0 {
		q.InsertFields = fields
	}
	q.ValuesList = append(q.ValuesList, values)
	return q
}

func (q InsertQuery) Select(selectQuery SelectQuery) InsertQuery {
	q.SelectQuery = &selectQuery
	return q
}

func (q InsertQuery) OnConflict(fields ...qx.Field) insertConflict {
	q.HandleConflict = true
	q.ConflictFields = fields
	return insertConflict{insertQuery: &q}
}

func (q InsertQuery) OnConflictOnConstraint(name string) insertConflict {
	q.HandleConflict = true
	q.ConflictConstraint = name
	return insertConflict{insertQuery: &q}
}

type insertConflict struct{ insertQuery *InsertQuery }

func (c insertConflict) Where(predicates ...qx.Predicate) insertConflict {
	c.insertQuery.ConflictPredicates.Predicates = append(c.insertQuery.ConflictPredicates.Predicates, predicates...)
	return c
}

func (c insertConflict) DoNothing() InsertQuery {
	if c.insertQuery == nil {
		return InsertQuery{}
	}
	return *c.insertQuery
}

func (c insertConflict) DoUpdateSet(sets ...qx.FieldValueSet) InsertQuery {
	if c.insertQuery == nil {
		return InsertQuery{}
	}
	c.insertQuery.Resolution = append(c.insertQuery.Resolution, sets...)
	return *c.insertQuery
}

func Excluded(field qx.Field) qx.CustomField {
	return qx.CustomField{Format: "EXCLUDED." + field.GetName()}
}

func (q InsertQuery) Where(predicates ...qx.Predicate) InsertQuery {
	q.ResolutionPredicates.Predicates = append(q.ResolutionPredicates.Predicates, predicates...)
	return q
}

func (q InsertQuery) Returning(fields ...qx.Field) InsertQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

func (q InsertQuery) Returningx(mapper func(Row), accumulator func()) InsertQuery {
	q.Mapper = mapper
	q.Accumulator = accumulator
	return q
}

func (q InsertQuery) ReturningRowx(mapper func(Row)) InsertQuery {
	q.Mapper = mapper
	return q
}

func (q InsertQuery) Exec(db qx.Queryer) (err error) {
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
	r := &QyRow{QxRow: &qx.QxRow{}}
	if q.Mapper != nil {
		q.Mapper(r) // call the mapper once on the *Row to get all the selected that the user is interested in
	}
	q.ReturningFields = r.QxRow.Fields // then, transfer the selected collected by *Row to the InsertQuery
	r.QxRow.Active = true              // mark Row as active i.e.
	query, args := q.ToSQL()
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	var rowcount int
	if len(q.ReturningFields) == 0 {
		// if user didn't specify any fields to return, don't bother scanning anything and return early
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

func (q InsertQuery) As(alias string) InsertQuery {
	q.Alias = alias
	return q
}

func (q InsertQuery) GetAlias() string {
	return q.Alias
}

func (q InsertQuery) GetName() string {
	return ""
}

func (q InsertQuery) NestThis() qx.Query {
	q.Nested = true
	return q
}
