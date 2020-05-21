package qx

import (
	"context"
	"database/sql"
)

// Table is an interface representing anything that you can SELECT FROM or
// JOIN.
type Table interface {
	ToSQL() (string, []interface{})
	GetAlias() string
	GetName() string // Table name must exclude the schema (if any)
}

// Query is an interface that specialises the Table interface. It covers only
// queries like SELECT/INSERT/UPDATE/DELETE.
type Query interface {
	Table
	NestThis() Query
}

// BaseTable is an interface that specialises the Table interface. It covers
// only tables/views that exist in the database.
type BaseTable interface {
	Table
	GetFields() []Field
}

// Predicate is an interface that evaluates to true or false in SQL.
type Predicate interface {
	// Predicates should propagate the excludeTableQualifiers argument down to
	// its Fields. For info on what excludeTableQualifiers is, look at the
	// Field interface description.
	ToSQL(excludeTableQualifiers []string) (string, []interface{})

	AssertPredicate()
}

// Field is an interface that represents either a Table column or an SQL value.
type Field interface {
	// Fields should respect the excludeTableQualifiers argument in ToSQL().
	// E.g. if the field 'name' belongs to a table called 'users' and the
	// excludeTableQualifiers contains 'users', the field should present itself
	// as 'name' and not 'users.name'. i.e. any table qualifiers in the list
	// must be excluded.
	//
	// This is to play nice with certain clauses in the INSERT and UPDATE
	// queries that expressly forbid table qualified columns.
	ToSQL(excludeTableQualifiers []string) (string, []interface{})

	GetAlias() string
	GetName() string
}

// Queryer is an interface used to query the database.
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// QueryerContext is an extension of the Queryer interface, and can query the
// database with context.
type QueryerContext interface {
	Queryer
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// Logger is an interface that provides logging.
type Logger interface {
	Println(v ...interface{})
}
