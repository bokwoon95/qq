package qy

import (
	"database/sql"
	"strings"
	"time"

	"github.com/bokwoon95/qx-postgres/qx"
	"github.com/lib/pq"
)

type Row interface {
	ScanArray(array interface{}, f qx.Field)
	ScanInto(dest interface{}, f qx.Field)
	// bool
	Bool(qx.BooleanField) bool
	Bool_(qx.Field) bool
	BoolValid(qx.BooleanField) bool
	BoolValid_(qx.Field) bool
	NullBool(qx.BooleanField) sql.NullBool
	NullBool_(qx.Field) sql.NullBool
	// float64
	Float64(qx.NumberField) float64
	Float64_(qx.Field) float64
	Float64Valid(qx.NumberField) bool
	Float64Valid_(qx.Field) bool
	NullFloat64(qx.NumberField) sql.NullFloat64
	NullFloat64_(qx.Field) sql.NullFloat64
	// int
	Int(qx.NumberField) int
	Int_(qx.Field) int
	IntValid(qx.NumberField) bool
	IntValid_(qx.Field) bool
	// int32
	Int32(qx.NumberField) int32
	Int32_(qx.Field) int32
	Int32Valid(qx.NumberField) bool
	Int32Valid_(qx.Field) bool
	NullInt32(qx.NumberField) sql.NullInt32
	NullInt32_(qx.Field) sql.NullInt32
	// int64
	Int64(qx.NumberField) int64
	Int64_(qx.Field) int64
	Int64Valid(qx.NumberField) bool
	Int64Valid_(qx.Field) bool
	NullInt64(qx.NumberField) sql.NullInt64
	NullInt64_(qx.Field) sql.NullInt64
	// string
	String(qx.StringField) string
	String_(qx.Field) string
	StringValid(qx.StringField) bool
	StringValid_(qx.Field) bool
	NullString(qx.StringField) sql.NullString
	NullString_(qx.Field) sql.NullString
	// time.Time
	Time(qx.TimeField) time.Time
	Time_(qx.Field) time.Time
	TimeValid(qx.TimeField) bool
	TimeValid_(qx.Field) bool
	NullTime(qx.TimeField) sql.NullTime
	NullTime_(qx.Field) sql.NullTime
}

// QyRow is a wrapper around QxRow that additionally implements the scanning of
// postgres arrays into go slices. Only []bool, []float64, []int64 or
// []string slices are supported.
type QyRow struct {
	*qx.QxRow
}

// ScanArray implements Row interface. It received a pointer to a slice and
// scans a postgres array into that slice. Only []bool, []float64, []int64 or
// []string slices are supported.
func (r *QyRow) ScanArray(array interface{}, f qx.Field) {
	if !r.QxRow.Active {
		r.QxRow.Fields = append(r.QxRow.Fields, f)
		r.QxRow.Dest = append(r.QxRow.Dest, pq.Array(array))
	}
}

func Fieldf(format string, values ...interface{}) qx.CustomField {
	return qx.CustomField{
		Format:        format,
		Values:        values,
		CustomSprintf: CustomSprintf,
	}
}

func Predicatef(format string, values ...interface{}) qx.CustomPredicate {
	return qx.CustomPredicate{
		Format:        format,
		Values:        values,
		CustomSprintf: CustomSprintf,
	}
}

func Tablef(format string, values ...interface{}) qx.CustomTable {
	return qx.CustomTable{
		Format:        format,
		Values:        values,
		CustomSprintf: CustomSprintf,
	}
}

func Queryf(format string, values ...interface{}) qx.CustomQuery {
	return qx.CustomQuery{
		Postgres:      true,
		Format:        format,
		Values:        values,
		CustomSprintf: CustomSprintf,
	}
}

// CustomSprintf ...
func CustomSprintf(format string, values []interface{}, excludeTableQualifiers []string) (string, []interface{}) {
	var allQueries []string
	var allArgs []interface{}
	for i := range values {
		var query string
		var args []interface{}
		switch value := values[i].(type) {
		case nil:
			query, args = "NULL", nil
		case qx.Table:
			query, args = value.ToSQL()
		case qx.Predicate:
			query, args = value.ToSQL(excludeTableQualifiers)
		case qx.Field:
			query, args = value.ToSQL(excludeTableQualifiers)
		case qx.Fields:
			buf := &strings.Builder{}
			value.WriteSQL(buf, &args, "", "", excludeTableQualifiers)
			query = buf.String()
		case qx.FieldValueSet:
			sets := qx.FieldValueSets{value}
			buf := &strings.Builder{}
			sets.WriteSQL(buf, &args, "", "", excludeTableQualifiers)
			query = buf.String()
		case qx.FieldValueSets:
			buf := &strings.Builder{}
			value.WriteSQL(buf, &args, "", "", excludeTableQualifiers)
			query = buf.String()
		case qx.ValuesList:
			buf := &strings.Builder{}
			value.WriteSQL(buf, &args, "", "")
			query = buf.String()
		// lmao tfw no generics
		case []int:
			if len(value) == 0 {
				return "", nil
			}
			query = "?" + strings.Repeat(", ?", len(value)-1)
			args = make([]interface{}, len(value))
			for i := range value {
				args[i] = value[i]
			}
		case []int64:
			if len(value) == 0 {
				return "", nil
			}
			query = "?" + strings.Repeat(", ?", len(value)-1)
			args = make([]interface{}, len(value))
			for i := range value {
				args[i] = value[i]
			}
		case []float64:
			if len(value) == 0 {
				return "", nil
			}
			query = "?" + strings.Repeat(", ?", len(value)-1)
			args = make([]interface{}, len(value))
			for i := range value {
				args[i] = value[i]
			}
		case []string:
			if len(value) == 0 {
				return "", nil
			}
			query = "?" + strings.Repeat(", ?", len(value)-1)
			args = make([]interface{}, len(value))
			for i := range value {
				args[i] = value[i]
			}
		case []bool:
			if len(value) == 0 {
				return "", nil
			}
			query = "?" + strings.Repeat(", ?", len(value)-1)
			args = make([]interface{}, len(value))
			for i := range value {
				args[i] = value[i]
			}
		case []interface{}:
			if len(value) == 0 {
				return "", nil
			}
			args = make([]interface{}, len(value))
			query = "?" + strings.Repeat(", ?", len(value)-1)
			for i := range value {
				args[i] = value[i]
			}
		case int, int8, int16, uint8, uint16, uint32:
			query, args = "?::INT", []interface{}{value}
		case int64, uint64:
			query, args = "?::BIGINT", []interface{}{value}
		case float32, float64:
			query, args = "?::FLOAT", []interface{}{value}
		case string:
			query, args = "?::TEXT", []interface{}{value}
		case time.Time:
			query, args = "?::TIMESTAMPTZ", []interface{}{value}
		case bool:
			query, args = "?::BOOLEAN", []interface{}{value}
		default:
			query, args = "?", []interface{}{value}
		}
		allQueries = append(allQueries, query)
		allArgs = append(allArgs, args...)
	}
	buf := &strings.Builder{}
	for i := strings.Index(format, "?"); i >= 0 && len(allQueries) > 0; i = strings.Index(format, "?") {
		buf.WriteString(format[:i])
		if len(format[i:]) > 1 && format[i:i+2] == "??" {
			buf.WriteString("?")
			format = format[i+2:]
			continue
		}
		buf.WriteString(allQueries[0])
		format = format[i+1:]
		allQueries = allQueries[1:]
	}
	buf.WriteString(format)
	return buf.String(), allArgs
}
