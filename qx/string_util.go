package qx

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// TODO: rewrite this documentation
// defaultSprintf operates similar to fmt.Sprintf except it only recognizes ? question
// mark as a format specifier. It replaces each ? in the format string with the
// corresponding string representation of value in the values slice. Most types
// in the package are recognized: Table, Predicate, Field, Fields,
// FieldValueSet, FieldValueSets. Basically if it has an SQL representation,
// Sprintf will extract the query and args from it and add it to the format
// string and output args accordingly. If it's not one of the recognized types,
// Sprintf will simply treat it as a literal argument and add a "?" to the
// format string and the literal value to the output args. To escape a question
// mark ?, use two question marks ?? instead.
func defaultSprintf(format string, values []interface{}, excludeTableQualifiers []string) (string, []interface{}) {
	var allQueries []string
	var allArgs []interface{}
	for i := range values {
		var query string
		var args []interface{}
		switch value := values[i].(type) {
		case nil:
			query, args = "NULL", nil
		case Table:
			query, args = value.ToSQL()
		case Predicate:
			query, args = value.ToSQL(excludeTableQualifiers)
		case Field:
			query, args = value.ToSQL(excludeTableQualifiers)
		case Fields:
			buf := &strings.Builder{}
			value.WriteSQL(buf, &args, "", "", excludeTableQualifiers)
			query = buf.String()
		case FieldValueSet:
			sets := FieldValueSets{value}
			buf := &strings.Builder{}
			sets.WriteSQL(buf, &args, "", "", excludeTableQualifiers)
			query = buf.String()
		case FieldValueSets:
			buf := &strings.Builder{}
			value.WriteSQL(buf, &args, "", "", excludeTableQualifiers)
			query = buf.String()
		case ValuesList:
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

// MySQLToPostgresPlaceholders will replace all MySQL style ? with Postgres
// style incrementing placeholders i.e. $1, $2, $3 etc. To escape a literal
// question mark ? , use two question marks ?? instead.
func MySQLToPostgresPlaceholders(query string) string {
	buf := &strings.Builder{}
	i := 0
	for {
		p := strings.Index(query, "?")
		if p < 0 {
			break
		}
		buf.WriteString(query[:p])
		if len(query[p:]) > 1 && query[p:p+2] == "??" {
			buf.WriteString("?")
			query = query[p+2:]
		} else {
			i++
			buf.WriteString("$" + strconv.Itoa(i))
			query = query[p+1:]
		}
	}
	buf.WriteString(query)
	return buf.String()
}

// RandomString is the RandStringBytesMaskImprSrcSB function taken from
// https://stackoverflow.com/a/31832326. It generates a random alphabetical
// string of length n.
func RandomString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	var src = rand.NewSource(time.Now().UnixNano())
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return sb.String()
}

func ArgToString(arg interface{}) string {
	var str string // str is the SQL string representation of arg
	switch v := arg.(type) {
	case nil:
		str = "NULL"
	case bool:
		if v {
			str = "TRUE"
		} else {
			str = "FALSE"
		}
	case string:
		str = "'" + v + "'"
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		str = fmt.Sprint(arg)
	case time.Time:
		// consider using the AppendFormat custom allocation trick here
		// https://segment.com/blog/allocation-efficiency-in-high-performance-go-services/
		str = "'" + v.Format(time.RFC3339Nano) + "'"
	case driver.Valuer:
		Interface, err := v.Value()
		if err != nil {
			str = "(" + err.Error() + ")"
		} else {
			switch Concrete := Interface.(type) {
			case string:
				str = "'" + Concrete + "'"
			default:
				str = "NULL"
			}
		}
	default:
		b, err := json.Marshal(arg)
		if err != nil {
			str = "(" + err.Error() + ")"
		} else {
			str = "'" + string(b) + "'"
		}
	}
	return str
}

func PostgresInterpolateSQL(query string, args ...interface{}) string {
	oldnewSets := make(map[int][]string)
	for i, arg := range args {
		str := ArgToString(arg)
		placeholder := "$" + strconv.Itoa(i+1)
		oldnewSets[len(placeholder)] = append(oldnewSets[len(placeholder)], placeholder, str)
	}
	result := query
	for i := len(oldnewSets) + 1; i >= 2; i-- {
		result = strings.NewReplacer(oldnewSets[i]...).Replace(result)
	}
	return result
}

func MySQLInterpolateSQL(query string, args ...interface{}) string {
	buf := &strings.Builder{}
	// i is the position of the ? in the query
	for i := strings.Index(query, "?"); i >= 0 && len(args) > 0; i = strings.Index(query, "?") {
		buf.WriteString(query[:i])
		if len(query[i:]) > 1 && query[i:i+2] == "??" {
			buf.WriteString("?")
			query = query[i+2:]
			continue
		}
		buf.WriteString(ArgToString(args[0]))
		query = query[i+1:]
		args = args[1:]
	}
	buf.WriteString(query)
	return buf.String()
}
