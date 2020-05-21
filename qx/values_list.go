package qx

import (
	"strings"
)

// ValuesList represents the VALUES (a, b, c...), (d, e, f...), (g, h, i...)
// SQL clause.
type ValuesList [][]interface{}

// WriteSQL will write the VALUES clause into the buffer and args as described
// in the ValuesList description. If there are no values it will not write
// anything into the buffer. It returns a flag indicating whether anything was
// written into the buffer.
func (vl ValuesList) WriteSQL(buf *strings.Builder, args *[]interface{}, prependWith, appendWith string) (written bool) {
	valuesQueries, valuesArgs := []string{}, []interface{}{}
	for i := range vl {
		if len(vl[i]) == 0 {
			continue
		}
		valueQueries, valueArgs := []string{}, []interface{}{}
		for j := range vl[i] {
			if field, ok := vl[i][j].(Field); ok && field != nil {
				fieldQuery, fieldArgs := field.ToSQL(nil)
				valueQueries = append(valueQueries, fieldQuery)
				valueArgs = append(valueArgs, fieldArgs...)
			} else {
				valueQueries = append(valueQueries, "?")
				valueArgs = append(valueArgs, vl[i][j])
			}
		}
		valuesQueries = append(valuesQueries, "("+strings.Join(valueQueries, ", ")+")")
		valuesArgs = append(valuesArgs, valueArgs...)
	}
	if len(valuesQueries) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(prependWith + strings.Join(valuesQueries, ", ") + appendWith)
		*args = append(*args, valuesArgs...)
		return true
	}
	return false
}
