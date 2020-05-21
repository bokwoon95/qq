package qx

import (
	"strings"
)

// JoinGroup represents an SQL join.
type JoinGroup struct {
	JoinType     JoinType
	Table        Table
	OnPredicates VariadicPredicate
}

// Join constructs a new JoinGroup. Meant to be used if you want to do a custom
// join, like NATURAL JOIN.
func Join(joinType string, table Table, predicates ...Predicate) JoinGroup {
	return JoinGroup{
		JoinType: JoinType(joinType),
		Table:    table,
		OnPredicates: VariadicPredicate{
			Toplevel:   true,
			Predicates: predicates,
		},
	}
}

// JoinGroups is a list of JoinGroups.
type JoinGroups []JoinGroup

// WriteSQL will write the JOIN clause into the buffer and args. If there are
// no JoinGroups it simply writes nothing into the buffer. It returns a flag
// indicating whether anything was written into the buffer.
func (joins JoinGroups) WriteSQL(buf *strings.Builder, args *[]interface{}) (written bool) {
	for i := range joins {
		if joins[i].Table == nil {
			continue
		}
		tableQuery, tableArgs := joins[i].Table.ToSQL()
		if tableQuery == "" {
			continue
		}
		if _, ok := joins[i].Table.(Query); ok {
			tableQuery = "(" + tableQuery + ")"
		}
		if joins[i].JoinType == "" {
			joins[i].JoinType = JoinTypeDefault
		}
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		if joins[i].Table.GetAlias() != "" {
			buf.WriteString(string(joins[i].JoinType) + " " + tableQuery + " AS " + joins[i].Table.GetAlias())
		} else {
			buf.WriteString(string(joins[i].JoinType) + " " + tableQuery)
		}
		*args = append(*args, tableArgs...)
		written = true
		joins[i].OnPredicates.Toplevel = true
		joins[i].OnPredicates.WriteSQL(buf, args, "ON ", "", nil)
	}
	return written
}
