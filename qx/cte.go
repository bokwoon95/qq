package qx

import (
	"strings"
)

// CTE represents an SQL Common Table Expression.
type CTE struct {
	Name  string
	Query Query
}

// ToSQL simply returns the name of the CTE.
func (cte CTE) ToSQL() (string, []interface{}) {
	return cte.Name, nil
}

func NewCTE(name string, query Query) CTE {
	return CTE{
		Name:  name,
		Query: query,
	}
}

// GetAlias implements the Table interface. It always returns an empty string,
// because CTEs do not have aliases (only AliasedCTEs do).
func (cte CTE) GetAlias() string {
	return ""
}

// GetAlias implements the Table interface. It returns the name of the CTE.
func (cte CTE) GetName() string {
	return cte.Name
}

// Get returns a Field from the CTE identified by fieldName. No checks are done
// to see if the fieldName really exists in the CTE at all, CTE simply prepends
// its own name to the fieldName.
func (cte CTE) Get(fieldName string) CustomField {
	return CustomField{Format: cte.Name + "." + fieldName}
}

// CTEs represents a list of CTEs
type CTEs []CTE

// WriteSQL will write the CTE clause into the buffer and args. If there are no
// CTEs to be written, it will simply write nothing. It returns a flag
// indicating whether it wrote anything into the buffer.
func (ctes CTEs) WriteSQL(buf *strings.Builder, args *[]interface{}) (written bool) {
	ctesQueries, ctesArgs := []string{}, []interface{}{}
	for i := range ctes {
		if ctes[i].Query == nil {
			continue
		}
		cteQuery, cteArgs := ctes[i].Query.NestThis().ToSQL()
		if cteQuery == "" {
			continue
		}
		cteQuery = ctes[i].Name + " AS (" + cteQuery + ")"
		ctesQueries = append(ctesQueries, cteQuery)
		ctesArgs = append(ctesArgs, cteArgs...)
	}
	if len(ctesQueries) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString("WITH " + strings.Join(ctesQueries, ", "))
		*args = append(*args, ctesArgs...)
		return true
	}
	return false
}

// AliasedCTE is an aliased version of a CTE derived from a parent CTE.
type AliasedCTE struct {
	Name  string
	Alias string
}

// As returns a an Aliased CTE derived from the parent CTE that it was called
// on.
func (cte CTE) As(alias string) AliasedCTE {
	return AliasedCTE{Name: cte.Name, Alias: alias}
}

// ToSQL returns the name of the parent CTE the AliasedCTE was derived from.
// There is no need to provide the alias, as the caller of ToSQL() should be
// responsible for calling GetAlias() as well.
func (cte AliasedCTE) ToSQL() (string, []interface{}) {
	return cte.Name, nil
}

// GetAlias implements the Table interface. It returns the alias of the
// AliasedCTE.
func (cte AliasedCTE) GetAlias() string {
	return cte.Alias
}

// GetAlias implements the Table interface. It returns the name of the parent
// CTE.
func (cte AliasedCTE) GetName() string {
	return cte.Name
}

// Get returns a Field from the AliasedCTE identified by fieldName. No checks
// are done to see if the fieldName really exists in the AliasedCTE at all,
// AliasedCTE simply prepends its own alias to the fieldName.
func (cte AliasedCTE) Get(fieldName string) CustomField {
	return CustomField{Format: cte.Alias + "." + fieldName}
}
