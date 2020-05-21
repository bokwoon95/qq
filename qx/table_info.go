package qx

// TableInfo is struct that implements the Table interface, containing all the
// information needed to call itself a Table. It is meant to be embedded in
// arbitrary structs to also transform them into valid Tables.
type TableInfo struct {
	Schema string
	Name   string
	Alias  string
	// Fields contains a list of references to the table's fields. It is
	// ordinarily populated by the generated code, but is not actually used
	// anywhere in the codebase. It is only meant for the end user, to
	// programatically loop through a table's fields.
	Fields []Field
}

// NewTableInfo returns a new TableInfo.
func NewTableInfo(schema, name string) *TableInfo {
	return &TableInfo{
		Schema: schema,
		Name:   name,
	}
}

// ToSQL returns the fully qualified table name.
func (tbl *TableInfo) ToSQL() (string, []interface{}) {
	if tbl == nil {
		return "", nil
	}
	return tbl.Schema + "." + tbl.Name, nil
}

// GetAlias implements the Table interface. It returns the alias from the
// TableInfo.
func (tbl *TableInfo) GetAlias() string {
	if tbl == nil {
		return ""
	}
	return tbl.Alias
}

// GetName implements the Table interface. It returns the name from the
// TableInfo.
func (tbl *TableInfo) GetName() string {
	if tbl == nil {
		return ""
	}
	return tbl.Name
}

// AssertBaseTable implements the BaseTable interface.
func (tbl *TableInfo) AssertBaseTable() {}

func (tbl *TableInfo) GetFields() []Field {
	return tbl.Fields
}
