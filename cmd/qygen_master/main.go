package main

import (
	"database/sql"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	_ "github.com/lib/pq"
)

func main() {
	currdir, err := os.Getwd()
	if err != nil {
		printAndExit(err)
	}
	var (
		databaseFlag  = flag.String("database", "", "(required) Database URL")
		directoryFlag = flag.String("directory", filepath.Join(currdir, "tables"), "(optional) Directory to place the generated file. Can be absolute or relative filepath")
		dryrunFlag    = flag.Bool("dryrun", false, "(optional) Print the list of tables to be generated without generating the file")
		fileFlag      = flag.String("file", "tables.go", "(optional) Name of the file to be generated. If file already exists, -overwrite flag must be specified to overwrite the file")
		overwriteFlag = flag.Bool("overwrite", false, "(optional) Overwrite any files that already exist")
		packageFlag   = flag.String("package", "tables", "(optional) Package name of the file to be generated")
		schemasFlag   = flag.String("schema", "public", "(optional) A comma separated list of schemas that you want to generate tables for. Please don't include any spaces")
	)
	flag.Parse()
	log.SetFlags(log.Lshortfile)
	if len(os.Args[1:]) == 0 {
		flag.PrintDefaults()
		return
	}
	config, err := prepConfig(*databaseFlag, *schemasFlag, *directoryFlag, *fileFlag, *packageFlag, *dryrunFlag, *overwriteFlag)
	if err != nil {
		printAndExit(err)
	}
	db, err := sql.Open("postgres", config.databaseURL)
	if err != nil {
		printAndExit(err)
	}
	err = db.Ping()
	if err != nil {
		printAndExit("Could not ping the database, is the database reachable via " + config.databaseURL + "? " + err.Error())
	}

	//--------------------------------------------------------------------------------//
	// This is the step you should be modifying if you wish to control what
	// tables get generated. Any tables you pass on to the next step will be
	// written into the file. You can filter tables, modify names, or directly
	// edit the getTables/processTables functions. You can split tables by
	// schema and write them to separate packages and files by calling
	// writeTablesToFile repeatedly below.
	tables, err := processTables(getTables(db, config.databaseURL, config.schemas))
	if err != nil {
		printAndExit(err)
	}
	if config.dryrun {
		// Here is where you can print the list of filtered tables before you
		// actually write it out to a file.
		for _, table := range tables {
			fmt.Println(table)
		}
		return
	}
	//--------------------------------------------------------------------------------//

	err = writeTablesToFile(tables, config.directory, config.file, config.packageName)
	if err != nil {
		printAndExit(err)
	}
	fmt.Println("Result:      ", strconv.Itoa(len(tables)), "tables written into", filepath.Join(config.directory, config.file))
}

type Table struct {
	Schema      string
	Name        string
	StructName  string
	RawType     string
	Constructor string
	Fields      []Field
}

type Field struct {
	Name        string
	RawType     string
	Type        string
	Constructor string
}

// getTables will get all tables in a database for a list of schemas. It does
// not do any column type classification (i.e. which column is of type string,
// which column is of type integer etc). getTables simply stores the string
// descriptor of the column type into field.RawType, where it will be
// classified later by processTables.
func getTables(db *sql.DB, databaseURL string, schemas []string) ([]Table, error) {
	var tables []Table
	query := replacePlaceholders(
		"SELECT t.table_type, c.table_schema, c.table_name, c.column_name, c.data_type" +
			" FROM information_schema.tables AS t" +
			" JOIN information_schema.columns AS c USING (table_schema, table_name)" +
			" WHERE table_schema IN (?" + strings.Repeat(", ?", len(schemas)-1) + ")" +
			// https://stackoverflow.com/q/4088532
			" ORDER BY c.table_schema <> 'public', c.table_schema, t.table_type, c.table_name, c.column_name",
	)
	args := make([]interface{}, len(schemas))
	for i := range schemas {
		args[i] = schemas[i]
	}
	rows, err := db.Query(query, args...)
	fmt.Println("Query:       ", query, args)
	if err != nil {
		return tables, err
	}
	defer rows.Close()
	tableIndices := make(map[string]int)
	for rows.Next() {
		// Each row represents a specific column of specific table in the database
		var tableType, tableSchema, tableName, columnName, columnType string
		err := rows.Scan(&tableType, &tableSchema, &tableName, &columnName, &columnType)
		if err != nil {
			return tables, err
		}
		fullTableName := tableSchema + "." + tableName
		if _, ok := tableIndices[fullTableName]; !ok {
			// table does has not been created in the slice of Tables, create a
			// new one and append it to the slice
			table := Table{
				Schema:  tableSchema,
				Name:    tableName,
				RawType: tableType,
			}
			tables = append(tables, table)
			tableIndices[fullTableName] = len(tables) - 1
		}
		// create a new field and append it to the slice of Fields in the Table
		index := tableIndices[fullTableName]
		field := Field{
			Name:    columnName,
			RawType: columnType,
		}
		tables[index].Fields = append(tables[index].Fields, field)
	}
	return tables, nil
}

const (
	// https://www.postgresql.org/docs/current/infoschema-tables.html
	TableTypeBaseTable      = "BASE TABLE"
	TableTypeView           = "VIEW"
	TableTypeForeignTable   = "FOREIGN TABLE"
	TableTypeLocalTemporary = "LOCAL TEMPORARY"

	FieldTypeBoolean  = "qx.BooleanField"
	FieldTypeJSON     = "qx.JSONField"
	FieldTypeNumber   = "qx.NumberField"
	FieldTypeString   = "qx.StringField"
	FieldTypeTime     = "qx.TimeField"

	FieldConstructorBoolean  = "qx.NewBooleanField"
	FieldConstructorJSON     = "qx.NewJSONField"
	FieldConstructorNumber   = "qx.NewNumberField"
	FieldConstructorString   = "qx.NewStringField"
	FieldConstructorTime     = "qx.NewTimeField"
)

// processTables will walk through each table and its columns (fields) and annotate
// the table.StructName, table.Constructor, field.Type, field.Constructor based
// on table.RawType and field.RawType.
func processTables(inputTables []Table, err error) ([]Table, error) {
	if err != nil {
		return inputTables, err
	}
	var outputTables []Table
	for _, table := range inputTables {
		switch table.RawType {
		case TableTypeBaseTable:
			if table.Schema == "public" {
				table.StructName = "TABLE_" + strings.ToUpper(table.Name)
				table.Constructor = strings.ToUpper(table.Name)
			} else {
				table.StructName = "TABLE_" + strings.ToUpper(table.Schema+"__"+table.Name)
				table.Constructor = strings.ToUpper(table.Schema + "__" + table.Name)
			}
		case TableTypeView:
			if table.Schema == "public" {
				table.StructName = "VIEW_" + strings.ToUpper(table.Name)
				table.Constructor = strings.ToUpper(table.Name)
			} else {
				table.StructName = "VIEW_" + strings.ToUpper(table.Schema+"__"+table.Name)
				table.Constructor = strings.ToUpper(table.Schema + "__" + table.Name)
			}
		default:
			continue
		}
		var fields []Field
		for _, field := range table.Fields {
			switch {
			case isBoolean(field.RawType):
				field.Type = FieldTypeBoolean
				field.Constructor = FieldConstructorBoolean
			case isJSON(field.RawType):
				field.Type = FieldTypeJSON
				field.Constructor = FieldConstructorJSON
			case isNumber(field.RawType):
				field.Type = FieldTypeNumber
				field.Constructor = FieldConstructorNumber
			case isString(field.RawType):
				field.Type = FieldTypeString
				field.Constructor = FieldConstructorString
			case isTime(field.RawType):
				field.Type = FieldTypeTime
				field.Constructor = FieldConstructorTime
			default:
				continue
			}
			fields = append(fields, field)
		}
		table.Fields = fields
		outputTables = append(outputTables, table)
	}
	return outputTables, nil
}

type FileData struct {
	PackageName string
	Imports     []string
	Tables      []Table
}

var Imports = []string{
	"github.com/bokwoon95/qx-postgres/qx",
}

var qxgenTemplate = `// Code generated by qxgen; DO NOT EDIT.
package {{$.PackageName}}

import (
	{{- range $_, $import := $.Imports}}
	"{{$import}}"
	{{- end}}
)
{{- range $_, $table := $.Tables}}
{{template "table_struct_definition" $table}}
{{template "table_constructor" $table}}
{{template "table_as" $table}}
{{- end}}

{{- define "table_struct_definition"}}
{{- with $table := .}}
type {{uppercase $table.StructName}} struct {
	*qx.TableInfo
	{{- range $_, $field := $table.Fields}}
	{{uppercase $field.Name}} {{$field.Type}}
	{{- end}}
}
{{- end}}
{{- end}}

{{- define "table_constructor"}}
{{- with $table := .}}
func {{$table.Constructor}}() {{$table.StructName}} {
	tbl := {{$table.StructName}}{TableInfo: qx.NewTableInfo("{{$table.Schema}}", "{{$table.Name}}")}
	{{- range $_, $field := $table.Fields}}
	tbl.{{uppercase $field.Name}} = {{$field.Constructor}}("{{$field.Name}}", tbl.TableInfo)
	{{- end}}
	return tbl
}
{{- end}}
{{- end}}

{{- define "table_as"}}
{{- with $table := .}}
func (tbl {{$table.StructName}}) As(alias string) {{$table.StructName}} {
	tbl2 := {{$table.Constructor}}()
	tbl2.TableInfo.Alias = alias
	return tbl2
}
{{- end}}
{{- end}}`

// writeTablesToFile will write the tables into a file specified by
// filepath.Join(directory, file).
func writeTablesToFile(tables []Table, directory, file, packageName string) error {
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return fmt.Errorf("Could not create directory %s: %w", directory, err)
	}
	filename := filepath.Join(directory, file)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	t, err := template.New("").Funcs(template.FuncMap{"uppercase": strings.ToUpper}).Parse(qxgenTemplate)
	if err != nil {
		return err
	}
	data := FileData{
		PackageName: packageName,
		Imports:     Imports,
		Tables:      tables,
	}
	err = t.Execute(f, data)
	if err != nil {
		return err
	}
	if _, err := exec.LookPath("goimports"); err == nil {
		_ = exec.Command("goimports", "-w", filename).Run()
	} else if _, err := exec.LookPath("gofmt"); err == nil {
		_ = exec.Command("gofmt", "-w", filename).Run()
	}
	return nil
}

type Config struct {
	databaseURL string
	directory   string
	dryrun      bool
	file        string
	overwrite   bool
	packageName string
	schemas     []string
}

// prepConfig will process the incoming data and initialize the config object
// accordingly
func prepConfig(database, schemas, directory, file, packageName string, dryrun, overwrite bool) (cfg Config, err error) {
	// databaseURL
	cfg.databaseURL = database
	fmt.Println("Database URL:", cfg.databaseURL)
	if cfg.databaseURL == "" {
		return cfg, fmt.Errorf("Database URL is either empty or not passed in. You need to specify a database URL with the -database option.")
	}

	// schemas
	if schemas == "" {
		return cfg, fmt.Errorf("At least one database schema needs to be specified")
	}
	cfg.schemas = strings.FieldsFunc(schemas, func(r rune) bool { return r == ',' || unicode.IsSpace(r) })
	fmt.Println("Schemas:     ", cfg.schemas)

	// directory
	cfg.directory = directory
	fmt.Println("Directory:   ", cfg.directory)
	if cfg.directory == "" {
		return cfg, fmt.Errorf("-directory was not specified. You need to provide a directory to place the generated file in")
	}
	cfg.directory, err = filepath.Abs(cfg.directory)
	if err != nil {
		return cfg, err
	}

	// file
	cfg.file = file
	fmt.Println("File:        ", cfg.file)
	if cfg.file == "" {
		return cfg, fmt.Errorf("-file was not specified. You need to provide a file name (e.g. tables.go) for the generated file")
	}
	if !strings.HasSuffix(cfg.file, ".go") {
		cfg.file = cfg.file + ".go"
	}
	asboluteFilePath := filepath.Join(directory, file)
	if _, err := os.Stat(asboluteFilePath); err == nil && !overwrite {
		return cfg, fmt.Errorf("Specified file %s already exists. If you wish to overwrite it, provide the -overwrite flag", asboluteFilePath)
	}

	// packageName
	cfg.packageName = packageName
	fmt.Println("Package Name:", cfg.packageName)
	if cfg.packageName == "" {
		return cfg, fmt.Errorf("-package name was not provided. You need to provide the package name for the generated file")
	}

	// dryrun
	cfg.dryrun = dryrun
	return cfg, nil
}

/* Utility functions */

func printAndExit(v ...interface{}) {
	fmt.Println("======================================== ERROR! ========================================")
	log.Output(2, fmt.Sprintln(v...))
	os.Exit(1)
}

// replacePlaceholders will replace all ? placeholders in a query string to the
// postgres-valid incrementing placeholders $1, $2, $3 etc.
func replacePlaceholders(query string) string {
	buf := &strings.Builder{}
	i := 0
	for pos := strings.Index(query, "?"); pos >= 0; pos = strings.Index(query, "?") {
		i++
		buf.WriteString(query[:pos] + "$" + strconv.Itoa(i))
		query = query[pos+1:]
	}
	buf.WriteString(query)
	return buf.String()
}

// String implements fmt.Stringer for type Table, allowing you to call
// fmt.Println(table) and have it formatted accordingly.
func (table Table) String() string {
	var output string
	if table.Constructor != "" && table.StructName != "" {
		output += fmt.Sprintf("%s.%s => func %s() %s\n", table.Schema, table.Name, table.Constructor, table.StructName)
	} else {
		output += fmt.Sprintf("%s.%s\n", table.Schema, table.Name)
	}
	for _, field := range table.Fields {
		if field.Constructor != "" && field.Type != "" {
			output += fmt.Sprintf("    %s: %s => %s\n", field.Name, field.RawType, field.Type)
		} else {
			output += fmt.Sprintf("    %s: %s\n", field.Name, field.RawType)
		}
	}
	return output
}

/* Type classification functions */

func isBoolean(rawtype string) bool {
	// https://www.postgresql.org/docs/current/datatype-boolean.html
	return rawtype == "boolean"
}

func isJSON(rawtype string) bool {
	// https://www.postgresql.org/docs/current/datatype.html Table 8.1
	return strings.HasPrefix(rawtype, "json")
}

func isNumber(rawtype string) bool {
	// https://www.postgresql.org/docs/current/datatype-numeric.html
	switch rawtype {
	case "smallint", "integer", "bigint", "decimal", "numeric",
		"real", "double precision", "smallserial", "serial", "bigserial":
		return true
	case "oid":
		// https://www.postgresql.org/docs/current/datatype-oid.html
		return true
	default:
		return false
	}
}

func isString(rawtype string) bool {
	// https://www.postgresql.org/docs/current/datatype-character.html
	switch {
	case rawtype == "text", strings.HasPrefix(rawtype, "char"), strings.HasPrefix(rawtype, "varchar"):
		return true
	case rawtype == "name":
		// https://dba.stackexchange.com/questions/217533/what-is-the-data-type-name-in-postgresql
		return true
	default:
		return false
	}
}

func isTime(rawtype string) bool {
	// https://www.postgresql.org/docs/current/datatype-datetime.html
	switch {
	case strings.HasPrefix(rawtype, "time"), rawtype == "date":
		return true
	default:
		return false
	}
}
