package main

import (
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/DATA-DOG/go-txdb"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

var (
	dropFlag       = flag.Bool("drop", false, "Drop all tables")
	cleanFlag      = flag.Bool("clean", false, "Cleanly reset the database. Drops all tables and rebuilds it from scratch")
	dockerFlag     = flag.Bool("docker", false, "Use docker's psql instead of native psql")
	testFlag       = flag.Bool("test", false, "Run the tests, stop on first failure")
	testonlyFlag   = flag.Bool("testonly", false, "Run only the tests and nothing else")
	testallFlag    = flag.Bool("testall", false, "Run all tests, even on failure")
	hasCustomFiles bool // Represents whether the user passed in a custom list of files

	databaseURL  string
	databaseName string
	databaseUser string

	// sourcefile is the path to this file
	_, sourcefile, _, _ = runtime.Caller(0)
	// rootdir is the path to the sourcefile's directory
	rootdir = filepath.Dir(sourcefile) + string(os.PathSeparator)

	// dockerRootdir is the path to the project root directory as mounted in the docker postgres container (refer to docker-compose.yml, under services.database_dev.volumes)
	dockerRootdir = "/testdata/"
	// dockerContainerName is the name of the docker container (refer to docker-compose.yml, under services.database_dev.container_name)
	dockerContainerName string
)

// sortedFiles is a struct holding a bunch of sql files categorized into
// different groups
type sortedFiles struct {
	views     []string
	tests     []string
	data      []string
	triggers  []string
	functions []string
}

func cmdExists(command string) bool {
	_, err := exec.LookPath(command)
	return err == nil
}

func main() {
	flag.Parse()
	if err := godotenv.Load(filepath.Join(rootdir, ".env")); err != nil {
		panic("unable to source from .env file")
	}
	txdb.Register("txdb", "postgres", os.Getenv("DATABASE_URL"))
	log.SetFlags(log.LstdFlags | log.Llongfile)
	databaseURL = os.Getenv("DATABASE_URL")
	databaseName = os.Getenv("DATABASE_NAME")
	databaseUser = os.Getenv("DATABASE_USER")
	// The container name is same as the database name (refer to docker-compose.yml, under 'services.database_dev.container_name').
	dockerContainerName = databaseName

	if !cmdExists("psql") && !cmdExists("docker") {
		log.Fatalf("Either psql or docker needs to be installed but both not found, exiting")
	}

	db, err := setupDatabase()
	if err != nil {
		log.Fatalln(err.Error())
	}
	numberOfTables, err := getNumberOfTables(db)
	if err != nil {
		log.Fatalln(err.Error())
	}
	if numberOfTables == 0 {
		fmt.Println("No tables found, will reset the database")
		*cleanFlag = true
	}

	var sorted sortedFiles
	if args := flag.Args(); len(args) == 0 {
		sorted = sortDirs()
	} else {
		hasCustomFiles = true
		sorted = sortFiles(args)
	}

	migrationDir := os.Getenv("MIGRATION_DIR")
	if migrationDir == "" {
		printAndExit("Migration Directory not found (empty string). Please ensure the environment variable MIGRATION_DIR is set.")
	}
	if *dropFlag || *cleanFlag {
		const START = "---------------------------------------- RESETTING THE DATABASE ----------------------------------------"
		const END = "---------------------------------------- RESET SUCCESSFUL ----------------------------------------------"
		fmt.Println(START)
		err = migrationsDrop(migrationDir)
		if err != nil {
			log.Fatalln(err)
		}
		if *dropFlag {
			fmt.Println(END)
			return
		}
		err = migrationsUp(migrationDir)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(END)
	}

	if *testonlyFlag && !*cleanFlag {
		goto RUN_TESTS
	}

	if len(sorted.views) > 0 {
		fmt.Printf("\nLoading view files\n")
		err = loadFiles(sorted.views)
		if err != nil {
			printAndExit("Error loading view files: %s", err)
		}
	}

	if len(sorted.triggers) > 0 {
		if !hasCustomFiles {
			resetSchema(db, "trg")
		}
		fmt.Printf("\nLoading trigger files\n")
		err = loadFiles(sorted.triggers, plpgsqlCheckTrigger(db))
		if err != nil {
			printAndExit("Error loading trigger files: %s", err)
		}
	}

	if len(sorted.functions) > 0 {
		fmt.Printf("\nLoading function files\n")
		err = loadFiles(sorted.functions, plpgsqlCheckFunction(db))
		if err != nil {
			printAndExit("Error loading function files: %s", err)
		}
	}

	if len(sorted.data) > 0 && *cleanFlag {
		fmt.Printf("\nLoading data files\n")
		err = loadFiles(sorted.data)
		if err != nil {
			printAndExit("Error loading data files: %s", err)
		}
	}

RUN_TESTS:
	if len(sorted.tests) > 0 && (hasCustomFiles || *testFlag || *testonlyFlag || *testallFlag) {
		if !hasCustomFiles {
			resetSchema(db, "t")
		}
		fmt.Printf("\nLoading test files\n")
		err = loadFiles(sorted.tests, plpgsqlCheckFunction(db))
		if err != nil {
			printAndExit("Error loading test files: %s", err)
		}
		err = runTests(sorted.tests)
		if err != nil {
			printAndExit("Error running test files: %s", err)
		}
	}
}

func printAndExit(format string, v ...interface{}) {
	fmt.Println("======================================== ERROR! ========================================")
	log.Fatalf(format, v...)
}

func sortDirs() sortedFiles {
	var sorted sortedFiles
	var err error
	sorted.views, err = filepath.Glob(filepath.Join("views", "*.sql"))
	if err != nil {
		panic(err)
	}
	sorted.triggers, err = filepath.Glob(filepath.Join("triggers", "*.sql"))
	if err != nil {
		panic(err)
	}
	sorted.functions, err = filepath.Glob(filepath.Join("functions", "*.sql"))
	if err != nil {
		panic(err)
	}
	sorted.data, err = filepath.Glob(filepath.Join("data", "*sql"))
	if err != nil {
		panic(err)
	}
	sorted.tests, err = filepath.Glob(filepath.Join("tests", "*sql"))
	if err != nil {
		panic(err)
	}
	return sorted
}

func sortFiles(args []string) sortedFiles {
	var sorted sortedFiles
	for _, arg := range args {
		filename, _ := filepath.Abs(arg)
		switch {
		case strings.Contains(filename, filepath.Join(rootdir, "views")):
			sorted.views = append(sorted.views, arg)
		case strings.Contains(filename, filepath.Join(rootdir, "triggers")):
			sorted.triggers = append(sorted.triggers, arg)
		case strings.Contains(filename, filepath.Join(rootdir, "functions")):
			sorted.functions = append(sorted.functions, arg)
		case strings.Contains(filename, filepath.Join(rootdir, "data")):
			sorted.data = append(sorted.data, arg)
		case strings.Contains(filename, filepath.Join(rootdir, "tests")):
			sorted.tests = append(sorted.tests, arg)
		default:
			fmt.Printf("WARNING: %s could not be identified as a view, trigger, function or data script\n", arg)
		}
	}
	return sorted
}

func setupDatabase() (db *sql.DB, err error) {
	// Setup database connection
	if databaseURL == "" {
		return db, fmt.Errorf("Database URL cannot be empty")
	}
	db, err = sql.Open("postgres", databaseURL)
	if err != nil {
		return db, fmt.Errorf("Unable to open the database: %w", err)
	}
	err = db.Ping()
	if err != nil {
		return db, fmt.Errorf("Unable to ping the database: %w", err)
	}
	return db, nil
}

func getNumberOfTables(db *sql.DB) (int, error) {
	var numberOfTables int
	err := db.QueryRow(`
	-- The below query is equivalent to psql's \dt meta-command, except it
	-- counts the number of tables instead of listing the tables. To check what
	-- \dt runs under the hood, start psql with the -E flag and run \dt as usual.
	SELECT
		COUNT(*)
	FROM
		pg_catalog.pg_class c
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE
		c.relkind IN ('r','p','')
		AND n.nspname <> 'pg_catalog'
		AND n.nspname <> 'information_schema'
		AND n.nspname !~ '^pg_toast'
		AND pg_catalog.pg_table_is_visible(c.oid)
	`).Scan(&numberOfTables)
	return numberOfTables, err
}

func plpgsqlCheckTrigger(db *sql.DB) func(string) error {
	return func(filename string) error {
		// We assume that the filename is the same as the table name. Example:
		// if a trigger functions is tied to the table "applications", the
		// filename containing the trigger function must be called
		// "applications.sql".
		table := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filepath.Base(filename)))
		functions := getFunctions(filename)
		for _, function := range functions {
			text := fmt.Sprintf("SELECT * FROM plpgsql_check_function('%s', '%s');", function, table)
			errcount, err := getErrcount(db, function, table)
			if err != nil {
				// Error may be due to the user not naming the file the same
				// name as the table.
				var pqerr *pq.Error
				if errors.As(err, &pqerr) && pqerr.Code == "42P01" { // 42P01 is postgres error code for undefined_table
					fmt.Println(text + " ✗")
					fmt.Printf(
						`
Error: table '%[1]s' does not exist.
The table name was derived from the filename containing the
function %[2]s(), which is '%[1]s.sql'.
Did you misname the file? The name of the file must be the same as the table.
`, table, function)
					fmt.Println()
				}
				return fmt.Errorf("Error(s) in function %s: %w", function, err)
			}
			if errcount > 0 {
				fmt.Println(text + " ✗")
				displayErrors(db, function, table)
				return fmt.Errorf("Error(s) in function %s", function)
			}
			fmt.Println(text + " ✓")
		}
		return nil
	}
}

func plpgsqlCheckFunction(db *sql.DB) func(string) error {
	return func(filename string) error {
		functions := getFunctions(filename)
		for _, function := range functions {
			text := fmt.Sprintf("SELECT * FROM plpgsql_check_function('%s');", function)
			errcount, err := getErrcount(db, function, "")
			if err != nil {
			}
			if errcount > 0 {
				fmt.Println(text + " ✗")
				displayErrors(db, function, "")
				return fmt.Errorf("Error(s) in function %s", function)
			}
			fmt.Println(text + " ✓")
		}
		return nil
	}
}

// getFunctions extracts all function names from a filename using regex.
func getFunctions(filename string) (functions []string) {
	body, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	matches := regexp.MustCompile(`(?i)`+ // case insensitive
		`\n?`+ // new line representing start of a line (optional)
		`.*`+ // zero or more characters, intended to include any comment characters '--'
		`CREATE\s+`+ // CREATE
		`(?:OR\s+REPLACE\s+)?`+ // OR REPLACE (optional, non capturing)
		`FUNCTION\s+`+ // FUNCTION
		`([\w.]+)`, // capture the function name into group 1
	).FindAllSubmatch(body, -1)
	for _, match := range matches {
		if bytes.HasPrefix(bytes.TrimSpace(match[0]), []byte("--")) {
			continue // If line starts with a '--' i.e. it is commented out, we want to ignore it
		}
		functions = append(functions, string(match[1]))
	}
	return functions
}

func getErrcount(db *sql.DB, function, table string) (errcount int, err error) {
	var rows *sql.Rows
	if table == "" {
		rows, err = db.Query("SELECT COUNT(*) FROM plpgsql_check_function_tb($1)", function)
	} else {
		rows, err = db.Query("SELECT COUNT(*) FROM plpgsql_check_function_tb($1, $2)", function, table)
	}
	if err != nil {
		return errcount, err
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&errcount)
		if err != nil {
			panic(err)
		}
		if errcount > 0 {
			return errcount, nil
		}
	}
	return 0, nil
}

func displayErrors(db *sql.DB, function, table string) {
	var rows *sql.Rows
	var err error
	if table == "" {
		rows, err = db.Query("SELECT * FROM plpgsql_check_function($1)", function)
	} else {
		rows, err = db.Query("SELECT * FROM plpgsql_check_function($1, $2)", function, table)
	}
	if err != nil {
		panic(err)
	}
	printRows(rows)
}

func printRows(rows *sql.Rows) []string {
	var err error
	var strs []string
	defer rows.Close()
	for rows.Next() {
		var str sql.NullString
		err = rows.Scan(&str)
		if err != nil {
			panic(err)
		}
		strs = append(strs, str.String)
		fmt.Println(str.String)
	}
	return strs
}

func migrationsDrop(migrationDir string) error {
	fmt.Println("DROPPING ALL TABLES")
	m, err := migrate.New("file://"+migrationDir, databaseURL)
	if err != nil {
		return err
	}
	defer m.Close()
	if err = m.Drop(); err != nil {
		return err
	}
	fmt.Println("DROP SUCCESSFUL")
	return nil
}

func migrationsUp(migrationDir string) error {
	fmt.Println("APPLYING MIGRATIONS")
	m, err := migrate.New("file://"+migrationDir, databaseURL)
	if err != nil {
		return err
	}
	defer m.Close()
	if err = m.Up(); err != nil {
		return err
	}
	fmt.Println("MIGRATIONS SUCCESSFUL")
	return nil
}

// Load a bunch of filenames
func loadFiles(filenames []string, postprocessors ...func(string) error) error {
	var err error
	cmdName := "psql"
	cmdArgs := []string{databaseURL, "--variable=ON_ERROR_STOP=1"}
	if !cmdExists("psql") || *dockerFlag {
		cmdName = "docker"
		cmdArgs = []string{"exec", dockerContainerName, "psql", "--dbname=" + databaseName, "--username=" + databaseUser, "--variable=ON_ERROR_STOP=1"}
	}
	// Append the filenames to the command
	var i int
	for _, filename := range filenames {
		i++
		fmt.Printf("%02d. %s\n", i, filename)
		switch cmdName {
		case "psql":
			cmdArgs = append(cmdArgs, "--file="+rootdir+filename)
		case "docker":
			cmdArgs = append(cmdArgs, "--file="+dockerRootdir+filename)
		default:
			panic(fmt.Errorf("Unrecognized command: %s", cmdName))
		}
	}
	// Run the command
	cmd := exec.Command(cmdName, cmdArgs...)
	fmt.Println(cmd.String())
	err = runCmd(cmd)
	if err != nil {
		return err
	}
	// Call each postprocessor function on each file
	for _, filename := range filenames {
		for _, fn := range postprocessors {
			if fn != nil {
				err = fn(filename)
				if err != nil {
					return fmt.Errorf("Error when postprocessing file %s: %w", filename, err)
				}
			}
		}
	}
	return nil
}

func runTests(filenames []string) error {
	testdb, err := sql.Open("txdb", databaseURL)
	if err != nil {
		panic(err)
	}
	defer testdb.Close()
	start := func() {
		fmt.Println()
		fmt.Println(`SELECT no_plan();`)
		_, err = testdb.Exec(`SELECT no_plan();`)
		if err != nil {
			panic(err)
		}
	}
	finish := func() {
		fmt.Println()
		fmt.Println(`SELECT finish();`)
		rows, err := testdb.Query(`SELECT finish();`)
		if err != nil {
			panic(err)
		}
		printRows(rows)
	}
	start()
	var sometestfailed bool
	for _, filename := range filenames {
		fmt.Println()
		fmt.Println("┌" + strings.Repeat("─", len(filename)+10) + "┐")
		fmt.Println("│ ↓↓↓ " + filename + " ↓↓↓ │")
		fmt.Println("└" + strings.Repeat("─", len(filename)+10) + "┘")
		var testfunctions []string
		functions := getFunctions(filename)
		for _, function := range functions {
			// Only test functions that start with 't.test', the rest are
			// considered helper functions.
			if strings.HasPrefix(function, "t.test") {
				testfunctions = append(testfunctions, function)
			}
		}
		for _, function := range testfunctions {
			// Here is where the function is actually tested
			testpassed, err := testFunction(testdb, function)
			if err != nil {
				fmt.Printf("%s() threw an unexpected error: %s\n\n", function, err)
				return fmt.Errorf("Error when running test file: %s", filename)
			}
			finish() // Print the status report at the end of each test function
			if !testpassed {
				if *testallFlag {
					sometestfailed = true
					continue // If user wants to run all tests, continue
				}
				return fmt.Errorf("Error when running test file: %s", filename)
			}
		}
	}
	if sometestfailed {
		return fmt.Errorf("Some test(s) failed")
	}
	return nil
}

func testFunction(db *sql.DB, function string) (testpassed bool, err error) {
	query := fmt.Sprintf(`SELECT %s();`, function)
	fmt.Println()
	fmt.Println(query)
	fmt.Println(strings.Repeat("-", len(query)))
	rows, err := db.Query(query)
	if err != nil {
		return testpassed, err
	}
	strs := printRows(rows)
	// Check if any of the tests failed in this function.
	for _, str := range strs {
		// Reference: https://testanything.org/tap-specification.html
		// According to the the Test Anything Protocol (TAP), any line
		// that starts with 'not ok' indicates a failure.
		if strings.HasPrefix(str, "not ok") {
			// But lines containing '# SKIP' or '# TODO' should have
			// their failures ignored. Tests can be marked as todo
			// using the todo_start() and todo_end() functions.
			// https://pgtap.org/documentation.html#conditionaltests
			isSkip, _ := regexp.MatchString(`# SKIP\S*\s+`, str)
			isTodo, _ := regexp.MatchString(`# TODO\S*\s+`, str)
			if isSkip || isTodo {
				continue
			}
			return false, nil
		}
	}
	return true, nil
}

// runCmd will run an *exec.Cmd and return the error.
//
// The reason why runCmd(cmd) is used over cmd.Run() is because runCmd streams
// the output of the command to stdout as the command is running. If cmd.Run is
// used instead, you would see the program 'freeze' for a few seconds before
// dumping a load of text once it finishes running.
//
// I copied the code from
// https://blog.kowalczyk.info/article/wOYk/advanced-command-execution-in-go-with-osexec.html.
// It's not pretty, but it works.
func runCmd(cmd *exec.Cmd) error {
	var stdoutBuf, stderrBuf bytes.Buffer
	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()
	stdout := io.MultiWriter(os.Stdout, &stdoutBuf)
	stderr := io.MultiWriter(os.Stderr, &stderrBuf)
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("Error in cmd.Start(): %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var errStdout, errStderr error
	go func() {
		_, errStdout = io.Copy(stdout, stdoutIn)
		wg.Done()
	}()
	_, errStderr = io.Copy(stderr, stderrIn)
	wg.Wait()
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("Command encountered an error")
	}
	if errStdout != nil || errStderr != nil {
		return fmt.Errorf("Failed to capture stdout or stderr")
	}
	return nil
}

func resetSchema(db *sql.DB, schema string) {
	_, err := db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema))
	if err != nil {
		panic(err)
	}
}
