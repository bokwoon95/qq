package qy

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/DATA-DOG/go-txdb"
	"github.com/bokwoon95/qx-postgres/qx"
	"github.com/bokwoon95/qx-postgres/tables"
	"github.com/joho/godotenv"
	"github.com/matryer/is"
)

var (
	// sourcefile is the path to this file
	_, sourcefile, _, _ = runtime.Caller(0)
	// testdir is the path to the testdata directory
	testdir = filepath.Join(filepath.Dir(sourcefile), "testdata") + string(os.PathSeparator)
)

func init() {
	if err := godotenv.Load(testdir + ".env"); err != nil {
		panic(fmt.Sprintf("unable to source from either .env"))
	}
	txdb.Register("txdb", "postgres", os.Getenv("DATABASE_URL"))
}

func TestSelectQuery_Execf(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	is := is.New(t)
	db, err := sql.Open("txdb", qx.RandomString(8))
	is.NoErr(err)
	defer db.Close()
	type User struct {
		Valid       bool
		Uid         int
		Displayname string
		Email       string
	}
	var user User
	var users []User
	u := tables.USERS().As("u")
	ur := tables.USER_ROLES().As("ur")
	q := NewSelectQuery()

	// Basic Selectx
	err = q.From(u).Join(ur, ur.UID.Eq(u.UID)).OrderBy(u.UID).Limit(5).Selectx(func(row Row) {
		user = User{
			Valid:       row.IntValid(u.UID),
			Uid:         row.Int(u.UID),
			Displayname: row.String(u.DISPLAYNAME),
			Email:       row.String(u.EMAIL),
		}
	}, func() {
		users = append(users, user)
	}).Exec(db)
	is.NoErr(err)
	is.Equal(5, len(users))

	// Basic SelectRowx
	err = q.From(u).OrderBy(u.UID).Limit(5).SelectRowx(func(row Row) {
		user = User{
			Valid:       row.IntValid(u.UID),
			Uid:         row.Int(u.UID),
			Displayname: row.String_(u.UID),
			Email:       row.String_(u.UID),
		}
	}).Exec(db)
	is.NoErr(err)

	// ScanInto variant
	var uid sql.NullInt64
	var displayname, email sql.NullString
	err = q.From(u).OrderBy(u.UID).Limit(5).SelectRowx(func(row Row) {
		row.ScanInto(&uid, u.UID)
		row.ScanInto(&displayname, u.DISPLAYNAME)
		row.ScanInto(&email, u.EMAIL)
		user = User{
			Valid:       uid.Valid,
			Uid:         int(uid.Int64),
			Displayname: displayname.String,
			Email:       email.String,
		}
	}).Exec(db)
	is.NoErr(err)

	// If user doesn't do anything in the mapper func, it is a no-op (no errors should occur)
	err = q.From(u).SelectRowx(func(row Row) {}).Exec(db)
	is.NoErr(err)

	// Exec without mapper function throws an error
	err = q.From(u).Exec(db)
	is.True(err != nil)

	// Named mapper/accumulator functions
	unmarshalUser := func(user *User, u tables.TABLE_USERS) func(Row) {
		return func(row Row) {
			user = &User{
				Valid:       row.IntValid(u.UID),
				Uid:         row.Int(u.UID),
				Displayname: row.String(u.DISPLAYNAME),
				Email:       row.String(u.EMAIL),
			}
		}
	}
	aggregateUsers := func(user *User, users *[]User) func() {
		return func() {
			*users = append(*users, *user)
		}
	}
	user, users = User{}, nil
	err = q.From(u).OrderBy(u.UID).Limit(5).Selectx(
		unmarshalUser(&user, u),
		aggregateUsers(&user, &users),
	).Exec(db)
	is.NoErr(err)
	is.Equal(5, len(users))
}

func TestRow_Array(t *testing.T) {
	is := is.New(t)
	db, err := sql.Open("txdb", qx.RandomString(8))
	is.NoErr(err)
	defer db.Close()
	q := NewSelectQuery()
	q.Log = log.New(os.Stdout, "", 0)

	var strs []string
	err = q.From(Tablef("generate_series(1,10) n")).SelectRowx(func(row Row) {
		row.ScanArray(&strs, Fieldf("array_agg(n::INT)"))
	}).Exec(db)
	is.NoErr(err)
	is.Equal([]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, strs)
	err = q.SelectRowx(func(row Row) {
		row.ScanArray(&strs, Fieldf(`'{"a","b","c"}'`))
	}).Exec(db)
	is.NoErr(err)
	is.Equal([]string{"a", "b", "c"}, strs)

	type User struct {
		Valid       bool
		Uid         int64
		Displayname string
		Email       string
		Roles       map[string]int64
	}
	var user = User{Roles: make(map[string]int64)}
	var users []User
	var roles []string
	var urids []int64
	u, ur := tables.USERS().As("u"), tables.USER_ROLES().As("ur")
	err = q.From(u).
		Join(ur, ur.UID.Eq(u.UID)).
		GroupBy(u.UID, u.DISPLAYNAME, u.EMAIL).
		Limit(5).
		Selectx(func(row Row) {
			user.Valid = row.IntValid(u.UID)
			user.Uid = row.Int64(u.UID)
			user.Displayname = row.String(u.DISPLAYNAME)
			user.Email = row.String(u.EMAIL)
			row.ScanArray(&roles, Fieldf("array_agg(?)", ur.ROLE))
			row.ScanArray(&urids, Fieldf("array_agg(?)", ur.URID))
			for i, role := range roles {
				user.Roles[role] = urids[i]
			}
		}, func() {
			users = append(users, user)
		}).Exec(db)
	is.NoErr(err)
	// fmt.Println(users)

	unmarshalUser := func(user *User, u tables.TABLE_USERS, ur tables.TABLE_USER_ROLES) func(Row) {
		var roles []string
		var urids []int64
		return func(row Row) {
			user.Valid = row.IntValid(u.UID)
			user.Uid = row.Int64(u.UID)
			user.Displayname = row.String(u.DISPLAYNAME)
			user.Email = row.String(u.EMAIL)
			row.ScanArray(&roles, Fieldf("array_agg(?)", ur.ROLE))
			row.ScanArray(&urids, Fieldf("array_agg(?)", ur.URID))
			for i, role := range roles {
				user.Roles[role] = urids[i]
			}
		}
	}
	users = nil
	err = q.From(u).
		Join(ur, ur.UID.Eq(u.UID)).
		GroupBy(u.UID, u.DISPLAYNAME, u.EMAIL).
		Limit(5).
		Selectx(unmarshalUser(&user, u, ur), func() {
			users = append(users, user)
		}).Exec(db)
	is.NoErr(err)
	fmt.Println(users)
}
