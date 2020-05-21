package qy

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"testing"

	"github.com/bokwoon95/qx-postgres/qx"
	"github.com/bokwoon95/qx-postgres/tables"
	"github.com/matryer/is"
)

type TestUser struct {
	Valid    bool
	Uid      int64
	Name     string
	Email    string
	Password sql.NullString
}

func TestInsertTemp1(t *testing.T) {
	wantQuery, wantArgs := "", []interface{}{}

	applicants := qx.NewCTE("applicants", func() qx.Query {
		u, ur, ura := tables.USERS().As("u"), tables.USER_ROLES().As("ur"), tables.USER_ROLES_APPLICANTS().As("ura")
		return Select(u.UID, ur.URID, u.DISPLAYNAME, u.EMAIL, ura.APPLICATION, ura.DATA).
			From(u).Join(ur, ur.UID.Eq(u.UID)).
			LeftJoin(ura, ura.URID.Eq(ur.URID)).
			Where(ur.ROLE.EqString("applicant"))
	}())
	wantQuery += "WITH applicants AS" +
		" (SELECT u.uid, ur.urid, u.displayname, u.email, ura.application, ura.data" +
		" FROM public.users AS u JOIN public.user_roles AS ur ON ur.uid = u.uid" +
		" LEFT JOIN public.user_roles_applicants AS ura ON ura.urid = ur.urid" +
		" WHERE ur.role = $1)"
	wantArgs = append(wantArgs, "applicant")

	students := qx.NewCTE("students", func() qx.Query {
		u, ur, urs := tables.USERS().As("u"), tables.USER_ROLES().As("ur"), tables.USER_ROLES_STUDENTS().As("urs")
		return Select(u.UID, ur.URID, u.DISPLAYNAME, u.EMAIL, urs.TEAM, urs.DATA).
			From(u).Join(ur, ur.UID.Eq(u.UID)).
			LeftJoin(urs, urs.URID.Eq(ur.URID)).
			Where(ur.ROLE.EqString("student"))
	}())
	wantQuery += ", students AS" +
		" (SELECT u.uid, ur.urid, u.displayname, u.email, urs.team, urs.data" +
		" FROM public.users AS u JOIN public.user_roles AS ur ON ur.uid = u.uid" +
		" LEFT JOIN public.user_roles_students AS urs ON urs.urid = ur.urid" +
		" WHERE ur.role = $2)"
	wantArgs = append(wantArgs, "student")

	advisers := qx.NewCTE("advisers", func() qx.Query {
		u, ur := tables.USERS().As("u"), tables.USER_ROLES().As("ur")
		return Select(u.UID, ur.URID, u.DISPLAYNAME, u.EMAIL).
			From(u).Join(ur, ur.UID.Eq(u.UID)).
			Where(ur.ROLE.EqString("adviser"))
	}())
	wantQuery += ", advisers AS" +
		" (SELECT u.uid, ur.urid, u.displayname, u.email" +
		" FROM public.users AS u JOIN public.user_roles AS ur ON ur.uid = u.uid" +
		" WHERE ur.role = $3)"
	wantArgs = append(wantArgs, "adviser")

	// stu1, stu2 := students.As("stu1"), students.As("stu2")
	q := NewInsertQuery()
	q.Log = log.New(os.Stdout, "", 0)
	u := tables.USERS().As("u")
	q = q.With(qx.CTE{}, applicants, students, advisers).
		InsertInto(u).
		InsertRow(
			u.UID.SetInt(1),
			u.DISPLAYNAME.SetString("aaa"),
			u.EMAIL.SetString("aaa@email.com"),
		).
		InsertRow(
			u.UID.SetInt(2),
			u.DISPLAYNAME.SetString("bbb"),
			u.EMAIL.SetString("bbb@email.com"),
		).
		InsertRow(
			u.UID.SetInt(3),
			u.DISPLAYNAME.SetString("ccc"),
			u.EMAIL.SetString("ccc@email.com"),
		).
		OnConflict(u.EMAIL).
		Where(u.EMAIL.NeString("")).
		DoUpdateSet(
			u.DISPLAYNAME.Set(Excluded(u.DISPLAYNAME)),
		).
		Where(
			u.UID.GtInt(0),
			u.DISPLAYNAME.NeString(""),
		)
	wantQuery += " INSERT INTO public.users AS u (uid, displayname, email)" +
		" VALUES ($4, $5, $6), ($7, $8, $9), ($10, $11, $12)" +
		" ON CONFLICT (email) WHERE email <> $13 DO UPDATE SET" +
		" displayname = EXCLUDED.displayname WHERE u.uid > $14 AND u.displayname <> $15"
	wantArgs = append(wantArgs, 1, "aaa", "aaa@email.com", 2, "bbb", "bbb@email.com",
		3, "ccc", "ccc@email.com", "", 0, "")

	is := is.New(t)
	gotQuery, gotArgs := q.ToSQL()
	is.Equal(wantQuery, gotQuery)
	is.Equal(wantArgs, gotArgs)
}

func TestInsertTemp2(t *testing.T) {
	wantQuery, wantArgs := "", []interface{}{}

	q := NewInsertQuery()
	q.Log = log.New(os.Stdout, "", 0)
	u := tables.USERS().As("u")
	q = q.InsertInto(u).
		InsertRow(
			u.UID.SetInt(1),
			u.DISPLAYNAME.SetString("aaa"),
			u.EMAIL.SetString("aaa@email.com"),
		).
		InsertRow(
			u.UID.SetInt(2),
			u.DISPLAYNAME.SetString("bbb"),
			u.EMAIL.SetString("bbb@email.com"),
		).
		InsertRow(
			u.UID.SetInt(3),
			u.DISPLAYNAME.SetString("ccc"),
			u.EMAIL.SetString("ccc@email.com"),
		).
		OnConflictOnConstraint("users_email_key").
		Where(u.EMAIL.NeString("")).
		DoNothing().
		Where(
			u.UID.GtInt(0),
			u.DISPLAYNAME.NeString(""),
		).
		Returning(u.UID, u.DISPLAYNAME, u.EMAIL)
	wantQuery += "INSERT INTO public.users AS u (uid, displayname, email)" +
		" VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)" +
		" ON CONFLICT ON CONSTRAINT users_email_key DO NOTHING" +
		" RETURNING u.uid, u.displayname, u.email"
	wantArgs = append(wantArgs, 1, "aaa", "aaa@email.com", 2, "bbb", "bbb@email.com",
		3, "ccc", "ccc@email.com")

	is := is.New(t)
	gotQuery, gotArgs := q.ToSQL()
	is.Equal(wantQuery, gotQuery)
	is.Equal(wantArgs, gotArgs)
}

func TestInsertReal(t *testing.T) {
	is := is.New(t)
	db, err := sql.Open("txdb", qx.RandomString(8))
	is.NoErr(err)
	defer db.Close()
	q := NewInsertQuery()
	q.Log = log.New(os.Stdout, "[BIG BREIN] ", 0)

	u := tables.USERS()
	names := []string{"aaa", "bbb", "ccc"}
	emails := []string{"aaa@email.com", "bbb@email.com", "ccc@email.com"}
	insert := q.InsertInto(u).
		OnConflict(u.EMAIL).
		Where(Predicatef("(? + ? - ?) <> ?", 5, 99999, u.UID, 0)).
		DoUpdateSet(u.DISPLAYNAME.Set(Excluded(u.DISPLAYNAME))).
		Where(u.EMAIL.NeString(""))
	for i := range names {
		if i == 0 {
			insert = insert.Values(names[i], emails[i])
			continue
		}
		insert = insert.InsertRow(
			u.DISPLAYNAME.SetString(names[i]),
			u.EMAIL.SetString(emails[i]),
		)
	}
	var user TestUser
	var users []TestUser
	err = insert.Returningx(func(row Row) {
		user.Valid = row.IntValid(u.UID)
		user.Uid = row.Int64(u.UID)
		user.Name = row.String(u.DISPLAYNAME)
		user.Email = row.String(u.EMAIL)
	}, func() {
		users = append(users, user)
	}).Exec(db)
	is.NoErr(err)
	fmt.Println(users)
	is.Equal(len(names), len(users))
	for i := range users {
		is.True(users[i].Valid)
		is.True(users[i].Uid != 0)
		is.Equal(names[i], users[i].Name)
		is.Equal(emails[i], users[i].Email)
	}

	selectCount := func(db qx.Queryer, q InsertQuery) (count int, err error) {
		q.ReturningFields = []qx.Field{Fieldf("1")}
		stmt := qx.NewCTE("stmt", q)
		q2 := NewSelectQuery()
		q2.Log = q.Log
		err = q2.With(stmt).From(stmt).SelectRowx(func(row Row) {
			count = row.Int_(Fieldf("COUNT(*)"))
		}).Exec(db)
		return count, err
	}
	count, err := selectCount(db,
		q.InsertInto(u).
			Columns(u.UID, u.DISPLAYNAME, u.EMAIL).
			Values(users[0].Uid, users[0].Name, users[0].Email).
			Values(users[1].Uid, users[1].Name, users[1].Email).
			Values(users[2].Uid, users[2].Name, users[2].Email).
			OnConflict().DoNothing(),
	)
	is.Equal(0, count)
	is.NoErr(err)
	var randStrs []string
	for i := 0; i < 10; i++ {
		randStrs = append(randStrs, qx.RandomString(8))
	}
	q = q.InsertInto(u).Columns(u.UID, u.DISPLAYNAME, u.EMAIL).OnConflict().DoNothing()
	for i := range randStrs {
		q = q.Values(math.MaxInt32-i, randStrs[i], randStrs[i])
	}
	count, err = selectCount(db, q)
	is.NoErr(err)
	is.Equal(len(randStrs), count)
}

func TestInsertNull(t *testing.T) {
	is := is.New(t)
	db, err := sql.Open("txdb", qx.RandomString(8))
	is.NoErr(err)
	defer db.Close()
	const DEFAULT qx.FieldLiteral = "DEFAULT"
	q := NewInsertQuery()
	q.Log = log.New(os.Stdout, "", 0)

	u := tables.USERS().As("u")
	names := []string{"aaa", "bbb", "ccc"}
	emails := []string{"aaa@email.com", "bbb@email.com", "ccc@email.com"}
	insert := q.InsertInto(u).
		OnConflict(u.EMAIL).
		Where(Predicatef("(? + ? - ?) <> ?", 5, 99999, u.UID, 0)).
		Where(Predicatef("? = ?", u.DISPLAYNAME, nil)).
		DoUpdateSet(u.DISPLAYNAME.Set(Excluded(u.DISPLAYNAME))).
		Where(u.EMAIL.NeString(""))
	for i := range names {
		if i == 0 {
			insert = insert.Values(names[i], emails[i], "yohoho")
			continue
		}
		if i == 1 {
			insert = insert.Values(names[i], emails[i], DEFAULT)
			continue
		}
		insert = insert.InsertRow(
			u.DISPLAYNAME.SetString(names[i]),
			u.EMAIL.SetString(emails[i]),
			u.PASSWORD.Set(nil),
		)
	}
	var user TestUser
	var users []TestUser
	err = insert.Returningx(func(row Row) {
		uid := row.NullInt64(u.UID)
		user.Valid = uid.Valid
		user.Uid = uid.Int64
		user.Name = row.String(u.DISPLAYNAME)
		user.Email = row.String(u.EMAIL)
		user.Password = row.NullString(u.PASSWORD)
	}, func() {
		users = append(users, user)
	}).Exec(db)
	fmt.Println(users)
}
