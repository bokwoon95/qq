package qx

import (
	"database/sql"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestReplacePlaceholders(t *testing.T) {
	tests := []struct {
		DESCRIPTION string
		input       string
		want        string
	}{
		{
			"basic with escape",
			"SELECT ?, ?, ? -- escape this ??",
			"SELECT $1, $2, $3 -- escape this ?",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			got := MySQLToPostgresPlaceholders(tt.input)
			is.Equal(tt.want, got)
		})
	}
}

func TestInterpolateSQL(t *testing.T) {
	type TT struct {
		DESCRIPTION string
		query       string
		args        []interface{}
		want        string
	}
	tests := []TT{
		{
			"nil",
			"SELECT $1",
			[]interface{}{nil},
			"SELECT NULL",
		},
		{
			"bool",
			"SELECT $1 AND $2",
			[]interface{}{true, false},
			"SELECT TRUE AND FALSE",
		},
		func() TT {
			DESCRIPTION := "time.Time"
			now := time.Now()
			nowstring := now.Format(time.RFC3339Nano)
			query := "SELECT $1"
			args := []interface{}{now}
			want := "SELECT '" + nowstring + "'"
			return TT{DESCRIPTION, query, args, want}
		}(),
		func() TT {
			DESCRIPTION := "driver.Valuer: non-null string"
			val := sql.NullString{Valid: true, String: "lorem ipsum"}
			query := "SELECT $1"
			args := []interface{}{val}
			want := "SELECT 'lorem ipsum'"
			return TT{DESCRIPTION, query, args, want}
		}(),
		func() TT {
			DESCRIPTION := "driver.Valuer: null string"
			val := sql.NullString{Valid: false, String: "lorem ipsum"}
			query := "SELECT $1"
			args := []interface{}{val}
			want := "SELECT NULL"
			return TT{DESCRIPTION, query, args, want}
		}(),
		func() TT {
			DESCRIPTION := "JSONable"
			val := TestUser{Valid: true, Uid: 11, Name: "Bob", Email: "bob@email.com"}
			query := "SELECT $1"
			args := []interface{}{val}
			want := `SELECT '{"Valid":true,"Uid":11,"Name":"Bob","Email":"bob@email.com","Password":{"String":"","Valid":false}}'`
			return TT{DESCRIPTION, query, args, want}
		}(),
		{
			"two digit placeholders",
			"WITH stmt AS (" +
				"INSERT INTO public.users AS users_KNFDq (displayname, email)" +
				" VALUES ($1, $2), ($3, $4), ($5, TRIM($6))" +
				" ON CONFLICT (email) WHERE ($7 - (($8 + uid) + $9)) < $10" +
				" DO UPDATE SET displayname = EXCLUDED.displayname, uid = EXCLUDED.uid, email = $11" +
				" WHERE users_KNFDq.uid = $12 RETURNING 1" +
				") SELECT COUNT(*) FROM stmt",
			[]interface{}{"aaa", "aaa@email.com", "bbb", "bbb@email.com", "ccc",
				"ccc@email.com", 10, 22, 99, 10, "big boy pants", 0},
			"WITH stmt AS (" +
				"INSERT INTO public.users AS users_KNFDq (displayname, email)" +
				" VALUES ('aaa', 'aaa@email.com'), ('bbb', 'bbb@email.com'), ('ccc', TRIM('ccc@email.com'))" +
				" ON CONFLICT (email) WHERE (10 - ((22 + uid) + 99)) < 10" +
				" DO UPDATE SET displayname = EXCLUDED.displayname, uid = EXCLUDED.uid, email = 'big boy pants'" +
				" WHERE users_KNFDq.uid = 0 RETURNING 1" +
				") SELECT COUNT(*) FROM stmt",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			is := is.New(t)
			got := PostgresInterpolateSQL(tt.query, tt.args...)
			is.Equal(tt.want, got)
		})
	}
}
