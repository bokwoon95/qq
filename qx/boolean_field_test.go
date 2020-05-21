package qx

import (
	"fmt"
	"testing"

	"github.com/matryer/is"
)

func TestBooleanField_ToSQL(t *testing.T) {
	type TT struct {
		DESCRIPTION            string
		f                      BooleanField
		excludeTableQualifiers []string
		wantQuery              string
		wantArgs               []interface{}
	}
	tests := []TT{
		func() TT {
			DESCRIPTION := "typical boolean column"
			tbl := NewTableInfo("public", "users")
			f := NewBooleanField("is_user", tbl)
			excludeTableQualifiers := []string{}
			wantQuery := "users.is_user"
			return TT{DESCRIPTION, f, excludeTableQualifiers, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "literal bool value"
			f := Bool(true)
			excludeTableQualifiers := []string{}
			wantQuery := "?"
			wantArgs := []interface{}{true}
			return TT{DESCRIPTION, f, excludeTableQualifiers, wantQuery, wantArgs}
		}(),
		func() TT {
			DESCRIPTION := "aliasing a BooleanField works"
			tbl := NewTableInfo("public", "users")
			f := NewBooleanField("is_user", tbl).As("abcd")
			if f.GetAlias() != "abcd" {
				t.Errorf("expected field to get set with abcd alias")
			}
			excludeTableQualifiers := []string{}
			wantQuery := "users.is_user"
			return TT{DESCRIPTION, f, excludeTableQualifiers, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "BooleanField respects excludeTableQualifiers (name)"
			tbl := NewTableInfo("public", "users")
			f := NewBooleanField("is_user", tbl)
			excludeTableQualifiers := []string{"users"}
			wantQuery := "is_user"
			return TT{DESCRIPTION, f, excludeTableQualifiers, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "BooleanField respects excludeTableQualifiers (alias)"
			tbl := NewTableInfo("public", "users")
			tbl.Alias = "abcd"
			f := NewBooleanField("is_user", tbl)
			excludeTableQualifiers := []string{"abcd"}
			wantQuery := "is_user"
			return TT{DESCRIPTION, f, excludeTableQualifiers, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "ASC NULLS LAST"
			tbl := NewTableInfo("public", "users")
			f := NewBooleanField("is_user", tbl).Asc().NullsLast()
			wantQuery := "users.is_user ASC NULLS LAST"
			return TT{DESCRIPTION, f, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "DESC NULLS FIRST"
			tbl := NewTableInfo("public", "users")
			tbl.Alias = "abcd"
			f := NewBooleanField("is_user", tbl).Desc().NullsFirst()
			wantQuery := "abcd.is_user DESC NULLS FIRST"
			return TT{DESCRIPTION, f, nil, wantQuery, nil}
		}(),
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			gotQuery, gotArgs := tt.f.ToSQL(tt.excludeTableQualifiers)
			is.Equal(tt.wantQuery, gotQuery)
			is.Equal(tt.wantArgs, gotArgs)
		})
	}
}

func TestBooleanField_GameTheNumbers(t *testing.T) {
	is := is.New(t)
	var query string
	var args []interface{}
	tbl := NewTableInfo("public", "users")
	f := NewBooleanField("is_user", tbl)
	is.Equal("", f.GetAlias())
	is.Equal("is_user", f.GetName())
	f.AssertPredicate()
	var _ Field = f
	// Set
	f.Set(false)
	f.SetBool(true)
	// Not
	query, args = f.Not().ToSQL(nil)
	is.Equal(query, "NOT users.is_user")
	is.Equal(args, nil)
	// Predicates
	f.IsNull()
	f.IsNotNull()
	f.Eq(f)
	f.Ne(f)
	// fmt.Stringer
	fmt.Println(f)
}
