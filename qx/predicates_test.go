package qx

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestCustomPredicate_ToSQL(t *testing.T) {
	type TT struct {
		DESCRIPTION string
		p           CustomPredicate
		wantQuery   string
		wantArgs    []interface{}
	}
	tests := []TT{
		func() TT {
			DESCRIPTION := "basic"
			u := USERS().As("u")
			p := CustomPredicate{
				Format: "? IS NULL AND ? > 2 AND ? AND ? AND ? AND ?",
				Values: []interface{}{u.UID, Int(2), u.UID.Eq(u.UID), u, 22, nil},
			}
			wantQuery := "u.uid IS NULL AND ? > 2 AND u.uid = u.uid AND public.users AND ? AND NULL"
			wantArgs := []interface{}{2, 22}
			return TT{DESCRIPTION, p, wantQuery, wantArgs}
		}(),
		func() TT {
			DESCRIPTION := "literal"
			p := CustomPredicate{
				Format: "lorem ipsum",
			}
			wantQuery := "lorem ipsum"
			return TT{DESCRIPTION, p, wantQuery, nil}
		}(),
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			gotQuery, gotArgs := tt.p.ToSQL(nil)
			is.Equal(tt.wantQuery, gotQuery)
			is.Equal(tt.wantArgs, gotArgs)
		})
	}
}

func TestVariadicPredicates_ToSQL(t *testing.T) {
	type TT struct {
		DESCRIPTION            string
		p                      VariadicPredicate
		excludeTableQualifiers []string
		wantQuery              string
		wantArgs               []interface{}
	}
	tests := []TT{
		func() TT {
			DESCRIPTION := "empty"
			p := VariadicPredicate{}
			return TT{DESCRIPTION, p, nil, "", nil}
		}(),
		func() TT {
			DESCRIPTION := "all nil Predicates"
			p := VariadicPredicate{
				Predicates: []Predicate{nil, nil, nil},
			}
			return TT{DESCRIPTION, p, nil, "", nil}
		}(),
		func() TT {
			DESCRIPTION := "single predicate"
			p := VariadicPredicate{
				Predicates: []Predicate{
					CustomPredicate{
						Format: "lorem ipsum dolor sit amet ?",
						Values: []interface{}{5},
					},
				},
			}
			wantQuery := "lorem ipsum dolor sit amet ?"
			return TT{DESCRIPTION, p, nil, wantQuery, []interface{}{5}}
		}(),
		func() TT {
			DESCRIPTION := "double predicates (without Toplevel)"
			p := VariadicPredicate{
				Toplevel: false, // means enclosing brackets
				Predicates: []Predicate{
					CustomPredicate{Format: "cats"},
					CustomPredicate{Format: "dogs"},
				},
			}
			wantQuery := "(cats AND dogs)"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "basic non-nested"
			p := VariadicPredicate{
				Toplevel: true,
				Operator: PredicateOr,
				Predicates: []Predicate{
					CustomPredicate{Format: "EXPLOSION"},
					CustomPredicate{Format: "EXPLOSION"},
					CustomPredicate{Format: "EXPLOSION"},
				},
			}
			wantQuery := "EXPLOSION OR EXPLOSION OR EXPLOSION"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "nested"
			p := VariadicPredicate{
				Toplevel: true,
				Predicates: []Predicate{
					VariadicPredicate{
						Operator: PredicateOr,
						Predicates: []Predicate{
							CustomPredicate{Format: "cats"},
							CustomPredicate{Format: "dogs"},
						},
					},
					VariadicPredicate{
						Operator: PredicateOr,
						Predicates: []Predicate{
							CustomPredicate{Format: "apples"},
							CustomPredicate{Format: "bananas"},
						},
					},
					CustomPredicate{Format: "five"},
					CustomPredicate{Format: "six"},
				},
			}
			wantQuery := "(cats OR dogs) AND (apples OR bananas) AND five AND six"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "AND and OR"
			p := And(
				Or(
					CustomPredicate{Format: "cats"},
					CustomPredicate{Format: "dogs"},
				),
				Or(
					CustomPredicate{Format: "apples"},
					CustomPredicate{Format: "bananas"},
				),
				CustomPredicate{Format: "five"},
				CustomPredicate{Format: "six"},
			)
			// Toplevel is by default false which is why the entire expression
			// is couched in a set of brackets.
			wantQuery := "((cats OR dogs) AND (apples OR bananas) AND five AND six)"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "respect excludeTableQualifiers"
			u, ur := USERS().As("u"), USER_ROLES().As("ur")
			p := VariadicPredicate{
				Toplevel: true,
				Predicates: []Predicate{
					u.DISPLAYNAME.ILikeString(`%bob%`),
					CustomPredicate{
						Format: "? BETWEEN ? AND ?",
						Values: []interface{}{ur.COHORT, "aaa", "zzz"},
					},
					ur.ROLE.Eq(u.EMAIL),
				},
			}
			excludeTableQualifiers := []string{ur.GetAlias()}
			wantQuery := "u.displayname ILIKE ? AND cohort BETWEEN ? AND ? AND role = u.email"
			wantArgs := []interface{}{`%bob%`, "aaa", "zzz"}
			return TT{DESCRIPTION, p, excludeTableQualifiers, wantQuery, wantArgs}
		}(),
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			gotQuery, gotArgs := tt.p.ToSQL(tt.excludeTableQualifiers)
			is.Equal(tt.wantQuery, gotQuery)
			is.Equal(tt.wantArgs, gotArgs)
		})
	}
}

func TestVariadicPredicates_WriteSQL(t *testing.T) {
	type TT struct {
		DESCRIPTION            string
		p                      VariadicPredicate
		prependWith            string
		appendWith             string
		excludeTableQualifiers []string
		written                bool
		wantQuery              string
		wantArgs               []interface{}
	}
	tests := []TT{
		func() TT {
			DESCRIPTION := "having only one child VariadicPredicate" +
				" converts child VariadicPredicate to Toplevel (no double nesting)"
			p := VariadicPredicate{
				Predicates: []Predicate{
					VariadicPredicate{
						Predicates: []Predicate{
							CustomPredicate{Format: "aaa"},
							CustomPredicate{Format: "bbb"},
							CustomPredicate{Format: "ccc"},
						},
					},
				},
			}
			prependWith, appendWith := "LOREM IPSUM (", ") DOLOR SIT AMET"
			wantQuery := "LOREM IPSUM (aaa AND bbb AND ccc) DOLOR SIT AMET"
			written := true
			return TT{DESCRIPTION, p, prependWith, appendWith, nil, written, wantQuery, []interface{}{}}
		}(),
		func() TT {
			DESCRIPTION := "nested nil predicates still result in empty string"
			p := VariadicPredicate{
				Toplevel: true,
				Predicates: []Predicate{
					VariadicPredicate{
						Predicates: []Predicate{
							VariadicPredicate{
								Predicates: []Predicate{nil, nil, nil},
							},
						},
					},
				},
			}
			prependWith, appendWith := "THIS WILL (", ") NOT BE WRITTEN"
			wantQuery := ""
			written := false
			return TT{DESCRIPTION, p, prependWith, appendWith, nil, written, wantQuery, []interface{}{}}
		}(),
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			buf, args := &strings.Builder{}, []interface{}{}
			buf.WriteString("xxx")
			written := tt.p.WriteSQL(buf, &args, tt.prependWith, tt.appendWith, tt.excludeTableQualifiers)
			is.Equal(written, tt.written)
			if tt.written {
				// if something was written, a space should have been inserted
				is.Equal("xxx "+tt.wantQuery, buf.String())
			} else {
				// if nothing was written, there should be no extra space inserted
				is.Equal("xxx"+tt.wantQuery, buf.String())
			}
			is.Equal(tt.wantArgs, args)
		})
	}
}

func TestUnaryPredicate_ToSQL(t *testing.T) {
	type TT struct {
		DESCRIPTION            string
		p                      UnaryPredicate
		excludeTableQualifiers []string
		wantQuery              string
		wantArgs               []interface{}
	}
	tests := []TT{
		func() TT {
			DESCRIPTION := "IS NULL (implicit)"
			u := USERS().As("u")
			p := UnaryPredicate{
				Field: u.UID,
			}
			wantQuery := "u.uid IS NULL"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "IS NULL (explicit)"
			u := USERS().As("u")
			p := UnaryPredicate{
				Operator: PredicateIsNull,
				Field:    u.UID,
			}
			wantQuery := "u.uid IS NULL"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "empty field results in NULL"
			p := UnaryPredicate{
				Operator: PredicateIsNull,
			}
			wantQuery := "NULL IS NULL"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "excludeTableQualifiers is obeyed"
			u := USERS().As("u")
			p := UnaryPredicate{
				Operator: PredicateIsNotNull,
				Field:    u.UID,
			}
			excludeTableQualifiers := []string{u.GetAlias(), u.GetName()}
			wantQuery := "uid IS NOT NULL"
			return TT{DESCRIPTION, p, excludeTableQualifiers, wantQuery, nil}
		}(),
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			gotQuery, gotArgs := tt.p.ToSQL(tt.excludeTableQualifiers)
			is.Equal(tt.wantQuery, gotQuery)
			is.Equal(tt.wantArgs, gotArgs)
		})
	}
}

func TestBinaryPredicate_ToSQL(t *testing.T) {
	type TT struct {
		DESCRIPTION            string
		p                      BinaryPredicate
		excludeTableQualifiers []string
		wantQuery              string
		wantArgs               []interface{}
	}
	tests := []TT{
		func() TT {
			DESCRIPTION := "Eq (implicit) with NULL RightField"
			u := USERS().As("u")
			p := BinaryPredicate{
				LeftField: u.PASSWORD,
			}
			wantQuery := "u.password = NULL"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "Eq (explicit) with NULL LeftField"
			u := USERS().As("u")
			p := BinaryPredicate{
				Operator:   PredicateEq,
				RightField: u.PASSWORD,
			}
			wantQuery := "NULL = u.password"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "respect excludeTableQualifiers"
			u := USERS().As("u")
			p := BinaryPredicate{
				Operator:   PredicateEq,
				RightField: u.EMAIL,
			}
			excludeTableQualifiers := []string{u.GetAlias()}
			wantQuery := "NULL = email"
			return TT{DESCRIPTION, p, excludeTableQualifiers, wantQuery, nil}
		}(),
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			gotQuery, gotArgs := tt.p.ToSQL(tt.excludeTableQualifiers)
			is.Equal(tt.wantQuery, gotQuery)
			is.Equal(tt.wantArgs, gotArgs)
		})
	}
}

func TestTernaryPredicate_ToSQL(t *testing.T) {
	type TT struct {
		DESCRIPTION            string
		p                      TernaryPredicate
		excludeTableQualifiers []string
		wantQuery              string
		wantArgs               []interface{}
	}
	tests := []TT{
		func() TT {
			DESCRIPTION := "all NULLS with (implicit) between"
			p := TernaryPredicate{}
			wantQuery := "NULL BETWEEN NULL AND NULL"
			return TT{DESCRIPTION, p, nil, wantQuery, nil}
		}(),
		func() TT {
			DESCRIPTION := "respect excludeTableQualifiers"
			u, ur := USERS().As("u"), USER_ROLES().As("ur")
			p := TernaryPredicate{
				Operator: PredicateNotBetween,
				Field:    u.UID,
				FieldX:   ur.COHORT,
				FieldY:   ur.ROLE,
			}
			excludeTableQualifiers := []string{ur.GetAlias()}
			wantQuery := "u.uid NOT BETWEEN cohort AND role"
			return TT{DESCRIPTION, p, excludeTableQualifiers, wantQuery, nil}
		}(),
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.DESCRIPTION, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)
			gotQuery, gotArgs := tt.p.ToSQL(tt.excludeTableQualifiers)
			is.Equal(tt.wantQuery, gotQuery)
			is.Equal(tt.wantArgs, gotArgs)
		})
	}
}
