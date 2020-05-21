package main

import (
	"fmt"

	qy "github.com/bokwoon95/qx-postgres"
	"github.com/bokwoon95/qx-postgres/qx"
	"github.com/davecgh/go-spew/spew"
)

var _ = fmt.Sprint

type TABLE_APPLICATIONS struct {
	*qx.TableInfo
	APNID         qx.NumberField
	COHORT        qx.StringField
	CREATED_AT    qx.TimeField
	CREATOR       qx.NumberField
	DATA          qx.JSONField
	DELETED_AT    qx.TimeField
	MAGICSTRING   qx.StringField
	PROJECT_IDEA  qx.StringField
	PROJECT_LEVEL qx.StringField
	SCHEMA        qx.NumberField
	STATUS        qx.StringField
	SUBMITTED     qx.BooleanField
	TEAM          qx.NumberField
	TEAM_NAME     qx.StringField
	UPDATED_AT    qx.TimeField
}

func APPLICATIONS() TABLE_APPLICATIONS {
	tbl := TABLE_APPLICATIONS{TableInfo: qx.NewTableInfo("public", "applications")}
	tbl.APNID = qx.NewNumberField("apnid", tbl.TableInfo)
	tbl.COHORT = qx.NewStringField("cohort", tbl.TableInfo)
	tbl.CREATED_AT = qx.NewTimeField("created_at", tbl.TableInfo)
	tbl.CREATOR = qx.NewNumberField("creator", tbl.TableInfo)
	tbl.DATA = qx.NewJSONField("data", tbl.TableInfo)
	tbl.DELETED_AT = qx.NewTimeField("deleted_at", tbl.TableInfo)
	tbl.MAGICSTRING = qx.NewStringField("magicstring", tbl.TableInfo)
	tbl.PROJECT_IDEA = qx.NewStringField("project_idea", tbl.TableInfo)
	tbl.PROJECT_LEVEL = qx.NewStringField("project_level", tbl.TableInfo)
	tbl.SCHEMA = qx.NewNumberField("schema", tbl.TableInfo)
	tbl.STATUS = qx.NewStringField("status", tbl.TableInfo)
	tbl.SUBMITTED = qx.NewBooleanField("submitted", tbl.TableInfo)
	tbl.TEAM = qx.NewNumberField("team", tbl.TableInfo)
	tbl.TEAM_NAME = qx.NewStringField("team_name", tbl.TableInfo)
	tbl.UPDATED_AT = qx.NewTimeField("updated_at", tbl.TableInfo)
	return tbl
}

func (tbl TABLE_APPLICATIONS) As(alias string) TABLE_APPLICATIONS {
	tbl2 := APPLICATIONS()
	tbl2.TableInfo.Alias = alias
	return tbl2
}

func main() {
	_ = func() qx.Query {
		return nil
	}
	_ = func() qx.Table {
		return nil
	}
	_ = qx.And(nil, nil)
	_ = func(row qy.Row) {
		_ = row.Bool_(nil)
	}
	a := APPLICATIONS()
	a.Alias = ""
	a.TableInfo.Fields = append(a.TableInfo.Fields, qx.Float64(22.345))
	a.APNID = a.APNID.As("jablinski")
	spew.Dump(a)
	a = a.As("a")
	q := qy.Queryf(
		"SELECT ? FROM ? AS ? WHERE ?",
		qx.Fields{a.APNID, a.COHORT, a.CREATED_AT},
		a, qy.Fieldf(a.GetAlias()),
		qx.VariadicPredicate{
			Toplevel: true,
			Predicates: []qx.Predicate{
				a.SUBMITTED.IsNotNull(),
				qy.Predicatef("? IN (?)", a.COHORT, []string{"2020", "2019", "2018"}),
			},
		},
	)
	fmt.Println(q.ToSQL())
	sel := qy.Select(a.APNID, a.COHORT, a.CREATED_AT).From(a).
		Where(
			a.SUBMITTED.IsNotNull(),
			qy.Predicatef("? IN (?)", a.COHORT, []string{"2020", "2019", "2018"}),
		)
	fmt.Println(sel.ToSQL())
	q.Postgres = true
	q = qy.Queryf("INSERT INTO ? VALUES ?", a, qx.ValuesList{
		[]interface{}{1, "yohoho"},
		[]interface{}{2, "schymoyo"},
	})
	fmt.Println(q.ToSQL())
}
