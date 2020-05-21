package qx

import "database/sql"

type TestUser struct {
	Valid    bool
	Uid      int64
	Name     string
	Email    string
	Password sql.NullString
}

type TABLE_USERS struct {
	*TableInfo
	DISPLAYNAME StringField
	EMAIL       StringField
	PASSWORD    StringField
	UID         NumberField
}

func USERS() TABLE_USERS {
	tbl := TABLE_USERS{TableInfo: NewTableInfo("public", "users")}
	tbl.DISPLAYNAME = NewStringField("displayname", tbl.TableInfo)
	tbl.EMAIL = NewStringField("email", tbl.TableInfo)
	tbl.PASSWORD = NewStringField("password", tbl.TableInfo)
	tbl.UID = NewNumberField("uid", tbl.TableInfo)
	return tbl
}

func (tbl TABLE_USERS) As(alias string) TABLE_USERS {
	tbl2 := USERS()
	tbl2.TableInfo.Alias = alias
	return tbl2
}

type TABLE_USER_ROLES struct {
	*TableInfo
	COHORT     StringField
	CREATED_AT TimeField
	DELETED_AT TimeField
	ROLE       StringField
	UID        NumberField
	UPDATED_AT TimeField
	URID       NumberField
}

func USER_ROLES() TABLE_USER_ROLES {
	tbl := TABLE_USER_ROLES{TableInfo: NewTableInfo("public", "user_roles")}
	tbl.COHORT = NewStringField("cohort", tbl.TableInfo)
	tbl.CREATED_AT = NewTimeField("created_at", tbl.TableInfo)
	tbl.DELETED_AT = NewTimeField("deleted_at", tbl.TableInfo)
	tbl.ROLE = NewStringField("role", tbl.TableInfo)
	tbl.UID = NewNumberField("uid", tbl.TableInfo)
	tbl.UPDATED_AT = NewTimeField("updated_at", tbl.TableInfo)
	tbl.URID = NewNumberField("urid", tbl.TableInfo)
	return tbl
}

func (tbl TABLE_USER_ROLES) As(alias string) TABLE_USER_ROLES {
	tbl2 := USER_ROLES()
	tbl2.TableInfo.Alias = alias
	return tbl2
}

type TABLE_COHORT_ENUM struct {
	*TableInfo
	COHORT          StringField
	INSERTION_ORDER NumberField
}

func COHORT_ENUM() TABLE_COHORT_ENUM {
	tbl := TABLE_COHORT_ENUM{TableInfo: NewTableInfo("public", "cohort_enum")}
	tbl.COHORT = NewStringField("cohort", tbl.TableInfo)
	tbl.INSERTION_ORDER = NewNumberField("insertion_order", tbl.TableInfo)
	return tbl
}

func (tbl TABLE_COHORT_ENUM) As(alias string) TABLE_COHORT_ENUM {
	tbl2 := COHORT_ENUM()
	tbl2.TableInfo.Alias = alias
	return tbl2
}
