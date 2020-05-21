package qx

// SelectType represents the various SQL selects.
type SelectType string

// SelectTypes
const (
	SelectTypeDefault    SelectType = "SELECT"
	SelectTypeDistinct   SelectType = "SELECT DISTINCT"
	SelectTypeDistinctOn SelectType = "SELECT DISTINCT ON" // postgres only
)

// JoinType represents the various types of SQL joins.
type JoinType string

// JoinTypes
const (
	JoinTypeDefault JoinType = "JOIN"
	JoinTypeLeft    JoinType = "LEFT JOIN"
	JoinTypeRight   JoinType = "RIGHT JOIN"
	JoinTypeFull    JoinType = "FULL JOIN"
	JoinTypeCross   JoinType = "CROSS JOIN"
)
