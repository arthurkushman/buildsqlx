package buildsqlx

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	joinInner = "INNER"
	//JoinCross     = "CROSS"
	joinLeft      = "LEFT"
	joinRight     = "RIGHT"
	joinFull      = "FULL"
	joinFullOuter = "FULL OUTER"
	where         = " WHERE "
	and           = " AND "
	or            = " OR "
)

// inner type to build qualified sql
type builder struct {
	whereBindings   []map[string]interface{}
	startBindingsAt int
	where           string
	table           string
	from            string
	join            []string
	orderBy         []map[string]string
	orderByRaw      *string
	groupBy         string
	having          string
	columns         []string
	union           []string
	isUnionAll      bool
	offset          int64
	limit           int64
	lockForUpdate   *string
	whereExists     string
}

// DB is an entity that composite builder and Conn types
type DB struct {
	Builder *builder
	Conn    *Connection
}

func newBuilder() *builder {
	return &builder{
		columns: []string{"*"},
	}
}

// Sql returns DB struct
func (r *DB) Sql() *sql.DB {
	return r.Conn.db
}

// NewDb constructs default DB structure
func NewDb(c *Connection) *DB {
	b := newBuilder()
	return &DB{Builder: b, Conn: c}
}

// Table appends table name to sql query
func (r *DB) Table(table string) *DB {
	// reset before constructing again
	r.reset()
	r.Builder.table = table
	return r
}

// resets all builder elements to prepare them for next round
func (r *DB) reset() {
	r.Builder.table = ""
	r.Builder.columns = []string{"*"}
	r.Builder.where = ""
	r.Builder.whereBindings = make([]map[string]interface{}, 0)
	r.Builder.groupBy = ""
	r.Builder.having = ""
	r.Builder.orderBy = make([]map[string]string, 0)
	r.Builder.offset = 0
	r.Builder.limit = 0
	r.Builder.join = []string{}
	r.Builder.from = ""
	r.Builder.lockForUpdate = nil
	r.Builder.whereExists = ""
	r.Builder.orderByRaw = nil
	r.Builder.startBindingsAt = 1

	if len(r.Builder.union) == 0 {
		r.Builder.union = []string{}
	}
}

// Select accepts columns to select from a table
func (r *DB) Select(args ...string) *DB {
	r.Builder.columns = []string{}
	r.Builder.columns = append(r.Builder.columns, args...)
	return r
}

// OrderBy adds ORDER BY expression to SQL stmt
func (r *DB) OrderBy(column string, direction string) *DB {
	r.Builder.orderBy = append(r.Builder.orderBy, map[string]string{column: direction})
	return r
}

// OrderByRaw adds ORDER BY raw expression to SQL stmt
func (r *DB) OrderByRaw(exp string) *DB {
	r.Builder.orderByRaw = &exp
	return r
}

// InRandomOrder add ORDER BY random() - note be cautious on big data-tables it can lead to slowing down perf
func (r *DB) InRandomOrder() *DB {
	r.OrderByRaw("random()")
	return r
}

// GroupBy adds GROUP BY expression to SQL stmt
func (r *DB) GroupBy(expr string) *DB {
	r.Builder.groupBy = expr
	return r
}

// Having similar to Where but used with GroupBy to apply over the grouped results
func (r *DB) Having(operand, operator string, val interface{}) *DB {
	r.Builder.having = operand + " " + operator + " " + convertToStr(val)
	return r
}

// HavingRaw accepts custom string to apply it to having clause
func (r *DB) HavingRaw(raw string) *DB {
	r.Builder.having = raw
	return r
}

// OrHavingRaw accepts custom string to apply it to having clause with logical OR
func (r *DB) OrHavingRaw(raw string) *DB {
	r.Builder.having += or + raw
	return r
}

// AndHavingRaw accepts custom string to apply it to having clause with logical OR
func (r *DB) AndHavingRaw(raw string) *DB {
	r.Builder.having += and + raw
	return r
}

// AddSelect accepts additional columns to select from a table
func (r *DB) AddSelect(args ...string) *DB {
	r.Builder.columns = append(r.Builder.columns, args...)
	return r
}

// SelectRaw accepts custom string to select from a table
func (r *DB) SelectRaw(raw string) *DB {
	r.Builder.columns = []string{raw}
	return r
}

// InnerJoin joins tables by getting elements if found in both
func (r *DB) InnerJoin(table, left, operator, right string) *DB {
	return r.buildJoin(joinInner, table, left+operator+right)
}

// LeftJoin joins tables by getting elements from left without those that null on the right
func (r *DB) LeftJoin(table, left, operator, right string) *DB {
	return r.buildJoin(joinLeft, table, left+operator+right)
}

// RightJoin joins tables by getting elements from right without those that null on the left
func (r *DB) RightJoin(table, left, operator, right string) *DB {
	return r.buildJoin(joinRight, table, left+operator+right)
}

// CrossJoin joins tables by getting intersection of sets
// todo: MySQL/PostgreSQL versions are different here impl their difference
//func (r *DB) CrossJoin(table string, left string, operator string, right string) *DB {
//	return r.buildJoin(JoinCross, table, left+operator+right)
//}

// FullJoin joins tables by getting all elements of both sets
func (r *DB) FullJoin(table, left, operator, right string) *DB {
	return r.buildJoin(joinFull, table, left+operator+right)
}

// FullOuterJoin joins tables by getting an outer sets
func (r *DB) FullOuterJoin(table, left, operator, right string) *DB {
	return r.buildJoin(joinFullOuter, table, left+operator+right)
}

// Union joins multiple queries omitting duplicate records
func (r *DB) Union() *DB {
	r.Builder.union = append(r.Builder.union, r.Builder.buildSelect())
	return r
}

// UnionAll joins multiple queries to select all rows from both tables with duplicate
func (r *DB) UnionAll() *DB {
	r.Union()
	r.Builder.isUnionAll = true
	return r
}

// WhereExists constructs one builder from another to implement WHERE EXISTS sql/dml clause
func (r *DB) WhereExists(rr *DB) *DB {
	r.Builder.whereExists = " WHERE EXISTS(" + rr.Builder.buildSelect() + ")"
	return r
}

// WhereNotExists constructs one builder from another to implement WHERE NOT EXISTS sql/dml clause
func (r *DB) WhereNotExists(rr *DB) *DB {
	r.Builder.whereExists = " WHERE NOT EXISTS(" + rr.Builder.buildSelect() + ")"
	return r
}

func (r *DB) buildJoin(joinType, table, on string) *DB {
	r.Builder.join = append(r.Builder.join, " "+joinType+" JOIN "+table+" ON "+on+" ")
	return r
}

// Where accepts left operand-operator-right operand to apply them to where clause
func (r *DB) Where(operand, operator string, val interface{}) *DB {
	return r.buildWhere("", operand, operator, val)
}

// AndWhere accepts left operand-operator-right operand to apply them to where clause
// with AND logical operator
func (r *DB) AndWhere(operand, operator string, val interface{}) *DB {
	return r.buildWhere("AND", operand, operator, val)
}

// OrWhere accepts left operand-operator-right operand to apply them to where clause
// with OR logical operator
func (r *DB) OrWhere(operand, operator string, val interface{}) *DB {
	return r.buildWhere("OR", operand, operator, val)
}

func (r *DB) buildWhere(prefix, operand, operator string, val interface{}) *DB {
	if prefix != "" {
		r.Builder.whereBindings = append(r.Builder.whereBindings, map[string]interface{}{" " + prefix + " " + operand + " " + operator: val})
	} else {
		r.Builder.whereBindings = append(r.Builder.whereBindings, map[string]interface{}{operand + " " + operator: val})
	}
	return r
}

// WhereBetween sets the clause BETWEEN 2 values
func (r *DB) WhereBetween(col string, val1, val2 interface{}) *DB {
	r.Builder.where = where + col + " BETWEEN " + convertToStr(val1) + and + convertToStr(val2)
	return r
}

// OrWhereBetween sets the clause OR BETWEEN 2 values
func (r *DB) OrWhereBetween(col string, val1, val2 interface{}) *DB {
	r.Builder.where += or + col + " BETWEEN " + convertToStr(val1) + and + convertToStr(val2)
	return r
}

// AndWhereBetween sets the clause AND BETWEEN 2 values
func (r *DB) AndWhereBetween(col string, val1, val2 interface{}) *DB {
	r.Builder.where += and + col + " BETWEEN " + convertToStr(val1) + and + convertToStr(val2)
	return r
}

// WhereNotBetween sets the clause NOT BETWEEN 2 values
func (r *DB) WhereNotBetween(col string, val1, val2 interface{}) *DB {
	r.Builder.where = where + col + " NOT BETWEEN " + convertToStr(val1) + and + convertToStr(val2)
	return r
}

// OrWhereNotBetween sets the clause OR BETWEEN 2 values
func (r *DB) OrWhereNotBetween(col string, val1, val2 interface{}) *DB {
	r.Builder.where += or + col + " NOT BETWEEN " + convertToStr(val1) + and + convertToStr(val2)
	return r
}

// AndWhereNotBetween sets the clause AND BETWEEN 2 values
func (r *DB) AndWhereNotBetween(col string, val1, val2 interface{}) *DB {
	r.Builder.where += and + col + " NOT BETWEEN " + convertToStr(val1) + and + convertToStr(val2)
	return r
}

func convertToStr(val interface{}) string {
	switch v := val.(type) {
	case string:
		return "'" + v + "'"
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float64:
		return fmt.Sprintf("%g", v)
	}

	return ""
}

// WhereRaw accepts custom string to apply it to where clause
func (r *DB) WhereRaw(raw string) *DB {
	r.Builder.where = where + raw
	return r
}

// OrWhereRaw accepts custom string to apply it to where clause with logical OR
func (r *DB) OrWhereRaw(raw string) *DB {
	r.Builder.where += or + raw
	return r
}

// AndWhereRaw accepts custom string to apply it to where clause with logical OR
func (r *DB) AndWhereRaw(raw string) *DB {
	r.Builder.where += and + raw
	return r
}

// Offset accepts offset to start slicing results from
func (r *DB) Offset(off int64) *DB {
	r.Builder.offset = off
	return r
}

// Limit accepts limit to end slicing results to
func (r *DB) Limit(lim int64) *DB {
	r.Builder.limit = lim
	return r
}

//func Create(table string, closure func()) {
//
//}

// Drop drops >=1 tables
func (r *DB) Drop(tables string) (sql.Result, error) {
	return r.Sql().Exec("DROP TABLE " + tables)
}

// Truncate clears >=1 tables
func (r *DB) Truncate(tables string) (sql.Result, error) {
	return r.Sql().Exec("TRUNCATE " + tables)
}

// DropIfExists drops >=1 tables if they are existent
func (r *DB) DropIfExists(tables ...string) (res sql.Result, err error) {
	for _, tbl := range tables {
		res, err = r.Sql().Exec("DROP TABLE" + IfExistsExp + tbl)
	}

	return res, err
}

// Rename renames from - to new table name
func (r *DB) Rename(from, to string) (sql.Result, error) {
	return r.Sql().Exec("ALTER TABLE " + from + " RENAME TO " + to)
}

// WhereIn appends IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) WhereIn(field string, in interface{}) *DB {
	ins, err := interfaceToSlice(in)
	if err != nil {
		return nil
	}
	r.Builder.where = where + field + " IN (" + strings.Join(prepareSlice(ins), ", ") + ")"
	return r
}

// WhereNotIn appends NOT IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) WhereNotIn(field string, in interface{}) *DB {
	ins, err := interfaceToSlice(in)
	if err != nil {
		return nil
	}
	r.Builder.where = where + field + " NOT IN (" + strings.Join(prepareSlice(ins), ", ") + ")"
	return r
}

// OrWhereIn appends OR IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) OrWhereIn(field string, in interface{}) *DB {
	ins, err := interfaceToSlice(in)
	if err != nil {
		return nil
	}
	r.Builder.where += or + field + " IN (" + strings.Join(prepareSlice(ins), ", ") + ")"
	return r
}

// OrWhereNotIn appends OR NOT IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) OrWhereNotIn(field string, in interface{}) *DB {
	ins, err := interfaceToSlice(in)
	if err != nil {
		return nil
	}
	r.Builder.where += or + field + " NOT IN (" + strings.Join(prepareSlice(ins), ", ") + ")"
	return r
}

// AndWhereIn appends OR IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) AndWhereIn(field string, in interface{}) *DB {
	ins, err := interfaceToSlice(in)
	if err != nil {
		return nil
	}
	r.Builder.where += and + field + " IN (" + strings.Join(prepareSlice(ins), ", ") + ")"
	return r
}

// AndWhereNotIn appends OR NOT IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) AndWhereNotIn(field string, in interface{}) *DB {
	ins, err := interfaceToSlice(in)
	if err != nil {
		return nil
	}
	r.Builder.where += and + field + " NOT IN (" + strings.Join(prepareSlice(ins), ", ") + ")"
	return r
}

// WhereNull appends fieldName IS NULL stmt to WHERE clause
func (r *DB) WhereNull(field string) *DB {
	r.Builder.where = where + field + " IS NULL"
	return r
}

// WhereNotNull appends fieldName IS NOT NULL stmt to WHERE clause
func (r *DB) WhereNotNull(field string) *DB {
	r.Builder.where = where + field + " IS NOT NULL"
	return r
}

// OrWhereNull appends fieldName IS NULL stmt to WHERE clause
func (r *DB) OrWhereNull(field string) *DB {
	r.Builder.where += or + field + " IS NULL"
	return r
}

// OrWhereNotNull appends fieldName IS NOT NULL stmt to WHERE clause
func (r *DB) OrWhereNotNull(field string) *DB {
	r.Builder.where += or + field + " IS NOT NULL"
	return r
}

// AndWhereNull appends fieldName IS NULL stmt to WHERE clause
func (r *DB) AndWhereNull(field string) *DB {
	r.Builder.where += and + field + " IS NULL"
	return r
}

// AndWhereNotNull appends fieldName IS NOT NULL stmt to WHERE clause
func (r *DB) AndWhereNotNull(field string) *DB {
	r.Builder.where += and + field + " IS NOT NULL"
	return r
}

// prepares slice for Where bindings, IN/NOT IN etc
func prepareSlice(in []interface{}) (out []string) {
	for _, value := range in {
		switch v := value.(type) {
		case string:
			out = append(out, v)
		case int:
			out = append(out, strconv.FormatInt(int64(v), 10))
		case float64:
			out = append(out, fmt.Sprintf("%g", v))
		case int64:
			out = append(out, strconv.FormatInt(v, 10))
		case uint64:
			out = append(out, strconv.FormatUint(v, 10))
		}
	}

	return
}

// From prepares sql stmt to set data from another table, ex.:
// UPDATE employees SET sales_count = sales_count + 1 FROM accounts
func (r *DB) From(fromTbl string) *DB {
	r.Builder.from = fromTbl
	return r
}

// LockForUpdate locks table/row
func (r *DB) LockForUpdate() *DB {
	str := " FOR UPDATE"
	r.Builder.lockForUpdate = &str
	return r
}

// Dump prints raw sql to stdout
func (r *DB) Dump() {
	log.SetOutput(os.Stdout)
	log.Println(r.Builder.buildSelect())
}

// Dd prints raw sql to stdout and exit
func (r *DB) Dd() {
	r.Dump()
	os.Exit(0)
}

// HasTable determines whether table exists in particular schema
func (r *DB) HasTable(schema, tbl string) (tblExists bool, err error) {
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE  schemaname = '%s' AND tablename = '%s')", schema, tbl)
	err = r.Sql().QueryRow(query).Scan(&tblExists)
	return
}

// HasColumns checks whether those cols exists in a particular schema/table
func (r *DB) HasColumns(schema, tbl string, cols ...string) (colsExists bool, err error) {
	andColumns := ""
	for _, v := range cols { // todo: find a way to check columns in 1 query
		andColumns = " AND column_name = '" + v + "'"
		query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s'"+andColumns+")", schema, tbl)
		err = r.Sql().QueryRow(query).Scan(&colsExists)

		if !colsExists { // if at least once col doesn't exist - return false, nil
			return
		}
	}
	return
}
