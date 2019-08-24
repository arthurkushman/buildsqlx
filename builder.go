package arsqlx

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// inner type to build qualified sql
type builder struct {
	where      string
	whereNamed map[string]interface{}
	table      string
	from       string
	join       []string
	orderBy    map[string]string
	groupBy    string
	having     string
	columns    []string
	union      []string
	offset     int64
	limit      int64
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

func (r *DB) Sql() *sql.DB {
	return r.Conn.db
}

// NewDb constructs default DB structure
func NewDb(c *Connection) *DB {
	b := newBuilder()
	return &DB{Builder: b, Conn: c}
}

//func HasTable(table string) bool {
//
//}
//
//func HasColumns(table string, columns []string) bool {
//
//}
//
//func GetColumnType() string {
//
//}

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
	r.Builder.groupBy = ""
	r.Builder.having = ""
	r.Builder.orderBy = map[string]string{}
	r.Builder.offset = 0
	r.Builder.limit = 0
}

// Select accepts columns to select from a table
func (r *DB) Select(args ...string) *DB {
	r.Builder.columns = []string{}
	r.Builder.columns = append(r.Builder.columns, args...)
	//for k, arg := range args {
	//	if k == 0 {
	//		r.builder.columns
	//	}
	//
	//	r.builder.columns = append(r.builder.columns, arg)
	//}

	return r
}

// GroupBy adds ORDER BY expression to SQL stmt
func (r *DB) OrderBy(column string, direction string) *DB {
	if len(r.Builder.orderBy) == 0 {
		r.Builder.orderBy = make(map[string]string)
	}

	r.Builder.orderBy[column] = direction

	return r
}

// GroupBy adds GROUP BY expression to SQL stmt
func (r *DB) GroupBy(expr string) *DB {
	r.Builder.groupBy = expr

	return r
}

// AddSelect accepts additional columns to select from a table
func (r *DB) AddSelect(args ...string) *DB {
	for _, arg := range args {
		r.Builder.columns = append(r.Builder.columns, arg)
	}

	return r
}

// SelectRow accepts custom string to select from a table
func (r *DB) SelectRow(row string) *DB {
	r.Builder.columns = append(r.Builder.columns, row)

	return r
}

func (r *DB) LeftJoin(table string, left string, operator string, right string) *DB {
	r.Builder.join = append(r.Builder.join, " LEFT JOIN "+table+" ON "+left+operator+right+" ")

	return r
}

func (r *DB) RightJoin(table string, left string, operator string, right string) *DB {
	r.Builder.join = append(r.Builder.join, " RIGHT JOIN "+table+" ON "+left+operator+right+" ")

	return r
}

func (r *DB) CrossJoin(table string, left string, operator string, right string) *DB {
	r.Builder.join = append(r.Builder.join, " CROSS JOIN "+table+" ON "+left+operator+right+" ")

	return r
}

// Where accepts left operand-operator-right operand to apply them to where clause
func (r *DB) Where(operand string, operator string, val interface{}) *DB {
	r.Builder.where = operand + " " + operator + " " + convertToStr(val)

	return r
}

// Where accepts left operand-operator-right operand to apply them to where clause
// with AND logical operator
func (r *DB) AndWhere(operand string, operator string, val interface{}) *DB {
	r.Builder.where += " AND " + operand + " " + operator + " " + convertToStr(val)

	return r
}

// OrWhere accepts left operand-operator-right operand to apply them to where clause
// with OR logical operator
func (r *DB) OrWhere(operand string, operator string, val interface{}) *DB {
	r.Builder.where += " OR " + operand + " " + operator + " " + convertToStr(val)

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
	case float64:
		return fmt.Sprintf("%g", v)
	}

	return ""
}

// WhereRaw accepts custom string to apply it to where clause
func (r *DB) WhereRaw(raw string) *DB {
	r.Builder.where = raw

	return r
}

// OrWhereRaw accepts custom string to apply it to where clause with logical OR
func (r *DB) OrWhereRaw(raw string) *DB {
	r.Builder.where = " OR " + raw

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

// Drop drops >=1 tables if they are existent
func (r *DB) DropIfExists(tables string) (sql.Result, error) {
	return r.Sql().Exec("DROP TABLE IF EXISTS " + tables)
}

// Rename renames from - to new table name
func (r *DB) Rename(from, to string) (sql.Result, error) {
	return r.Sql().Exec("ALTER TABLE " + from + " RENAME TO " + to)
}

// WhereIn appends IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) WhereIn(field string, in []interface{}) *DB {
	r.Builder.where += " " + field + " IN (" + strings.Join(prepareSlice(in), ", ") + ")"

	return r
}

// WhereNotIn appends NOT IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) WhereNotIn(field string, in []interface{}) *DB {
	r.Builder.where += " " + field + " NOT IN (" + strings.Join(prepareSlice(in), ", ") + ")"

	return r
}

// OrWhereIn appends OR IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) OrWhereIn(field string, in []interface{}) *DB {
	r.Builder.where += " OR " + field + " IN (" + strings.Join(prepareSlice(in), ", ") + ")"

	return r
}

// OrWhereNotIn appends OR NOT IN (val1, val2, val3...) stmt to WHERE clause
func (r *DB) OrWhereNotIn(field string, in []interface{}) *DB {
	r.Builder.where += " OR " + field + " NOT IN (" + strings.Join(prepareSlice(in), ", ") + ")"

	return r
}

// WhereIsNull appends fieldName IS NULL stmt to WHERE clause
func (r *DB) WhereNull(field string) *DB {
	r.Builder.where += " " + field + " IS NULL"

	return r
}

// WhereNotNull appends fieldName IS NOT NULL stmt to WHERE clause
func (r *DB) WhereNotNull(field string) *DB {
	r.Builder.where += " " + field + " IS NOT NULL"

	return r
}

// OrWhereIsNull appends fieldName IS NULL stmt to WHERE clause
func (r *DB) OrWhereNull(field string) *DB {
	r.Builder.where += " OR " + field + " IS NULL"

	return r
}

// OrWhereNotNull appends fieldName IS NOT NULL stmt to WHERE clause
func (r *DB) OrWhereNotNull(field string) *DB {
	r.Builder.where += " OR " + field + " IS NOT NULL"

	return r
}

// prepares slice for IN/NOT IN etc
func prepareSlice(in []interface{}) (out []string) {
	for _, value := range in {
		switch v := value.(type) {
		case string:
			out = append(out, v)
			break
		case int:
			out = append(out, strconv.FormatInt(int64(v), 10))
			break
		case float64:
			out = append(out, fmt.Sprintf("%g", v))
			break
		case int64:
		case uint64:
			out = append(out, strconv.FormatUint(v, 10))
			break
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

// Dump prints raw sql to stdout
func (r *DB) Dump() {
	log.SetOutput(os.Stdout)
	log.Println(r.Builder.buildSelect())
}

// Dd prints raw sql to stdout and exit
func (r *DB) Dd() {
	log.SetOutput(os.Stdout)
	log.Println(r.Builder.buildSelect())
	os.Exit(0)
}
