package arsqlx

import (
	"database/sql"
)

type Builder struct {
	where      string
	whereNamed map[string]interface{}
	table      string
	from       []string
	join       []string
	orderBy    map[string]string
	groupBy    string
	having     string
	columns    []string
	union      []string
	offset     int64
	limit      int64
}

type DB struct {
	Builder *Builder
	Conn    *Connection
}

type Where struct {
	left  string
	op    string
	right string
}

func NewBuilder() *Builder {
	return &Builder{
		columns: []string{"*"},
	}
}

func (r *DB) Sql() *sql.DB {
	return r.Conn.db
}

func NewDb(c *Connection) *DB {
	b := NewBuilder()
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

func (r *DB) Table(table string) *DB {
	r.Builder.table = table
	return r
}

// Select accepts columns to select from a table
func (r *DB) Select(args ...string) *DB {
	r.Builder.columns = []string{}
	r.Builder.columns = append(r.Builder.columns, args...)
	//for k, arg := range args {
	//	if k == 0 {
	//		r.Builder.columns
	//	}
	//
	//	r.Builder.columns = append(r.Builder.columns, arg)
	//}

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

// Where accepts left operand-operator-right operand to apply them to where clause
func (r *DB) Where(args []string) *DB {
	r.Builder.where = args[0] + " " + args[1] + " " + args[2]

	return r
}

// Where accepts left operand-operator-right operand to apply them to where clause
// with AND logical operator
func (r *DB) AndWhere(args []string) *DB {
	r.Builder.where += " AND " + args[0] + " " + args[1] + " " + args[2]

	return r
}

// OrWhere accepts left operand-operator-right operand to apply them to where clause
// with OR logical operator
func (r *DB) OrWhere(args []string) *DB {
	r.Builder.where += " OR " + args[0] + " " + args[1] + " " + args[2]

	return r
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

func Create(table string, closure func()) {

}

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

func Rename(from, to string) {

}
