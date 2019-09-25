package buildsqlx

import (
	"database/sql"
	"strconv"
)

// column types
const (
	TypeSerial       = "SERIAL"
	TypeBigSerial    = "BIGSERIAL"
	TypeSmallInt     = "SMALLINT"
	TypeInt          = "INTEGER"
	TypeBigInt       = "BIGINT"
	TypeText         = "TEXT"
	TypeVarchar      = "VARCHAR"
	TypeChar         = "CHAR"
	TypeDate         = "DATE"
	TypeTime         = "TIME"
	TypeDateTime     = "TIMESTAMP"
	TypeDateTimeTz   = "TIMESTAMPTZ"
	CurrentDate      = "CURRENT_DATE"
	CurrentTime      = "CURRENT_TIME"
	CurrentDateTime  = "NOW()"
	TypeDblPrecision = "DOUBLE PRECISION"
	TypeNumeric      = "NUMERIC"
	TypeTsVector     = "TSVECTOR"
	TypeTsQuery      = "TSQUERY"
	TypeJson         = "JSON"
	TypeJsonb        = "JSONB"
	TypePoint        = "POINT"
	TypePolygon      = "POLYGON"
)

// specific for PostgreSQL driver
const (
	StandardSchema = "public"
	SemiColon      = ";"
	AlterTable     = "ALTER TABLE "
	Add            = " ADD "
	Modify         = " MODIFY "
	Drop           = " DROP "
)

type colType string

type Table struct {
	columns []*column
	tblName string
	comment *string
}

// collection of properties for the column
type column struct {
	Name         string
	IsNotNull    bool
	IsPrimaryKey bool
	ColumnType   colType
	Default      *string
	IsIndex      bool
	IsUnique     bool
	ForeignKey   *string
	IdxName      string
	Comment      *string
	IsDrop       bool
	IsModify     bool
}

// Schema creates and/or manipulates table structure with an appropriate types/indices/comments/defaults/nulls etc
func (r *DB) Schema(tblName string, fn func(table *Table)) (res sql.Result, err error) {
	tbl := &Table{tblName: tblName}
	fn(tbl) // run fn with Table struct passed to collect columns to []*column slice

	l := len(tbl.columns)
	if l > 0 {
		tblExists, err := r.HasTable(StandardSchema, tblName)
		if err != nil {
			return nil, err
		}

		if tblExists { // modify tbl by adding/modifying/deleting columns/indices
			return r.modifyTable(tbl)
		} else { // create table with relative columns/indices
			return r.createTable(tbl)
		}
	}
	return
}

func (r *DB) createIndices(indices []string) (res sql.Result, err error) {
	for _, idx := range indices {
		if idx != "" {
			res, err = r.Sql().Exec(idx)
			if err != nil {
				return nil, err
			}
		}
	}
	return
}

func (r *DB) createComments(comments []string) (res sql.Result, err error) {
	for _, comment := range comments {
		if comment != "" {
			res, err = r.Sql().Exec(comment)
			if err != nil {
				return nil, err
			}
		}
	}
	return
}

// builds column definition
func composeColumn(col *column) string {
	return col.Name + " " + string(col.ColumnType) + buildColumnOptions(col)
}

// builds column definition
func composeAddColumn(tblName string, col *column) string {
	return columnDef(tblName, col, Add)
}

// builds column definition
func composeModifyColumn(tblName string, col *column) string {
	return columnDef(tblName, col, Modify)
}

// builds column definition
func composeDropColumn(tblName string, col *column) string {
	return columnDef(tblName, col, Drop)
}

func columnDef(tblName string, col *column, op string) string {
	return AlterTable + tblName + op + "COLUMN " + col.Name + " " + string(col.ColumnType) + buildColumnOptions(col)
}

func buildColumnOptions(col *column) (colSchema string) {
	if col.IsPrimaryKey {
		colSchema += " PRIMARY KEY"
	}

	if col.IsNotNull {
		colSchema += " NOT NULL"
	}

	if col.Default != nil {
		colSchema += " DEFAULT " + *col.Default
	}
	return
}

// build index for table on particular column depending on an index type
func composeIndex(tblName string, col *column) string {
	if col.IsIndex {
		return "CREATE INDEX " + col.IdxName + " ON " + tblName + " (" + col.Name + ")"
	}

	if col.IsUnique {
		return "CREATE UNIQUE INDEX " + col.IdxName + " ON " + tblName + " (" + col.Name + ")"
	}

	if col.ForeignKey != nil {
		return *col.ForeignKey
	}
	return ""
}

func composeComment(tblName string, col *column) string {
	if col.Comment != nil {
		return "COMMENT ON COLUMN " + tblName + "." + col.Name + " IS '" + *col.Comment + "'"
	}
	return ""
}

func (t *Table) composeTableComment() string {
	if t.comment != nil {
		return "COMMENT ON TABLE " + t.tblName + " IS '" + *t.comment + "'"
	}
	return ""
}

// Increments creates auto incremented primary key integer column
func (t *Table) Increments(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeSerial, IsPrimaryKey: true})
	return t
}

// BigIncrements creates auto incremented primary key big integer column
func (t *Table) BigIncrements(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeBigSerial, IsPrimaryKey: true})
	return t
}

// SmallInt creates small integer column
func (t *Table) SmallInt(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeSmallInt})
	return t
}

// Integer creates an integer column
func (t *Table) Integer(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeInt})
	return t
}

// BigInt creates big integer column
func (t *Table) BigInt(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeBigInt})
	return t
}

// String creates varchar(len) column
func (t *Table) String(colNm string, len uint64) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: colType(TypeVarchar + "(" + strconv.FormatUint(len, 10) + ")")})
	return t
}

// Char creates char(len) column
func (t *Table) Char(colNm string, len uint64) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: colType(TypeChar + "(" + strconv.FormatUint(len, 10) + ")")})
	return t
}

// Text	creates text column
func (t *Table) Text(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeText})
	return t
}

// Text	creates text column
func (t *Table) DblPrecision(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeDblPrecision})
	return t
}

// Numeric creates exact, user-specified precision number
func (t *Table) Numeric(colNm string, precision, scale uint64) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: colType(TypeNumeric + "(" + strconv.FormatUint(precision, 10) + ", " + strconv.FormatUint(scale, 10) + ")")})
	return t
}

// Decimal alias for Numeric as for PostgreSQL they are the same
func (t *Table) Decimal(colNm string, precision, scale uint64) *Table {
	return t.Numeric(colNm, precision, scale)
}

// NotNull sets the last column to not null
func (t *Table) NotNull() *Table {
	t.columns[len(t.columns)-1].IsNotNull = true
	return t
}

// Default sets the default column value
func (t *Table) Default(val interface{}) *Table {
	v := convertToStr(val)
	t.columns[len(t.columns)-1].Default = &v
	return t
}

// Comment sets the column comment
func (t *Table) Comment(cmt string) *Table {
	t.columns[len(t.columns)-1].Comment = &cmt
	return t
}

// TableComment sets the comment for table
func (t *Table) TableComment(cmt string) {
	t.comment = &cmt
}

// Index sets the last column to btree index
func (t *Table) Index(idxName string) *Table {
	t.columns[len(t.columns)-1].IsIndex = true
	return t
}

// Unique sets the last column to unique index
func (t *Table) Unique(idxName string) *Table {
	t.columns[len(t.columns)-1].IsUnique = true
	return t
}

// ForeignKey sets the last column to reference rfcTbl on onCol with idxName foreign key index
func (t *Table) ForeignKey(idxName, rfcTbl, onCol string) *Table {
	key := AlterTable + t.tblName + " ADD CONSTRAINT " + idxName + " FOREIGN KEY (" + t.columns[len(t.columns)-1].Name + ") REFERENCES " + rfcTbl + " (" + onCol + ")"
	t.columns[len(t.columns)-1].ForeignKey = &key
	return t
}

// Date	creates date column with an ability to set current_date as default value
func (t *Table) Date(colNm string, isDefault bool) *Table {
	t.columns = append(t.columns, buildDateTIme(colNm, TypeDate, CurrentDate, isDefault))
	return t
}

// Time creates time column with an ability to set current_time as default value
func (t *Table) Time(colNm string, isDefault bool) *Table {
	t.columns = append(t.columns, buildDateTIme(colNm, TypeTime, CurrentTime, isDefault))
	return t
}

// DateTime creates datetime column with an ability to set NOW() as default value
func (t *Table) DateTime(colNm string, isDefault bool) *Table {
	t.columns = append(t.columns, buildDateTIme(colNm, TypeDateTime, CurrentDateTime, isDefault))
	return t
}

// DateTimeTz creates datetime column with an ability to set NOW() as default value + time zone support
func (t *Table) DateTimeTz(colNm string, isDefault bool) *Table {
	t.columns = append(t.columns, buildDateTIme(colNm, TypeDateTimeTz, CurrentDateTime, isDefault))
	return t
}

// TsVector creates tsvector typed column
func (t *Table) TsVector(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeTsVector})
	return t
}

// TsVector creates tsvector typed column
func (t *Table) TsQuery(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeTsQuery})
	return t
}

// Json creates json text typed column
func (t *Table) Json(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeJson})
	return t
}

// Jsonb creates jsonb typed column
func (t *Table) Jsonb(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypeJsonb})
	return t
}

// Point creates point geometry typed column
func (t *Table) Point(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypePoint})
	return t
}

// Polygon creates point geometry typed column
func (t *Table) Polygon(colNm string) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: TypePolygon})
	return t
}

// build any date/time type with defaults preset
func buildDateTIme(colNm, t, defType string, isDefault bool) *column {
	col := &column{Name: colNm, ColumnType: colType(t)}
	if isDefault {
		col.Default = &defType
	}
	return col
}

// DropColumn the column named colNm in this table context
func (t *Table) DropColumn(colNm string) {
	t.columns = append(t.columns, &column{Name: colNm, IsDrop: true})
}

// createTable create table with relative columns/indices
func (r *DB) createTable(t *Table) (res sql.Result, err error) {
	l := len(t.columns)
	var indices []string
	var comments []string

	query := "CREATE TABLE " + t.tblName + "("
	for k, col := range t.columns {
		query += composeColumn(col)
		if k < l-1 {
			query += ","
		}
		indices = append(indices, composeIndex(t.tblName, col))
		comments = append(comments, composeComment(t.tblName, col))
	}
	query += ")"

	res, err = r.Sql().Exec(query)
	// create indices
	_, err = r.createIndices(indices)
	if err != nil {
		return nil, err
	}
	// create comments
	comments = append(comments, t.composeTableComment())
	_, err = r.createComments(comments)
	if err != nil {
		return nil, err
	}
	return
}

func (r *DB) modifyTable(t *Table) (res sql.Result, err error) {
	l := len(t.columns)

	var indices []string
	var comments []string
	query := ""
	for k, col := range t.columns {
		if col.IsModify {
			query += composeModifyColumn(t.tblName, col)
		} else if col.IsDrop {
			query += composeDropColumn(t.tblName, col)
		} else { // create new column/comment/index
			query += composeAddColumn(t.tblName, col)
			indices = append(indices, composeIndex(t.tblName, col))
			comments = append(comments, composeComment(t.tblName, col))
		}

		if k < l-1 {
			query += SemiColon
		}
	}

	res, err = r.Sql().Exec(query)

	// create indices
	_, err = r.createIndices(indices)
	if err != nil {
		return nil, err
	}
	// create comments
	_, err = r.createComments(comments)
	if err != nil {
		return nil, err
	}
	return
}
