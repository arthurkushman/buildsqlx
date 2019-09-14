package buildsqlx

import (
	"database/sql"
	"strconv"
)

const (
	TypeSerial       = "SERIAL"
	TypeBigSerial    = "BIGSERIAL"
	TypeSmallInt     = "SMALLINT"
	TypeInt          = "INTEGER"
	TypeBigInt       = "BIGINT"
	TypeText         = "TEXT"
	TypeVarchar      = "VARCHAR"
	TypeDblPrecision = "DOUBLE PRECISION"
	TypeNumeric      = "NUMERIC"
)

type colType string

type Table struct {
	columns []*column
}

type column struct {
	Name         string
	IsNotNull    bool
	IsPrimaryKey bool
	ColumnType   colType
	Default      *string
}

func (r *DB) CreateTable(tblName string, fn func(table *Table)) (sql.Result, error) {
	tbl := Table{}
	fn(&tbl) // run fn with Table struct passed to collect columns to []*column slice

	l := len(tbl.columns)
	query := "CREATE TABLE " + tblName + "("
	for k, col := range tbl.columns {
		query += composeColumn(col)
		if k < l-1 {
			query += ","
		}
	}
	query += ")"
	return r.Sql().Exec(query)
}

// builds column definition
func composeColumn(col *column) string {
	colSchema := col.Name + " " + string(col.ColumnType)
	if col.IsPrimaryKey {
		colSchema += " PRIMARY KEY"
	}

	if col.IsNotNull {
		colSchema += " NOT NULL"
	}

	if col.Default != nil {
		colSchema += " DEFAULT " + *col.Default
	}
	return colSchema
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

// Numeric	creates exact, user-specified precision number
func (t *Table) Numeric(colNm string, precision, scale uint64) *Table {
	t.columns = append(t.columns, &column{Name: colNm, ColumnType: colType(TypeNumeric + "(" + strconv.FormatUint(precision, 10) + ", " + strconv.FormatUint(scale, 10) + ")")})
	return t
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
