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
	tblName string
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
}

func (r *DB) CreateTable(tblName string, fn func(table *Table)) (res sql.Result, err error) {
	tbl := Table{tblName: tblName}
	fn(&tbl) // run fn with Table struct passed to collect columns to []*column slice

	l := len(tbl.columns)
	var indices []string
	query := "CREATE TABLE " + tblName + "("
	for k, col := range tbl.columns {
		query += composeColumn(col)
		if k < l-1 {
			query += ","
		}
		indices = append(indices, composeIndex(tblName, col))
	}
	query += ")"

	res, err = r.Sql().Exec(query)
	for _, idx := range indices {
		_, err = r.Sql().Exec(idx)
		if err != nil {
			return nil, err
		}
	}
	return
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
	key := "ALTER TABLE " + t.tblName + " ADD CONSTRAINT " + idxName + " FOREIGN KEY (" + t.columns[len(t.columns)-1].Name + ") REFERENCES " + rfcTbl + " (" + onCol + ")"
	t.columns[len(t.columns)-1].ForeignKey = &key
	return t
}
