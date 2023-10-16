package buildsqlx

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
)

// First getting the 1st row of query
func (r *DB) First(src any) error {
	err := r.ScanStruct(src)
	if err != nil {
		return err
	}

	return nil
}

// Value gets the value of column in first query resulting row
func (r *DB) Value(src any, column string) error {
	err := r.Select(column).ScanStruct(src)
	if err != nil {
		return err
	}

	return nil
}

// Find retrieves a single row by it's id column value
func (r *DB) Find(src any, id uint64) error {
	return r.Where("id", "=", id).First(src)
}

// Pluck getting values of a particular column and place them into slice
func (r *DB) Pluck(column string) (val []interface{}, err error) {
	res, err := r.Get()
	if err != nil {
		return nil, err
	}

	val = make([]interface{}, len(res))
	for k, m := range res {
		val[k] = m[column]
	}

	return
}

// PluckMap getting values of a particular key/value columns and place them into map
func (r *DB) PluckMap(colKey, colValue string) (val []map[interface{}]interface{}, err error) {
	res, err := r.Get()
	if err != nil {
		return nil, err
	}

	val = make([]map[interface{}]interface{}, len(res))
	for k, m := range res {
		val[k] = make(map[interface{}]interface{})
		val[k][m[colKey]] = m[colValue]
	}

	return
}

// Exists checks whether conditional rows are existing (returns true) or not (returns false)
func (r *DB) Exists() (exists bool, err error) {
	bldr := r.Builder
	if bldr.table == "" {
		return false, errTableCallBeforeOp
	}

	query := `SELECT EXISTS(SELECT 1 FROM "` + bldr.table + `" ` + bldr.buildClauses() + `)`
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&exists)

	return
}

// DoesntExists an inverse of Exists
func (r *DB) DoesntExists() (bool, error) {
	ex, err := r.Exists()
	if err != nil {
		return false, err
	}

	return !ex, nil
}

// Increment column on passed value
func (r *DB) Increment(column string, on uint64) (int64, error) {
	return r.incrDecr(column, plusSign, on)
}

// Decrement column on passed value
func (r *DB) Decrement(column string, on uint64) (int64, error) {
	return r.incrDecr(column, minusSign, on)
}

// increments or decrements depending on sign
func (r *DB) incrDecr(column, sign string, on uint64) (int64, error) {
	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	query := `UPDATE "` + r.Builder.table + `" SET ` + column + ` = ` + column + sign + strconv.FormatUint(on, 10)

	res, err := r.Sql().Exec(query)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// Chunk run queries by chinks by passing user-land function with an ability to stop execution when needed
// by returning false and proceed to execute queries when return true
func (r *DB) Chunk(src any, amount int64, fn func(rows []any) bool) error {
	cols := r.Builder.columns
	cnt, err := r.Count()
	if err != nil {
		return err
	}

	r.Builder.columns = cols
	if amount <= 0 {
		return fmt.Errorf("chunk can't be <= 0, your chunk is: %d", amount)
	}

	if cnt < amount {
		structRows, err := r.eachToStructRows(src, 0, 0)
		if err != nil {
			return err
		}

		fn(structRows) // execute all resulting records

		return nil
	}

	// executing chunks amount < cnt
	c := int64(math.Ceil(float64(cnt / amount)))
	for i := int64(0); i < c; i++ {
		structRows, err := r.eachToStructRows(src, i*amount, amount)
		if err != nil {
			return err
		}

		res := fn(structRows)
		if !res { // stop an execution when false returned by user
			break
		}
	}

	return nil
}

func (r *DB) eachToStructRows(src any, offset, limit int64) ([]any, error) {
	var structRows []any
	if limit > 0 {
		r.Offset(offset).Limit(limit)
	}

	err := r.EachToStruct(func(rows *sql.Rows) error {
		err := r.Next(rows, src)
		if err != nil {
			return err
		}

		v := reflect.ValueOf(src).Elem().Interface()
		structRows = append(structRows, v)

		return nil
	})

	return structRows, err
}
