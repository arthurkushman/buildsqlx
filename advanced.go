package buildsqlx

import (
	"fmt"
	"math"
	"strconv"
)

// First getting the 1st row of query
func (r *DB) First() (map[string]interface{}, error) {
	res, err := r.Get()
	if err != nil {
		return nil, err
	}

	if len(res) > 0 {
		return res[0], nil
	}
	return nil, fmt.Errorf("no records were produced by query: %s", r.Builder.buildSelect())
}

// Value gets the value of column in first query resulting row
func (r *DB) Value(column string) (val interface{}, err error) {
	r.Select(column)
	res, err := r.First()
	if err != nil {
		return
	}

	if val, ok := res[column]; ok {
		return val, err
	}

	return
}

// Find retrieves a single row by it's id column value
func (r *DB) Find(id int64) (map[string]interface{}, error) {
	return r.Where("id", "=", id).First()
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
	builder := r.Builder
	if builder.table == "" {
		return false, fmt.Errorf(errTableCallBeforeOp)
	}

	query := "SELECT EXISTS(SELECT 1 FROM " + builder.table + builder.buildClauses() + ")"
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
		return 0, fmt.Errorf(errTableCallBeforeOp)
	}

	query := "UPDATE " + r.Builder.table + " SET " + column + " = " + column + sign + strconv.FormatUint(on, 10)

	res, err := r.Sql().Exec(query)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// Chunk run queries by chinks by passing user-land function with an ability to stop execution when needed
// by returning false and proceed to execute queries when return true
func (r *DB) Chunk(amount int64, fn func(rows []map[string]interface{}) bool) error {
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
		res, err := r.Get()
		if err != nil {
			return err
		}
		fn(res) // execute all resulting records
		return nil
	}

	// executing chunks amount < cnt
	c := int64(math.Ceil(float64(cnt / amount)))
	var i int64
	for i = 0; i < c; i++ {
		rows, err := r.Offset(i * amount).Limit(amount).Get() // by 100 rows from 100 x n
		if err != nil {
			return err
		}
		res := fn(rows)
		if !res { // stop an execution when false returned by user
			break
		}
	}
	return nil
}
