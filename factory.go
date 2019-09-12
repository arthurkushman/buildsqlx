package buildsqlx

import (
	"fmt"
	"github.com/lib/pq"
	"log"
	"strconv"
	"strings"
)

const (
	PlusSign  = "+"
	MinusSign = "-"
	// Errors
	ErrTableCallBeforeOp = "sql: there was no Table() call with table name set"
)

// Get builds all sql statements chained before and executes query collecting data to the slice
func (r *DB) Get() ([]map[string]interface{}, error) {
	builder := r.Builder
	if builder.table == "" {
		return nil, fmt.Errorf(ErrTableCallBeforeOp)
	}

	query := ""
	if len(builder.union) > 0 { // got union - need different logic to glue
		for _, uBuilder := range builder.union {
			query += uBuilder + " UNION "

			if builder.isUnionAll == true {
				query += "ALL "
			}
		}

		query += builder.buildSelect()
	} else { // std builder
		query = builder.buildSelect()
	}

	rows, err := r.Sql().Query(query, prepareValues(r.Builder.whereBindings)...)
	if err != nil {
		return nil, err
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	// collecting data from struct with fields
	var res []map[string]interface{}

	for rows.Next() {
		collect := make(map[string]interface{}, count)

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)

		if err != nil {
			return nil, err
		}

		for i, col := range columns {
			val := values[i]

			b, ok := val.([]byte)
			if ok {
				collect[col] = string(b)
			} else {
				collect[col] = val
			}
		}

		res = append(res, collect)
	}

	return res, nil
}

func prepareValues(values []map[string]interface{}) []interface{} {
	var vls []interface{}
	for _, v := range values {
		_, vals, _ := prepareBindings(v, true)
		vls = append(vls, vals...)
	}
	return vls
}

// buildSelect constructs a query for select statement
func (r *builder) buildSelect() string {
	query := "SELECT " + strings.Join(r.columns, ", ") + " FROM " + r.table

	return query + r.buildClauses()
}

// builds query string clauses
func (r *builder) buildClauses() string {
	clauses := ""
	for _, j := range r.join {
		clauses += j
	}

	// build where clause
	if len(r.whereBindings) > 0 {
		clauses += composeWhere(r.whereBindings)
	}

	if r.groupBy != "" {
		clauses += " GROUP BY " + r.groupBy
	}

	if r.having != "" {
		clauses += " HAVING " + r.having
	}

	clauses += composeOrderBy(r.orderBy, r.orderByRaw)

	if r.limit > 0 {
		clauses += " LIMIT " + strconv.FormatInt(r.limit, 10)
	}

	if r.offset > 0 {
		clauses += " OFFSET " + strconv.FormatInt(r.offset, 10)
	}

	if r.lockForUpdate != nil {
		clauses += *r.lockForUpdate
	}

	return clauses
}

// composes WHERE clause string for particular query stmt
func composeWhere(whereBindings []map[string]interface{}) string {
	where := " WHERE "
	i := 1
	for _, m := range whereBindings {
		for k := range m {
			// operand >= $i
			where += k + " $" + strconv.Itoa(i)
			i++
		}
	}
	return where
}

// composers ORDER BY clause string for particular query stmt
func composeOrderBy(orderBy map[string]string, orderByRaw *string) string {
	if len(orderBy) > 0 {
		orderStr := ""
		for field, direct := range orderBy {
			if orderStr == "" {
				orderStr = " ORDER BY " + field + " " + direct
			} else {
				orderStr += ", " + field + " " + direct
			}
		}
		return orderStr
	} else if orderByRaw != nil {
		return " ORDER BY " + *orderByRaw
	}
	return ""
}

// Insert inserts one row with param bindings
func (r *DB) Insert(data map[string]interface{}) error {
	builder := r.Builder
	if builder.table == "" {
		return fmt.Errorf(ErrTableCallBeforeOp)
	}

	columns, values, bindings := prepareBindings(data, false)

	query := "INSERT INTO " + builder.table + " (" + strings.Join(columns, ", ") + ") VALUES(" + strings.Join(bindings, ", ") + ")"

	_, err := r.Sql().Exec(query, values...)

	if err != nil {
		return err
	}

	return nil
}

// InsertGetId inserts one row with param bindings and returning id
func (r *DB) InsertGetId(data map[string]interface{}) (uint64, error) {
	builder := r.Builder
	if builder.table == "" {
		return 0, fmt.Errorf(ErrTableCallBeforeOp)
	}

	columns, values, bindings := prepareBindings(data, false)

	query := "INSERT INTO " + builder.table + " (" + strings.Join(columns, ", ") + ") VALUES(" + strings.Join(bindings, ", ") + ") RETURNING id"

	var id uint64
	err := r.Sql().QueryRow(query, values...).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

// prepareBindings prepares slices to split in favor of INSERT sql statement
func prepareBindings(data map[string]interface{}, where bool) (columns []string, values []interface{}, bindings []string) {

	i := 1
	for column, value := range data {
		columns = append(columns, column)

		switch v := value.(type) {
		case string:
			if where {
				values = append(values, "'"+v+"'")
			} else {
				values = append(values, v)
			}
			break
		case int:
			values = append(values, strconv.FormatInt(int64(v), 10))
			break
		case float64:
			values = append(values, fmt.Sprintf("%g", v))
			break
		case int64:
			values = append(values, strconv.FormatInt(v, 10))
			break
		case uint64:
			values = append(values, strconv.FormatUint(v, 10))
			break
		}

		bindings = append(bindings, "$"+strconv.FormatInt(int64(i), 10))
		i++
	}

	return
}

// InsertBatch inserts multiple rows based on transaction
func (r *DB) InsertBatch(data []map[string]interface{}) error {
	builder := r.Builder
	if builder.table == "" {
		return fmt.Errorf(ErrTableCallBeforeOp)
	}

	txn, err := r.Sql().Begin()
	if err != nil {
		log.Fatal(err)
	}

	columns, values := prepareInsertBatch(data)

	stmt, err := txn.Prepare(pq.CopyIn(builder.table, columns...))
	if err != nil {
		return err
	}

	for _, value := range values {
		_, err = stmt.Exec(value...)
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

// prepareInsertBatch prepares slices to split in favor of INSERT sql statement
func prepareInsertBatch(data []map[string]interface{}) (columns []string, values [][]interface{}) {
	values = make([][]interface{}, len(data))
	colToIdx := make(map[string]int)

	i := 0
	for k, v := range data {
		values[k] = make([]interface{}, len(v))

		for column, value := range v {
			if k == 0 {
				columns = append(columns, column)
				// todo: don't know yet how to match them explicitly (it is bad idea, but it works well now)
				colToIdx[column] = i
				i++
			}

			switch casted := value.(type) {
			case string:
				values[k][colToIdx[column]] = casted
				break
			case int:
				values[k][colToIdx[column]] = strconv.FormatInt(int64(casted), 10)
				break
			case float64:
				values[k][colToIdx[column]] = fmt.Sprintf("%g", casted)
				break
			case int64:
				values[k][colToIdx[column]] = strconv.FormatInt(casted, 10)
				break
			case uint64:
				values[k][colToIdx[column]] = strconv.FormatUint(casted, 10)
				break
			}
		}
	}

	return
}

// Update builds an UPDATE sql stmt with corresponding where/from clauses if stated
// returning affected rows
func (r *DB) Update(data map[string]interface{}) (int64, error) {
	builder := r.Builder
	if builder.table == "" {
		return 0, fmt.Errorf(ErrTableCallBeforeOp)
	}

	columns, values, bindings := prepareBindings(data, false)

	setVal := ""
	l := len(columns)
	for k, col := range columns {
		setVal += col + " = " + bindings[k]
		if k < l-1 {
			setVal += ", "
		}
	}

	query := "UPDATE " + r.Builder.table + " SET " + setVal

	if r.Builder.from != "" {
		query += " FROM " + r.Builder.from
	}

	if r.Builder.where != "" {
		query += " WHERE " + r.Builder.where
	}

	res, err := r.Sql().Exec(query, values...)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// Delete builds a DELETE stmt with corresponding where clause if stated
// returning affected rows
func (r *DB) Delete() (int64, error) {
	builder := r.Builder
	if builder.table == "" {
		return 0, fmt.Errorf(ErrTableCallBeforeOp)
	}

	query := "DELETE FROM " + r.Builder.table

	if r.Builder.where != "" {
		query += " WHERE " + r.Builder.where
	}

	res, err := r.Sql().Exec(query)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// Replace inserts data if conflicting row hasn't been found, else it will update an existing one
func (r *DB) Replace(data map[string]interface{}, conflict string) (int64, error) {
	builder := r.Builder
	if builder.table == "" {
		return 0, fmt.Errorf(ErrTableCallBeforeOp)
	}

	columns, values, bindings := prepareBindings(data, false)

	query := "INSERT INTO " + builder.table + " (" + strings.Join(columns, ", ") + ") VALUES(" + strings.Join(bindings, ", ") + ") ON CONFLICT(" + conflict + ") DO UPDATE SET "

	for i, v := range columns {
		columns[i] = v + " = excluded." + v
	}

	query += strings.Join(columns, ", ")

	res, err := r.Sql().Exec(query, values...)

	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// InTransaction executes fn passed as an argument in transaction mode
// if there are no results returned - txn will be rolled back, otherwise committed and returned
func (r *DB) InTransaction(fn func() (interface{}, error)) error {
	txn, err := r.Sql().Begin()

	res, err := fn()
	if err != nil {
		return err
	}

	isOk := false
	switch v := res.(type) {
	case int64:
		if v > 0 {
			isOk = true
		}
		break
	case uint64:
		if v > 0 {
			isOk = true
		}
		break
	case []map[string]interface{}:
		if len(v) > 0 {
			isOk = true
		}
		break
	case map[string]interface{}:
		if len(v) > 0 {
			isOk = true
		}
		break
	}

	if !isOk {
		return txn.Rollback()
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}
