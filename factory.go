package buildsqlx

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

const (
	plusSign  = "+"
	minusSign = "-"
)

var (
	// Custom errors
	errTableCallBeforeOp        = fmt.Errorf("sql: there was no Table() call with table name set")
	errTransactionModeWithoutTx = fmt.Errorf("sql: there was no *sql.Tx object set properly")
)

// Get builds all sql statements chained before and executes query collecting data to the slice
func (r *DB) Get() ([]map[string]any, error) {
	builder := r.Builder
	if builder.table == "" {
		return nil, errTableCallBeforeOp
	}

	query := ""
	if len(builder.union) > 0 { // got union - need different logic to glue
		for _, uBuilder := range builder.union {
			query += uBuilder + " UNION "

			if builder.isUnionAll {
				query += "ALL "
			}
		}

		query += builder.buildSelect()
		// clean union (all) after ensuring selects are built
		r.Builder.union = []string{}
		r.Builder.isUnionAll = false
	} else { // std builder
		query = builder.buildSelect()
	}

	rows, err := r.Sql().Query(query, prepareValues(r.Builder.whereBindings)...)
	if err != nil {
		return nil, err
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]any, count)
	valuePtrs := make([]any, count)

	// collecting data from struct with fields
	var res []map[string]any

	for rows.Next() {
		collect := make(map[string]any, count)

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

func prepareValues(values []map[string]any) []any {
	var vls []any
	for _, v := range values {
		_, vals, _ := prepareBindings(v)
		vls = append(vls, vals...)
	}
	return vls
}

// buildSelect constructs a query for select statement
func (r *builder) buildSelect() string {
	query := `SELECT ` + strings.Join(r.columns, `, `) + ` FROM "` + r.table + `"`

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
		clauses += composeWhere(r.whereBindings, r.startBindingsAt)
	} else { // std without bindings todo: change all to bindings
		clauses += r.where
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
func composeWhere(whereBindings []map[string]any, startedAt int) string {
	where := " WHERE "
	i := startedAt
	for _, m := range whereBindings {
		for k, v := range m {
			// operand >= $i
			switch vi := v.(type) {
			case []any:
				placeholders := make([]string, 0, len(vi))
				for range vi {
					placeholders = append(placeholders, "$"+strconv.Itoa(i))
					i++
				}
				where += k + " (" + strings.Join(placeholders, ", ") + ")"
			default:
				if strings.Contains(k, sqlOperatorIs) || strings.Contains(k, sqlOperatorBetween) {
					where += k + " " + vi.(string)
					break
				}

				where += k + " $" + strconv.Itoa(i)
				i++
			}
		}
	}
	return where
}

// composers ORDER BY clause string for particular query stmt
func composeOrderBy(orderBy []map[string]string, orderByRaw *string) string {
	if len(orderBy) > 0 {
		orderStr := ""
		for _, m := range orderBy {
			for field, direct := range m {
				if orderStr == "" {
					orderStr = " ORDER BY " + field + " " + direct
				} else {
					orderStr += ", " + field + " " + direct
				}
			}
		}
		return orderStr
	} else if orderByRaw != nil {
		return " ORDER BY " + *orderByRaw
	}
	return ""
}

// Insert inserts one row with param bindings
func (r *DB) Insert(data map[string]any) error {
	if r.Txn != nil {
		return r.Txn.Insert(data)
	}

	builder := r.Builder
	if builder.table == "" {
		return errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindings(data)

	query := `INSERT INTO "` + builder.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `)`

	_, err := r.Sql().Exec(query, values...)

	if err != nil {
		return err
	}

	return nil
}

// Insert inserts one row with param bindings
func (r *Txn) Insert(data map[string]any) error {
	if r.Tx == nil {
		return errTransactionModeWithoutTx
	}

	builder := r.Builder
	if builder.table == "" {
		return errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindings(data)

	query := `INSERT INTO "` + builder.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `)`

	_, err := r.Tx.Exec(query, values...)

	if err != nil {
		return err
	}

	return nil
}

// InsertGetId inserts one row with param bindings and returning id
func (r *DB) InsertGetId(data map[string]any) (uint64, error) {
	if r.Txn != nil {
		return r.Txn.InsertGetId(data)
	}

	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindings(data)

	query := `INSERT INTO "` + builder.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `) RETURNING id`

	var id uint64
	err := r.Sql().QueryRow(query, values...).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

// InsertGetId inserts one row with param bindings and returning id
func (r *Txn) InsertGetId(data map[string]any) (uint64, error) {
	if r.Tx == nil {
		return 0, errTransactionModeWithoutTx
	}

	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindings(data)

	query := `INSERT INTO "` + builder.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `) RETURNING id`

	var id uint64
	err := r.Tx.QueryRow(query, values...).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

func prepareValue(value any) []any {
	var values []any
	switch v := value.(type) {
	case string:
		//if where { // todo: left comments for further exploration, probably incorrect behaviour for pg driver
		//	values = append(values, "'"+v+"'")
		//} else {
		values = append(values, v)
		//}
	case int:
		values = append(values, strconv.FormatInt(int64(v), 10))
	case float64:
		values = append(values, fmt.Sprintf("%g", v))
	case int64:
		values = append(values, strconv.FormatInt(v, 10))
	case uint64:
		values = append(values, strconv.FormatUint(v, 10))
	case []any:
		for _, vi := range v {
			values = append(values, prepareValue(vi)...)
		}
	case nil:
		values = append(values, nil)
	}

	return values
}

// prepareBindings prepares slices to split in favor of INSERT sql statement
func prepareBindings(data map[string]any) (columns []string, values []any, bindings []string) {
	i := 1
	for column, value := range data {
		if strings.Contains(column, sqlOperatorIs) || strings.Contains(column, sqlOperatorBetween) {
			continue
		}

		columns = append(columns, column)
		pValues := prepareValue(value)
		if len(pValues) > 0 {
			values = append(values, pValues...)

			for range pValues {
				bindings = append(bindings, "$"+strconv.FormatInt(int64(i), 10))
				i++
			}
		}
	}

	return
}

// InsertBatch inserts multiple rows based on transaction
func (r *DB) InsertBatch(data []map[string]any) error {
	builder := r.Builder
	if builder.table == "" {
		return errTableCallBeforeOp
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
func prepareInsertBatch(data []map[string]any) (columns []string, values [][]any) {
	values = make([][]any, len(data))
	colToIdx := make(map[string]int)

	i := 0
	for k, v := range data {
		values[k] = make([]any, len(v))

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
			case int:
				values[k][colToIdx[column]] = strconv.FormatInt(int64(casted), 10)
			case float64:
				values[k][colToIdx[column]] = fmt.Sprintf("%g", casted)
			case int64:
				values[k][colToIdx[column]] = strconv.FormatInt(casted, 10)
			case uint64:
				values[k][colToIdx[column]] = strconv.FormatUint(casted, 10)
			}
		}
	}

	return
}

// Update builds an UPDATE sql stmt with corresponding where/from clauses if stated
// returning affected rows
func (r *DB) Update(data map[string]any) (int64, error) {
	if r.Txn != nil {
		return r.Txn.Update(data)
	}

	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindings(data)
	setVal := ""
	l := len(columns)
	for k, col := range columns {
		setVal += col + " = " + bindings[k]
		if k < l-1 {
			setVal += ", "
		}
	}

	query := `UPDATE "` + r.Builder.table + `" SET ` + setVal
	if r.Builder.from != "" {
		query += " FROM " + r.Builder.from
	}

	r.Builder.startBindingsAt = l + 1
	query += r.Builder.buildClauses()
	values = append(values, prepareValues(r.Builder.whereBindings)...)
	res, err := r.Sql().Exec(query, values...)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// Update builds an UPDATE sql stmt with corresponding where/from clauses if stated
// returning affected rows
func (r *Txn) Update(data map[string]any) (int64, error) {
	if r.Tx == nil {
		return 0, errTransactionModeWithoutTx
	}

	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindings(data)
	setVal := ""
	l := len(columns)
	for k, col := range columns {
		setVal += col + " = " + bindings[k]
		if k < l-1 {
			setVal += ", "
		}
	}

	query := `UPDATE "` + r.Builder.table + `" SET ` + setVal
	if r.Builder.from != "" {
		query += " FROM " + r.Builder.from
	}

	r.Builder.startBindingsAt = l + 1
	query += r.Builder.buildClauses()
	values = append(values, prepareValues(r.Builder.whereBindings)...)
	res, err := r.Tx.Exec(query, values...)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// Delete builds a DELETE stmt with corresponding where clause if stated
// returning affected rows
func (r *DB) Delete() (int64, error) {
	if r.Txn != nil {
		return r.Txn.Delete()
	}

	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	query := `DELETE FROM "` + r.Builder.table + `"`
	query += r.Builder.buildClauses()
	res, err := r.Sql().Exec(query, prepareValues(r.Builder.whereBindings)...)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// Delete builds a DELETE stmt with corresponding where clause if stated
// returning affected rows
func (r *Txn) Delete() (int64, error) {
	if r.Tx == nil {
		return 0, errTransactionModeWithoutTx
	}

	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	query := `DELETE FROM "` + r.Builder.table + `"`
	query += r.Builder.buildClauses()
	res, err := r.Tx.Exec(query, prepareValues(r.Builder.whereBindings)...)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// Replace inserts data if conflicting row hasn't been found, else it will update an existing one
func (r *DB) Replace(data map[string]any, conflict string) (int64, error) {
	if r.Txn != nil {
		return r.Txn.Replace(data, conflict)
	}

	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindings(data)
	query := `INSERT INTO "` + builder.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `) ON CONFLICT(` + conflict + `) DO UPDATE SET `
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

// Replace inserts data if conflicting row hasn't been found, else it will update an existing one
func (r *Txn) Replace(data map[string]any, conflict string) (int64, error) {
	if r.Tx == nil {
		return 0, errTransactionModeWithoutTx
	}

	builder := r.Builder
	if builder.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindings(data)
	query := `INSERT INTO "` + builder.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `) ON CONFLICT(` + conflict + `) DO UPDATE SET `
	for i, v := range columns {
		columns[i] = v + " = excluded." + v
	}

	query += strings.Join(columns, ", ")
	res, err := r.Tx.Exec(query, values...)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// InTransaction executes fn passed as an argument in transaction mode
// if there are no results returned - txn will be rolled back, otherwise committed and returned
func (r *DB) InTransaction(fn func() (any, error)) error {
	txn, err := r.Sql().Begin()
	if err != nil {
		return err
	}

	// assign transaction + builder to Txn entity
	r.Txn = &Txn{
		Tx:      txn,
		Builder: r.Builder,
	}

	defer func() {
		// clear Txn object after commit
		r.Txn = nil
	}()
	res, err := fn()
	if err != nil {
		errTxn := txn.Rollback()
		if errTxn != nil {
			return errTxn
		}
		return err
	}

	isOk := false
	switch v := res.(type) {
	case int:
		if v > 0 {
			isOk = true
		}
	case int64:
		if v > 0 {
			isOk = true
		}
	case uint64:
		if v > 0 {
			isOk = true
		}
	case []map[string]any:
		if len(v) > 0 {
			isOk = true
		}
	case map[string]any:
		if len(v) > 0 {
			isOk = true
		}
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
