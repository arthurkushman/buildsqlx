package buildsqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/fatih/structs"
	"github.com/lib/pq"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
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

type EachToStructFunc func(rows *sql.Rows) error

// Get builds all sql statements chained before and executes query collecting data to the slice
// Deprecated: this method will no longer be used in future releases, because of ScanStruct and EachToStruct replacement
func (r *DB) Get() ([]map[string]any, error) {
	bldr := r.Builder
	if bldr.table == "" {
		return nil, errTableCallBeforeOp
	}

	query := ""
	if len(bldr.union) > 0 { // got union - need different logic to glue
		for _, uBuilder := range bldr.union {
			query += uBuilder + " UNION "

			if bldr.isUnionAll {
				query += "ALL "
			}
		}

		query += bldr.buildSelect()
		// clean union (all) after ensuring selects are built
		r.Builder.union = []string{}
		r.Builder.isUnionAll = false
	} else { // std bldr
		query = bldr.buildSelect()
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

		err = rows.Scan(valuePtrs...)
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

// ScanStruct scans query into specific struct
func (r *DB) ScanStruct(src any) error {
	if reflect.ValueOf(src).IsNil() {
		return fmt.Errorf("cannot decode into nil type %T", src)
	}

	sqlBuilder := r.Builder
	if sqlBuilder.table == "" {
		return errTableCallBeforeOp
	}

	sqlBuilder.limit = 1
	query := ""
	if len(sqlBuilder.union) > 0 { // got union - need different logic to glue
		for _, uBuilder := range sqlBuilder.union {
			query += uBuilder + " UNION "

			if sqlBuilder.isUnionAll {
				query += "ALL "
			}
		}

		query += sqlBuilder.buildSelect()
		// clean union (all) after ensuring selects are built
		r.Builder.union = []string{}
		r.Builder.isUnionAll = false
	} else { // std builder
		query = sqlBuilder.buildSelect()
	}

	rows, err := r.Sql().Query(query, prepareValues(r.Builder.whereBindings)...)
	if err != nil {
		return err
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]any, count)
	valuePtrs := make([]any, count)

	// resource is the actual value that ptr points to.
	resource := reflect.ValueOf(src).Elem()
	if err = validateFields(resource, src, columns); err != nil {
		return err
	}

	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}

		for i, col := range columns {
			val := values[i]
			setResourceValue(resource, src, cases.Title(language.English).String(col), val)
		}

		src = resource
	}

	return nil
}

// EachToStruct scans query into specific struct per row with iterative behaviour
func (r *DB) EachToStruct(fn EachToStructFunc) error {
	sqlBuilder := r.Builder
	if sqlBuilder.table == "" {
		return errTableCallBeforeOp
	}

	query := ""
	if len(sqlBuilder.union) > 0 { // got union - need different logic to glue
		for _, uBuilder := range sqlBuilder.union {
			query += uBuilder + " UNION "

			if sqlBuilder.isUnionAll {
				query += "ALL "
			}
		}

		query += sqlBuilder.buildSelect()
		// clean union (all) after ensuring selects are built
		r.Builder.union = []string{}
		r.Builder.isUnionAll = false
	} else { // std builder
		query = sqlBuilder.buildSelect()
	}

	rows, err := r.Sql().Query(query, prepareValues(r.Builder.whereBindings)...)
	if err != nil {
		return err
	}

	for {
		err = fn(rows)
		if errors.Is(err, ErrNoMoreRows) {
			return nil
		}

		if err != nil {
			return err
		}
	}
}

// ErrNoMoreRows is returned by Next when there were no more rows
var ErrNoMoreRows = errors.New("sql: no more rows")

// Next will parse the next row into a struct passed as src parameter.
// Returns ErrNoMoreRows if there are no more row to parse
func (r *DB) Next(rows *sql.Rows, src any) error {
	if reflect.ValueOf(src).IsNil() {
		return fmt.Errorf("cannot decode into nil type %T", src)
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	count := len(columns)
	// resource is the actual value that ptr points to.
	resource := reflect.ValueOf(src).Elem()
	if err = validateFields(resource, src, columns); err != nil {
		return err
	}

	values := make([]any, count)
	valuePtrs := make([]any, count)
	if rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}

		for i, col := range columns {
			val := values[i]
			setResourceValue(resource, src, cases.Title(language.English).String(col), val)
		}
		src = resource

		return nil
	}

	return ErrNoMoreRows
}

func setResourceValue(resource reflect.Value, src any, col string, value any) {
	upperCaseFieldName := cases.Upper(language.English).String(col)
	if !resource.FieldByName(col).IsValid() && !resource.FieldByName(upperCaseFieldName).IsValid() { // try to get field by db: tag
		fields := structs.Fields(src)
		for i, f := range fields {
			tag := f.Tag("db")
			if tag == strings.ToLower(col) {
				setValue(resource.Field(i), value)
				return
			}
		}
	}

	colName := col
	if resource.FieldByName(upperCaseFieldName).IsValid() {
		colName = upperCaseFieldName
	}

	setValue(resource.FieldByName(colName), value)
}

func setValue(field reflect.Value, val any) {
	if field.Kind() == reflect.Ptr {
		newVal := reflect.New(field.Type().Elem())
		newVal.Elem().Set(reflect.ValueOf(val))
		field.Set(newVal)

		return
	}

	switch v := val.(type) {
	case string:
		field.SetString(v)
	case int:
		field.SetInt(int64(v))
	case int64:
		field.SetInt(v)
	case float64:
		field.SetFloat(v)
	case uint64:
		field.SetUint(v)
	case nil:
		field.SetPointer(nil)
	}

	if reflect.TypeOf(val).Kind() == reflect.Ptr {
		setValue(field, reflect.ValueOf(val).Elem().Interface())
	}
}

func validateFields(resource reflect.Value, src any, columns []string) error {
	for _, col := range columns {
		foundColByTag := false
		// standard fields parse
		fieldName := cases.Title(language.English).String(col)
		// uppercase letters fields parse e.g.: ID, URL etc
		upperCaseFieldName := cases.Upper(language.English).String(col)
		if !resource.FieldByName(fieldName).IsValid() && !resource.FieldByName(upperCaseFieldName).IsValid() {
			fields := structs.Fields(src)
			for _, f := range fields {
				tag := f.Tag("db")
				if tag == col {
					foundColByTag = true
					break
				}
			}

			if !foundColByTag {
				return fmt.Errorf("field %s not found in struct", fieldName)
			}
		}
	}

	return nil
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

// Insert inserts one row with param bindings for struct
func (r *DB) Insert(data any) error {
	if r.Txn != nil {
		return r.Txn.Insert(data)
	}

	bldr := r.Builder
	if bldr.table == "" {
		return errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindingsForStruct(data)

	query := `INSERT INTO "` + bldr.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `)`

	_, err := r.Sql().Exec(query, values...)
	if err != nil {
		return err
	}

	return nil
}

// Insert inserts one row with param bindings from struct
// in transaction context
func (r *Txn) Insert(data any) error {
	if r.Tx == nil {
		return errTransactionModeWithoutTx
	}

	bldr := r.Builder
	if bldr.table == "" {
		return errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindingsForStruct(data)

	query := `INSERT INTO "` + bldr.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `)`

	_, err := r.Tx.Exec(query, values...)
	if err != nil {
		return err
	}

	return nil
}

// InsertGetId inserts one row with param bindings and returning id
func (r *DB) InsertGetId(data any) (uint64, error) {
	if r.Txn != nil {
		return r.Txn.InsertGetId(data)
	}

	bldr := r.Builder
	if bldr.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindingsForStruct(data)

	query := `INSERT INTO "` + bldr.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `) RETURNING id`

	var id uint64
	err := r.Sql().QueryRow(query, values...).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

// InsertGetId inserts one row with param bindings and returning id
// in transaction context
func (r *Txn) InsertGetId(data any) (uint64, error) {
	if r.Tx == nil {
		return 0, errTransactionModeWithoutTx
	}

	bldr := r.Builder
	if bldr.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindingsForStruct(data)

	query := `INSERT INTO "` + bldr.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `) RETURNING id`

	var id uint64
	err := r.Tx.QueryRow(query, values...).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

func prepareValuesForStruct(value reflect.Value) []any {
	var values []any
	switch value.Kind() {
	case reflect.String:
		values = append(values, value.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		values = append(values, strconv.FormatInt(value.Int(), 10))
	case reflect.Float32, reflect.Float64:
		values = append(values, fmt.Sprintf("%g", value.Float()))
	case reflect.Ptr:
		if value.IsNil() {
			values = append(values, nil)
		} else {
			values = prepareValuesForStruct(value.Elem())
		}
	}

	return values
}

func prepareValue(value any) []any {
	var values []any
	switch v := value.(type) {
	case string:
		values = append(values, v)
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

// prepareBindingsForStruct prepares all bindings for SQL-query
func prepareBindingsForStruct(data any) (columns []string, values []any, bindings []string) {
	j := 1
	resource := reflect.ValueOf(data)
	t := reflect.TypeOf(data)
	for i := 0; i < t.NumField(); i++ {
		value := resource.Field(i)
		col := getColumn(t.Field(i))

		if strings.Contains(col, sqlOperatorIs) || strings.Contains(col, sqlOperatorBetween) {
			continue
		}

		columns = append(columns, col)
		pValues := prepareValuesForStruct(value)
		if len(pValues) > 0 {
			values = append(values, pValues...)

			for range pValues {
				bindings = append(bindings, "$"+strconv.FormatInt(int64(j), 10))
				j++
			}
		}
	}

	return
}

// getColumn gets column name and value
func getColumn(structField reflect.StructField) string {
	col := strings.ToLower(structField.Name)
	if structField.Tag.Get("db") != "" {
		col = structField.Tag.Get("db")
	}

	return col
}

// InsertBatch inserts multiple rows based on transaction
func (r *DB) InsertBatch(data any) error {
	bldr := r.Builder
	if bldr.table == "" {
		return errTableCallBeforeOp
	}

	txn, err := r.Sql().Begin()
	if err != nil {
		log.Fatal(err)
	}

	iSlice := anySlice(data)
	columns, values := prepareInsertBatchForStructs(iSlice)

	stmt, err := txn.Prepare(pq.CopyIn(bldr.table, columns...))
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

// prepareInsertBatchForStructs prepares the column names and values for inserting multiple structs into a database table.
//
// It takes in a slice of any structs called data and returns two slices: columns and values.
// The columns slice contains the column names, while the values slice contains the corresponding values for each struct.
func prepareInsertBatchForStructs[T any](data []T) (columns []string, values [][]interface{}) {
	values = make([][]interface{}, len(data))
	colToIdx := make(map[string]int)

	i := 0
	for k, v := range data {
		values[k] = make([]interface{}, 0)

		structValue := reflect.ValueOf(v)
		structType := structValue.Type()

		for j := 0; j < structValue.NumField(); j++ {
			fieldValue := structValue.Field(j)
			fieldType := structType.Field(j)

			columnName := strings.ToLower(fieldType.Name)
			if fieldType.Tag.Get("db") != "" {
				columnName = fieldType.Tag.Get("db")
			}
			columnValue := fieldValue.Interface()

			if k == 0 {
				columns = append(columns, columnName)
				colToIdx[columnName] = i
				i++
			}

			values[k] = append(values[k], columnValue)
		}
	}

	return
}

// anySlice converts a slice of any type to a slice of interface{} type.
//
// It takes a slice as input and returns a new slice where each element is
// converted to the interface{} type. The input slice can be nil or empty,
// in which case the function returns nil.
//
// Parameters:
// - slice: The input slice to be converted.
//
// Return:
//   - []interface{}: The converted slice where each element is of type
//     interface{}.
func anySlice(slice any) []any {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		return nil
	}

	// Keep the distinction between nil and empty slice input
	if s.IsNil() {
		return nil
	}

	ret := make([]any, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}

// Update builds an UPDATE sql stmt with corresponding where/from clauses if stated
// returning affected rows
func (r *DB) Update(data any) (int64, error) {
	if r.Txn != nil {
		return r.Txn.Update(data)
	}

	bldr := r.Builder
	if bldr.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindingsForStruct(data)
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
func (r *Txn) Update(data any) (int64, error) {
	if r.Tx == nil {
		return 0, errTransactionModeWithoutTx
	}

	bldr := r.Builder
	if bldr.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindingsForStruct(data)
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

	bldr := r.Builder
	if bldr.table == "" {
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

	bldr := r.Builder
	if bldr.table == "" {
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
func (r *DB) Replace(data any, conflict string) (int64, error) {
	if r.Txn != nil {
		return r.Txn.Replace(data, conflict)
	}

	bldr := r.Builder
	if bldr.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindingsForStruct(data)
	query := `INSERT INTO "` + bldr.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `) ON CONFLICT(` + conflict + `) DO UPDATE SET `
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
func (r *Txn) Replace(data any, conflict string) (int64, error) {
	if r.Tx == nil {
		return 0, errTransactionModeWithoutTx
	}

	bldr := r.Builder
	if bldr.table == "" {
		return 0, errTableCallBeforeOp
	}

	columns, values, bindings := prepareBindingsForStruct(data)
	query := `INSERT INTO "` + bldr.table + `" (` + strings.Join(columns, `, `) + `) VALUES(` + strings.Join(bindings, `, `) + `) ON CONFLICT(` + conflict + `) DO UPDATE SET `
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
