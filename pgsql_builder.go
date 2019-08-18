package arsqlx

import (
	"fmt"
	"strconv"
	"strings"
)

func (r *DB) Get(p interface{}) ([]map[string]interface{}, error) {
	builder := r.Builder
	if builder.table == "" {
		return nil, fmt.Errorf("sql: there was no Table() call with table name set")
	}

	rows, err := r.Sql().Query(builder.buildSelect())
	if err != nil {
		return nil, err
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	// collecting data from struct with fields
	var res []map[string]interface{}

	collect := make(map[string]interface{})
	for rows.Next() {
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

// buildSelect constructs a query for select statement
func (r *Builder) buildSelect() string {
	query := "SELECT " + strings.Join(r.columns, ", ") + " FROM " + r.table

	// build where clause
	if r.where != "" {
		query += " WHERE " + r.where
	}

	if r.groupBy != "" {
		query += " GROUP BY " + r.groupBy
	}

	if r.having != "" {
		query += " HAVING " + r.having
	}

	if len(r.orderBy) > 0 {
		orderStr := ""
		for field, direct := range r.orderBy {
			if orderStr == "" {
				orderStr = " ORDER BY " + field + " " + direct
			} else {
				orderStr += ", " + field + " " + direct
			}
		}

		query += orderStr
	}

	if r.limit > 0 {
		query += " LIMIT " + strconv.FormatInt(r.limit, 10)
	}

	if r.offset > 0 {
		query += " OFFSET " + strconv.FormatInt(r.offset, 10)
	}

	return query
}

func (r *DB) Insert(data map[string]interface{}) error {
	builder := r.Builder
	if builder.table == "" {
		return fmt.Errorf("sql: there was no Table() call with table name set")
	}

	columns, values, bindings := prepareInsert(data)

	query := "INSERT INTO " + builder.table + " (" + strings.Join(columns, ", ") + ") VALUES(" + strings.Join(bindings, ", ") + ")"

	_, err := r.Sql().Exec(query, values...)

	if err != nil {
		return err
	}

	return nil
}

func (r *DB) InsertGetId(data map[string]interface{}) (uint64, error) {
	builder := r.Builder
	if builder.table == "" {
		return 0, fmt.Errorf("sql: there was no Table() call with table name set")
	}

	columns, values, bindings := prepareInsert(data)

	query := "INSERT INTO " + builder.table + " (" + strings.Join(columns, ", ") + ") VALUES(" + strings.Join(bindings, ", ") + ") RETURNING id"

	var id uint64
	err := r.Sql().QueryRow(query, values...).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

// prepareInsert prepares slices to split in favor of INSERT sql statement
func prepareInsert(data map[string]interface{}) (columns []string, values []interface{}, bindings []string) {

	i := 1
	for column, value := range data {
		columns = append(columns, column)

		switch v := value.(type) {
		case string:
			values = append(values, "'"+v+"'")
			break
		case int:
			values = append(values, strconv.FormatInt(int64(v), 10))
			break
		case float64:
			values = append(values, fmt.Sprintf("%g", v))
			break
		}

		bindings = append(bindings, "$"+strconv.FormatInt(int64(i), 10))
		i++
	}

	return
}
