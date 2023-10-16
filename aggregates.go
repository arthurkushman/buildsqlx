package buildsqlx

// Count counts resulting rows based on clause
func (r *DB) Count() (cnt int64, err error) {
	builder := r.Builder
	builder.columns = []string{"COUNT(*)"}
	query := builder.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&cnt)

	return
}

// Avg calculates average for specified column
func (r *DB) Avg(column string) (avg float64, err error) {
	builder := r.Builder
	builder.columns = []string{"AVG(" + column + ")"}
	query := builder.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&avg)

	return
}

// Min calculates minimum for specified column
func (r *DB) Min(column string) (min float64, err error) {
	builder := r.Builder
	builder.columns = []string{"MIN(" + column + ")"}
	query := builder.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&min)

	return
}

// Max calculates maximum for specified column
func (r *DB) Max(column string) (max float64, err error) {
	builder := r.Builder
	builder.columns = []string{"MAX(" + column + ")"}
	query := builder.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&max)

	return
}

// Sum calculates sum for specified column
func (r *DB) Sum(column string) (sum float64, err error) {
	builder := r.Builder
	builder.columns = []string{"SUM(" + column + ")"}
	query := builder.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&sum)

	return
}
