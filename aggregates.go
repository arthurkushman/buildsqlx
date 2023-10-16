package buildsqlx

// Count counts resulting rows based on clause
func (r *DB) Count() (cnt int64, err error) {
	bldr := r.Builder
	bldr.columns = []string{"COUNT(*)"}
	query := bldr.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&cnt)

	return
}

// Avg calculates average for specified column
func (r *DB) Avg(column string) (avg float64, err error) {
	bldr := r.Builder
	bldr.columns = []string{"AVG(" + column + ")"}
	query := bldr.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&avg)

	return
}

// Min calculates minimum for specified column
func (r *DB) Min(column string) (min float64, err error) {
	bldr := r.Builder
	bldr.columns = []string{"MIN(" + column + ")"}
	query := bldr.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&min)

	return
}

// Max calculates maximum for specified column
func (r *DB) Max(column string) (max float64, err error) {
	bldr := r.Builder
	bldr.columns = []string{"MAX(" + column + ")"}
	query := bldr.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&max)

	return
}

// Sum calculates sum for specified column
func (r *DB) Sum(column string) (sum float64, err error) {
	bldr := r.Builder
	bldr.columns = []string{"SUM(" + column + ")"}
	query := bldr.buildSelect()
	err = r.Sql().QueryRow(query, prepareValues(r.Builder.whereBindings)...).Scan(&sum)

	return
}
