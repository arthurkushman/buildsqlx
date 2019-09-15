package buildsqlx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const TableToCreate = "big_tbl"

func TestDB_CreateTable(t *testing.T) {
	res, err := db.CreateTable(TableToCreate, func(table *Table) {
		table.Increments("id")
		table.String("title", 128).Default("The quick brown fox jumped over the lazy dog").Unique("idx_ttl")
		table.SmallInt("cnt").Default(1)
		table.Integer("points").NotNull()
		table.BigInt("likes").Index("idx_likes")
		table.Text("comment")
		table.DblPrecision("likes_to_points").Default(0.0)
	})
	assert.NoError(t, err)

	_, err = res.RowsAffected()
	assert.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	assert.NoError(t, err)
	assert.True(t, is)

	_, err = db.Drop(TableToCreate)
	assert.NoError(t, err)
}

func TestTable_BigIncrements(t *testing.T) {
	res, err := db.CreateTable(TableToCreate, func(table *Table) {
		table.BigIncrements("id")
		table.Numeric("price", 4, 3)
	})
	assert.NoError(t, err)

	_, err = res.RowsAffected()
	assert.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	assert.NoError(t, err)
	assert.True(t, is)

	_, err = db.Drop(TableToCreate)
	assert.NoError(t, err)
}
