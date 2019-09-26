package buildsqlx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const TableToCreate = "big_tbl"

func TestDB_CreateTable(t *testing.T) {
	_, err := db.DropIfExists(TableToCreate)
	assert.NoError(t, err)

	_, err = db.Schema(TableToCreate, func(table *Table) {
		table.Increments("id")
		table.String("title", 128).Default("The quick brown fox jumped over the lazy dog").Unique("idx_ttl")
		table.SmallInt("cnt").Default(1)
		table.Integer("points").NotNull()
		table.BigInt("likes").Index("idx_likes")
		table.Text("comment").Comment("user comment")
		table.DblPrecision("likes_to_points").Default(0.0)
		table.Decimal("tax", 2, 2)
		table.TsVector("body")
		table.TsQuery("body_query")
		table.Point("pt")
		table.Polygon("poly")
		table.TableComment("big table for big data")
	})
	assert.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	assert.NoError(t, err)
	assert.True(t, is)

	_, err = db.Schema("tbl_to_ref", func(table *Table) {
		table.Increments("id")
		table.Integer("big_tbl_id").ForeignKey("fk_idx_big_tbl_id", TableToCreate, "id")
	})
	assert.NoError(t, err)

	// 1st drop the referencing tbl
	_, err = db.Drop("tbl_to_ref")
	assert.NoError(t, err)
	// then referenced
	_, err = db.Drop(TableToCreate)
	assert.NoError(t, err)
}

func TestTable_BigIncrements(t *testing.T) {
	_, err := db.DropIfExists(TableToCreate)
	assert.NoError(t, err)

	res, err := db.Schema(TableToCreate, func(table *Table) {
		table.BigIncrements("id")
		table.Numeric("price", 4, 3)
		table.Jsonb("taxes")
	})
	assert.NoError(t, err)

	_, err = res.RowsAffected()
	assert.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	assert.NoError(t, err)
	assert.True(t, is)

	// test add the column
	res, err = db.Schema(TableToCreate, func(table *Table) {
		table.String("title", 64)
	})
	assert.NoError(t, err)

	isCol, err := db.HasColumns("public", TableToCreate, "title")
	assert.NoError(t, err)
	assert.True(t, isCol)

	// test modify the column
	res, err = db.Schema(TableToCreate, func(table *Table) {
		table.String("title", 128).Change()
	})
	assert.NoError(t, err)

	// test drop the column
	res, err = db.Schema(TableToCreate, func(table *Table) {
		table.DropColumn("title")
	})
	assert.NoError(t, err)

	isCol, err = db.HasColumns("public", TableToCreate, "title")
	assert.NoError(t, err)
	assert.False(t, isCol)

	_, err = db.Drop(TableToCreate)
	assert.NoError(t, err)
}

func TestTable_DateTime(t *testing.T) {
	_, err := db.DropIfExists(TableToCreate)
	assert.NoError(t, err)

	_, err = db.Schema(TableToCreate, func(table *Table) {
		table.Increments("id")
		table.Json("settings")
		table.Char("tag", 10)
		table.Date("birthday", false)
		table.DateTime("created_at", true)
		table.DateTimeTz("updated_at", true)
	})
	assert.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	assert.NoError(t, err)
	assert.True(t, is)

	_, err = db.Drop(TableToCreate)
	assert.NoError(t, err)
}
