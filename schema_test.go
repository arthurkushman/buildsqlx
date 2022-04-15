package buildsqlx

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

const TableToCreate = "big_tbl"

func TestDB_CreateTable(t *testing.T) {
	_, err := db.DropIfExists(TableToCreate)
	assert.NoError(t, err)

	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.Increments("id")
		table.String("title", 128).Default("The quick brown fox jumped over the lazy dog").Unique("idx_ttl")
		table.Boolean("is_active")
		table.SmallInt("cnt").Default(1)
		table.Integer("points").NotNull()
		table.BigInt("likes").Index("idx_likes")
		table.Text("comment").Comment("user comment").Collation("de-LU-x-icu")
		table.DblPrecision("likes_to_points").Default(0.0)
		table.Decimal("tax", 2, 2)
		table.TsVector("body")
		table.TsQuery("body_query")
		table.Point("pt")
		table.Polygon("poly")
		table.TableComment("big table for big data")

		return nil
	})
	assert.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	assert.NoError(t, err)
	assert.True(t, is)

	_, err = db.Schema("tbl_to_ref", func(table *Table) error {
		table.Increments("id")
		table.Integer("big_tbl_id").ForeignKey("fk_idx_big_tbl_id", TableToCreate, "id")

		return nil
	})
	assert.NoError(t, err)

	// test some err returning from fn()
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		return errors.New("some err")
	})
	assert.Error(t, err)

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

	res, err := db.Schema(TableToCreate, func(table *Table) error {
		table.BigIncrements("id")
		table.Numeric("price", 4, 3).Index("idx_price")
		table.Jsonb("taxes")

		return nil
	})
	assert.NoError(t, err)

	_, err = res.RowsAffected()
	assert.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	assert.NoError(t, err)
	assert.True(t, is)

	// test add the column
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.String("title", 64)
		table.DropIndex("idx_price")

		return nil
	})
	assert.NoError(t, err)

	isCol, err := db.HasColumns("public", TableToCreate, "title")
	assert.NoError(t, err)
	assert.True(t, isCol)

	// test modify the column
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.String("title", 128).Change()

		return nil
	})
	assert.NoError(t, err)

	// test drop the column
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.DropColumn("title")

		return nil
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

	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.Increments("id")
		table.Json("settings")
		table.Char("tag", 10)
		table.Date("birthday", false)
		table.DateTime("created_at", true)
		table.DateTimeTz("updated_at", true)

		return nil
	})
	assert.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	assert.NoError(t, err)
	assert.True(t, is)

	// test modify the column
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.String("tag", 12).Index("idx_tag")
		table.Rename("settings", "options")

		return nil
	})
	assert.NoError(t, err)

	isCol, err := db.HasColumns("public", TableToCreate, "options")
	assert.NoError(t, err)
	assert.True(t, isCol)

	_, err = db.Drop(TableToCreate)
	assert.NoError(t, err)
}
