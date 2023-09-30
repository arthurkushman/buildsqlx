package buildsqlx

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

const TableToCreate = "big_tbl"

func TestDB_CreateEmptyTable(t *testing.T) {
	_, err := db.DropIfExists(TableToCreate)
	require.NoError(t, err)

	_, err = db.Schema(TableToCreate, func(table *Table) error {
		return nil
	})
}

func TestDB_CreateTable(t *testing.T) {
	_, err := db.DropIfExists(TableToCreate)
	require.NoError(t, err)

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
	require.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	require.NoError(t, err)
	require.True(t, is)

	_, err = db.Schema("tbl_to_ref", func(table *Table) error {
		table.Increments("id")
		table.Integer("big_tbl_id").ForeignKey("fk_idx_big_tbl_id", TableToCreate, "id").Concurrently()

		return nil
	})
	require.NoError(t, err)

	// test some err returning from fn()
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		return errors.New("some err")
	})
	require.Error(t, err)

	// 1st drop the referencing tbl
	_, err = db.Drop("tbl_to_ref")
	require.NoError(t, err)
	// then referenced
	_, err = db.Drop(TableToCreate)
	require.NoError(t, err)
}

func TestTable_BigIncrements(t *testing.T) {
	_, err := db.DropIfExists(TableToCreate)
	require.NoError(t, err)

	res, err := db.SchemaIfNotExists(TableToCreate, func(table *Table) error {
		table.BigIncrements("id")
		table.Numeric("price", 4, 3).Index("idx_price").Concurrently()
		table.Jsonb("taxes")

		return nil
	})
	require.NoError(t, err)

	_, err = res.RowsAffected()
	require.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	require.NoError(t, err)
	require.True(t, is)

	// test add columns
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		// if not exists will propagate onto both column and index
		table.String("title", 64).IfNotExists().Index("ttl_idx_if_not_exists").IfNotExists()
		table.DropIndex("idx_price")
		table.DropIndex("foo").IfExists()

		return nil
	})
	require.NoError(t, err)

	isCol, err := db.HasColumns("public", TableToCreate, "title")
	require.NoError(t, err)
	require.True(t, isCol)

	// test modify the column
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.String("title", 128).Change()

		return nil
	})
	require.NoError(t, err)

	// test drop the column
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.DropColumn("title")
		table.DropColumn("foo").IfExists()

		return nil
	})
	require.NoError(t, err)

	isCol, err = db.HasColumns("public", TableToCreate, "title")
	require.NoError(t, err)
	require.False(t, isCol)

	_, err = db.Drop(TableToCreate)
	require.NoError(t, err)
}

func TestTable_DateTime(t *testing.T) {
	_, err := db.DropIfExists(TableToCreate)
	require.NoError(t, err)

	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.Increments("id")
		table.Json("settings")
		table.Char("tag", 10)
		table.Date("birthday", false)
		table.Time("celebration_time", false)
		table.DateTime("created_at", true)
		table.DateTimeTz("updated_at", true)

		return nil
	})
	require.NoError(t, err)

	is, err := db.HasTable("public", TableToCreate)
	require.NoError(t, err)
	require.True(t, is)

	// test modify the column
	_, err = db.Schema(TableToCreate, func(table *Table) error {
		table.String("tag", 12).Index("idx_tag").Include("birthday", "created_at")
		table.Rename("settings", "options")

		return nil
	})
	require.NoError(t, err)

	isCol, err := db.HasColumns("public", TableToCreate, "options")
	require.NoError(t, err)
	require.True(t, isCol)

	_, err = db.Drop(TableToCreate)
	require.NoError(t, err)
}
