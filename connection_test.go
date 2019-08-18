package arsqlx

import (
	_ "github.com/lib/pq"
	"testing"
)

const TestTable = "test"

var db = NewDb(NewConnection("postgres", "user=postgres dbname=postgres password=postgres sslmode=disable"))

var dataMap = map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": int64(123)}

func TestSelectAndLimit(t *testing.T) {
	db.Truncate(TestTable)

	db.Table("test").Insert(dataMap)

	qDb := db.Table(TestTable).Select("foo", "bar")

	res, err := qDb.AddSelect("baz").Limit(15).Get()

	if err != nil {
		t.Fatal(err)
	}

	for k, mapVal := range dataMap {
		for _, v := range res {
			if v[k] != mapVal {
				t.Fatalf("want: %T, got: %T", mapVal, v[k])
			}
		}
	}

	db.Truncate(TestTable)
}

func TestInsert(t *testing.T) {
	db.Truncate(TestTable)

	err := db.Table("test").Insert(dataMap)

	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Table(TestTable).Select("foo", "bar", "baz").Get()

	if err != nil {
		t.Fatal(err)
	}

	for k, mapVal := range dataMap {
		for _, v := range res {
			if v[k] != mapVal {
				t.Fatalf("want: %v, got: %v", mapVal, v[k])
			}
		}
	}

	db.Truncate(TestTable)
}

var batchData = []map[string]interface{}{
	0: {"foo": "foo foo foo", "bar": "bar bar bar", "baz": 123},
	1: {"foo": "foo foo foo foo", "bar": "bar bar bar bar", "baz": 1234},
	2: {"foo": "foo foo foo foo foo", "bar": "bar bar bar bar bar", "baz": 12345},
}

func TestInsertBatch(t *testing.T) {
	db.Truncate(TestTable)

	err := db.Table("test").InsertBatch(batchData)

	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Table(TestTable).Select("foo", "bar", "baz").OrderBy("foo", "ASC").Get()

	if err != nil {
		t.Fatal(err)
	}

	for _, mapVal := range batchData {
		for k, mV := range mapVal {
			for _, v := range res {
				if v[k] != mV {
					t.Fatalf("want: %v, got: %v", mV, v[k])
				}
			}
		}
	}

	db.Truncate(TestTable)
}

func TestSelectRaw(t *testing.T) {

}

func TestWhereAndOr(t *testing.T) {

}
