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
	0: {"foo": "foo foo foo", "bar": "bar bar bar", "baz": int64(123)},
	1: {"foo": "foo foo foo foo", "bar": "bar bar bar bar", "baz": int64(1234)},
	2: {"foo": "foo foo foo foo foo", "bar": "bar bar bar bar bar", "baz": int64(12345)},
}

func TestInsertBatchSelectMultiple(t *testing.T) {
	db.Truncate(TestTable)

	err := db.Table("test").InsertBatch(batchData)

	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Table(TestTable).Select("foo", "bar", "baz").OrderBy("foo", "ASC").Get()

	if err != nil {
		t.Fatal(err)
	}

	for mapKey, mapVal := range batchData {
		for k, mV := range mapVal {
			if res[mapKey][k] != mV {
				t.Fatalf("want: %T, got: %T", mV, res[mapKey][k])
			}
		}
	}

	db.Truncate(TestTable)
}

func TestWhereAndOr(t *testing.T) {
	var cmp = "foo foo foo"

	db.Truncate(TestTable)

	err := db.Table("test").InsertBatch(batchData)

	res, err := db.Table(TestTable).Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").OrWhere("baz", "=", 123).Get()

	if err != nil {
		t.Fatal(err)
	}

	if res[0]["foo"] != cmp {
		t.Fatalf("want: %s, got: %s", res[0]["foo"], cmp)
	}

	db.Truncate(TestTable)
}

//var users = `create table users (id serial primary key, name varchar(128) not null, points integer)`
//
//var posts = `create table posts (id serial primary key, title varchar(128) not null, post text)`

var batchUsers = []map[string]interface{}{
	0: {"name": "Alex Shmidt", "points": int64(123)},
	1: {"name": "Darth Vader", "points": int64(1234)},
	2: {"name": "Dead Beaf", "points": int64(12345)},
}

var batchPosts = []map[string]interface{}{
	0: {"title": "ttl1", "points": int64(123)},
	1: {"title": "ttl2", "points": int64(1234)},
	2: {"title": "ttl3", "points": int64(12345)},
}

func TestJoins(t *testing.T) {
	db.Truncate(TestTable)

	err := db.Table("users").InsertBatch(batchUsers)

	err := db.Table("users").InsertBatch(batchPosts)

	res, err := db.Table("users").Select("text").LeftJoin("posts", "users.id", "=", "posts.user_id").Get()

	if err != nil {
		t.Fatal(err)
	}

	if res[0]["foo"] != cmp {
		t.Fatalf("want: %s, got: %s", res[0]["foo"], cmp)
	}

	db.Truncate(TestTable)
}
