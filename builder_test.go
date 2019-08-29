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

	db.Table(TestTable).Insert(dataMap)

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

	err := db.Table(TestTable).Insert(dataMap)

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

	err := db.Table(TestTable).InsertBatch(batchData)

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
				t.Fatalf("want: %v, got: %v", mV, res[mapKey][k])
			}
		}
	}

	db.Truncate(TestTable)
}

func TestWhereAndOr(t *testing.T) {
	var cmp = "foo foo foo"

	db.Truncate(TestTable)

	err := db.Table(TestTable).InsertBatch(batchData)

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
//var posts = `create table posts (id serial primary key, title varchar(128) not null, post text, user_id integer)`

var batchUsers = []map[string]interface{}{
	0: {"id": int64(1), "name": "Alex Shmidt", "points": int64(123)},
	1: {"id": int64(2), "name": "Darth Vader", "points": int64(1234)},
	2: {"id": int64(3), "name": "Dead Beaf", "points": int64(12345)},
}

func TestJoins(t *testing.T) {
	db.Truncate("users")
	db.Truncate("posts")

	var batchPosts []map[string]interface{}
	for _, v := range batchUsers {
		id, err := db.Table("users").InsertGetId(v)

		if err != nil {
			t.Fatal(err)
		}

		batchPosts = append(batchPosts, map[string]interface{}{
			"title": "ttl", "post": "foo bar baz", "user_id": id,
		})
	}

	err := db.Table("posts").InsertBatch(batchPosts)

	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Table("users").Select("name", "post", "user_id").LeftJoin("posts", "users.id", "=", "posts.user_id").Get()

	if err != nil {
		t.Fatal(err)
	}

	for k, val := range res {
		if val["name"] != batchUsers[k]["name"] {
			t.Fatalf("want: %s, got: %s", val["name"], batchUsers[k]["name"])
		}

		if val["user_id"] != batchUsers[k]["id"] {
			t.Fatalf("want: %d, got: %d", val["user_id"], batchUsers[k]["id"])
		}
	}

	db.Truncate("users")
	db.Truncate("posts")
}

var rowsToUpdate = []struct {
	insert map[string]interface{}
	update map[string]interface{}
}{
	{map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": 123}, map[string]interface{}{"foo": "foo changed"}},
}

func TestUpdate(t *testing.T) {
	db.Truncate(TestTable)

	for _, obj := range rowsToUpdate {
		err := db.Table(TestTable).Insert(obj.insert)

		if err != nil {
			t.Fatal(err)
		}

		rows, err := db.Table(TestTable).Where("foo", "=", "foo foo foo").Update(obj.update)

		if err != nil {
			t.Fatal(err)
		}

		if rows < 1 {
			t.Fatalf("Can not update rows: %s", obj.update)
		}
	}

	db.Truncate(TestTable)
}

var rowsToDelete = []struct {
	insert map[string]interface{}
	where  map[string]interface{}
}{
	{map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": 123}, map[string]interface{}{"bar": 123}},
}

func TestDelete(t *testing.T) {
	db.Truncate(TestTable)

	for _, obj := range rowsToDelete {
		err := db.Table(TestTable).Insert(obj.insert)

		if err != nil {
			t.Fatal(err)
		}

		rows, err := db.Table(TestTable).Where("baz", "=", obj.where["bar"]).Delete()

		if err != nil {
			t.Fatal(err)
		}

		if rows < 1 {
			t.Fatalf("Can not delete rows: %s", obj.where)
		}
	}
}

var incrDecr = []struct {
	insert  map[string]interface{}
	incr    uint64
	incrRes uint64
	decr    uint64
	decrRes uint64
}{
	{map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": 1}, 3, 4, 1, 3},
}

func TestDB_Increment_Decrement(t *testing.T) {
	db.Truncate(TestTable)

	for _, obj := range incrDecr {
		err := db.Table(TestTable).Insert(obj.insert)

		if err != nil {
			t.Fatal(err)
		}

		db.Table(TestTable).Increment("baz", obj.incr)

		res, err := db.Table(TestTable).Select("baz").Where("baz", "=", obj.incrRes).Get()

		if err != nil {
			t.Fatal(err)
		}

		if len(res) < 1 && res[0]["baz"] != obj.incrRes {
			t.Fatalf("want %d, got %d", res[0]["baz"], obj.incrRes)
		}

		db.Table(TestTable).Decrement("baz", obj.decr)

		res, err = db.Table(TestTable).Select("baz").Where("baz", "=", obj.decrRes).Get()

		if err != nil {
			t.Fatal(err)
		}

		if len(res) < 1 && res[0]["baz"] != obj.decrRes {
			t.Fatalf("want %d, got %d", res[0]["baz"], obj.decrRes)
		}
	}

	db.Truncate(TestTable)
}

var rowsToReplace = []struct {
	insert   map[string]interface{}
	conflict string
	replace  map[string]interface{}
}{
	{map[string]interface{}{"id": 1, "foo": "foo foo foo", "bar": "bar bar bar", "baz": 123}, "id", map[string]interface{}{"id": 1, "foo": "baz baz baz", "baz": 123}},
}

func TestDB_Replace(t *testing.T) {
	db.Truncate(TestTable)

	for _, obj := range rowsToReplace {
		rows, err := db.Table(TestTable).Replace(obj.insert, obj.conflict)

		if rows < 1 {
			t.Fatal(err)
		}

		rows, err = db.Table(TestTable).Replace(obj.replace, obj.conflict)

		if err != nil {
			t.Fatal(err)
		}

		if rows < 1 {
			t.Fatal(err)
		}

		res, err := db.Table(TestTable).Select("baz").Where("baz", "=", obj.replace["baz"]).Get()

		if len(res) < 1 && res[0]["foo"] != obj.replace["foo"] {
			t.Fatalf("want %d, got %d", obj.replace["foo"], res[0]["foo"])
		}
	}

	db.Truncate(TestTable)
}

var userForUnion = map[string]interface{}{"id": int64(1), "name": "Alex Shmidt", "points": int64(123)}

func TestDB_Union(t *testing.T) {
	db.Truncate(TestTable)

	err := db.Table(TestTable).Insert(dataMap)

	if err != nil {
		t.Fatal(err)
	}

	db.Table("users").Insert(userForUnion)

	union := db.Table(TestTable).Select("bar", "baz").Union()

	res, _ := union.Table("users").Select("name", "points").Get()

	for _, v := range res {
		if v["baz"] != userForUnion["points"] {
			t.Fatalf("want %d, got %d", userForUnion["points"], v["baz"])
		}
	}

	db.Truncate(TestTable)
}

func TestDB_InTransaction(t *testing.T) {
	err := db.InTransaction(func() (interface{}, error) {
		db.Truncate(TestTable)

		err := db.Table(TestTable).Insert(dataMap)

		db.Truncate(TestTable)

		return 1, err
	})

	if err != nil {
		t.Fatal(err)
	}
}
