package buildsqlx

import (
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	TestTable    = "test"
	PostsTable   = "posts"
	UsersTable   = "users"
	TestUserName = "Dead Beaf"
)

var db = NewDb(NewConnection("postgres", "user=postgres dbname=postgres password=postgres sslmode=disable"))

var dataMap = map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": int64(123)}

func TestSelectAndLimit(t *testing.T) {
	db.Truncate(TestTable)

	db.Table(TestTable).Insert(dataMap)

	qDb := db.Table(TestTable).Select("foo", "bar")

	res, err := qDb.AddSelect("baz").Limit(15).Get()
	assert.NoError(t, err)

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
	assert.NoError(t, err)

	res, err := db.Table(TestTable).Select("foo", "bar", "baz").Get()
	assert.NoError(t, err)

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
	assert.NoError(t, err)

	res, err := db.Table(TestTable).Select("foo", "bar", "baz").OrderBy("foo", "ASC").Get()
	assert.NoError(t, err)

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
	assert.NoError(t, err)
	res, err := db.Table(TestTable).Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").OrWhere("baz", "=", 123).Get()
	assert.NoError(t, err)

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
	3: {"id": int64(4), "name": "Dead Beaf", "points": int64(12345)},
}

var batchPosts = []map[string]interface{}{
	0: {"id": int64(1), "title": "Lorem ipsum dolor sit amet,", "post": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.", "user_id": int64(1)},
	1: {"id": int64(2), "title": "Sed ut perspiciatis unde omnis iste natus", "post": "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo.", "user_id": int64(2)},
	2: {"id": int64(3), "title": "Ut enim ad minima veniam", "post": "Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur?", "user_id": int64(3)},
}

func TestJoins(t *testing.T) {
	db.Truncate(UsersTable)
	db.Truncate(PostsTable)

	var posts []map[string]interface{}
	for _, v := range batchUsers {
		id, err := db.Table(UsersTable).InsertGetId(v)
		assert.NoError(t, err)

		posts = append(posts, map[string]interface{}{
			"title": "ttl", "post": "foo bar baz", "user_id": id,
		})
	}

	err := db.Table(PostsTable).InsertBatch(posts)
	assert.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name", "post", "user_id").LeftJoin("posts", "users.id", "=", "posts.user_id").Get()
	assert.NoError(t, err)

	for k, val := range res {
		if val["name"] != batchUsers[k]["name"] {
			t.Fatalf("want: %s, got: %s", val["name"], batchUsers[k]["name"])
		}

		if val["user_id"] != batchUsers[k]["id"] {
			t.Fatalf("want: %d, got: %d", val["user_id"], batchUsers[k]["id"])
		}
	}

	db.Truncate(UsersTable)
	db.Truncate(PostsTable)
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
		assert.NoError(t, err)

		rows, err := db.Table(TestTable).Where("foo", "=", "foo foo foo").Update(obj.update)
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		rows, err := db.Table(TestTable).Where("baz", "=", obj.where["bar"]).Delete()
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		db.Table(TestTable).Increment("baz", obj.incr)

		res, err := db.Table(TestTable).Select("baz").Where("baz", "=", obj.incrRes).Get()
		assert.NoError(t, err)

		if len(res) < 1 && res[0]["baz"] != obj.incrRes {
			t.Fatalf("want %d, got %d", res[0]["baz"], obj.incrRes)
		}

		db.Table(TestTable).Decrement("baz", obj.decr)

		res, err = db.Table(TestTable).Select("baz").Where("baz", "=", obj.decrRes).Get()
		assert.NoError(t, err)

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
	{map[string]interface{}{"id": 1, "foo": "foo foo foo", "bar": "bar bar bar", "baz": 123}, "id", map[string]interface{}{"id": 1, "foo": "baz baz baz", "bar": "bar bar bar", "baz": 123}},
}

func TestDB_Replace(t *testing.T) {
	db.Truncate(TestTable)

	for _, obj := range rowsToReplace {
		rows, err := db.Table(TestTable).Replace(obj.insert, obj.conflict)
		assert.NoError(t, err)

		rows, err = db.Table(TestTable).Replace(obj.replace, obj.conflict)
		assert.NoError(t, err)

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
	db.Truncate(UsersTable)

	err := db.Table(TestTable).Insert(dataMap)
	assert.NoError(t, err)
	err = db.Table(UsersTable).Insert(userForUnion)
	assert.NoError(t, err)

	union := db.Table(TestTable).Select("bar", "baz").Union()
	res, _ := union.Table(UsersTable).Select("name", "points").Get()
	for _, v := range res {
		if v["points"] != userForUnion["points"] {
			t.Fatalf("want %d, got %d", userForUnion["points"], v["points"])
		}
	}

	db.Truncate(UsersTable)
	db.Truncate(TestTable)
}

func TestDB_InTransaction(t *testing.T) {
	err := db.InTransaction(func() (interface{}, error) {
		db.Truncate(TestTable)

		err := db.Table(TestTable).Insert(dataMap)

		db.Truncate(TestTable)

		return 1, err
	})
	assert.NoError(t, err)
}

func TestDB_HasTable(t *testing.T) {
	tblExists, err := db.HasTable("public", PostsTable)
	assert.NoError(t, err)

	if !tblExists {
		t.Fatalf("expected: true, got: false")
	}
}

func TestDB_HasColumns(t *testing.T) {
	colsExists, err := db.HasColumns("public", PostsTable, "title", "user_id")
	assert.NoError(t, err)

	if !colsExists {
		t.Fatalf("expected: true, got: false")
	}
}

func TestDB_First(t *testing.T) {
	db.Truncate(TestTable)

	err := db.Table(TestTable).Insert(dataMap)
	assert.NoError(t, err)

	// write concurrent row ot order and get the only 1st
	db.Table(TestTable).Insert(map[string]interface{}{"foo": "foo foo foo 2", "bar": "bar bar bar 2", "baz": int64(1234)})

	res, err := db.Table(TestTable).Select("baz").OrderBy("baz", "desc").First()

	if res["baz"] != int64(1234) {
		t.Fatalf("want: %d, got: %d", int64(1234), res["baz"])
	}

	db.Truncate(TestTable)
}

func TestDB_WhereExists(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)

	res, er := db.Table(UsersTable).Select("name").WhereExists(
		db.Table(UsersTable).Select("name").Where("points", ">=", int64(12345)),
	).First()
	assert.NoError(t, er)

	if res["name"] != TestUserName {
		t.Fatalf("want %s, got: %s", TestUserName, res["name"])
	}

	db.Truncate(UsersTable)
}

func TestDB_Value(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)
	res, err := db.Table(UsersTable).OrderBy("points", "desc").Value("name")
	assert.NoError(t, err)

	if res != TestUserName {
		t.Fatalf("want: %s, got: %s", TestUserName, res)
	}

	db.Truncate(UsersTable)
}

func TestDB_Pluck(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)
	res, err := db.Table(UsersTable).Pluck("name")
	assert.NoError(t, err)

	for k, v := range res {
		resVal := v.(string)
		if batchUsers[k]["name"] != resVal {
			t.Fatalf("want: %s, got: %s", batchUsers[k]["name"], resVal)
		}
	}

	db.Truncate(UsersTable)
}

func TestDB_PluckMap(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)
	res, err := db.Table(UsersTable).PluckMap("name", "points")
	assert.NoError(t, err)

	for k, m := range res {
		for key, value := range m {
			keyVal := key.(string)
			valueVal := value.(int64)
			if batchUsers[k]["name"] != keyVal || batchUsers[k]["points"] != valueVal {
				t.Fatalf("want: %s, got: %s and want: %d, got: %d", batchUsers[k]["name"], keyVal, batchUsers[k]["points"], valueVal)
			}
		}
	}

	db.Truncate(UsersTable)
}

func TestDB_Exists(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)

	prepared := db.Table(UsersTable).Select("name").Where("points", ">=", int64(12345))

	exists, err := prepared.Exists()
	assert.NoError(t, err)

	doesntEx, err := prepared.DoesntExists()
	assert.NoError(t, err)

	assert.True(t, exists, "The record must exist at this state of db data")
	assert.False(t, doesntEx, "The record must exist at this state of db data")

	db.Truncate(UsersTable)
}

func TestDB_Count(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)

	cnt, err := db.Table(UsersTable).Count()
	assert.NoError(t, err)

	assert.Equalf(t, int64(len(batchUsers)), cnt, "want: %d, got: %d", len(batchUsers), cnt)
	db.Truncate(UsersTable)
}

func TestDB_Avg(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)

	avg, err := db.Table(UsersTable).Avg("points")
	assert.NoError(t, err)

	var cntBatch float64
	for _, v := range batchUsers {
		cntBatch += float64(v["points"].(int64)) / float64(len(batchUsers))
	}

	assert.Equalf(t, cntBatch, avg, "want: %d, got: %d", cntBatch, avg)
	db.Truncate(UsersTable)
}

func TestDB_MinMax(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)

	mn, err := db.Table(UsersTable).Min("points")
	assert.NoError(t, err)

	mx, err := db.Table(UsersTable).Max("points")
	assert.NoError(t, err)

	var max float64
	var min = float64(123456)
	for _, v := range batchUsers {
		val := float64(v["points"].(int64))
		if val > max {
			max = val
		}
		if val < min {
			min = val
		}
	}

	assert.Equalf(t, mn, min, "want: %d, got: %d", mn, min)
	assert.Equalf(t, mx, max, "want: %d, got: %d", mx, max)
	db.Truncate(UsersTable)
}

func TestDB_Sum(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)

	sum, err := db.Table(UsersTable).Sum("points")
	assert.NoError(t, err)

	var cntBatch float64
	for _, v := range batchUsers {
		cntBatch += float64(v["points"].(int64))
	}

	assert.Equalf(t, cntBatch, sum, "want: %d, got: %d", cntBatch, sum)
	db.Truncate(UsersTable)
}

func TestDB_GroupByHaving(t *testing.T) {
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)

	res, err := db.Table(UsersTable).Select("points").GroupBy("points").Having("points", ">=", 123).Get()
	assert.NoError(t, err)
	assert.Equal(t, len(res), len(batchUsers)-1)

	db.Truncate(UsersTable)
}

func TestDB_AllJoins(t *testing.T) {
	db.Truncate(PostsTable)
	db.Truncate(UsersTable)

	err := db.Table(UsersTable).InsertBatch(batchUsers)
	assert.NoError(t, err)

	err = db.Table(PostsTable).InsertBatch(batchPosts)
	assert.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name", "post", "user_id").InnerJoin(PostsTable, "users.id", "=", "posts.user_id").Get()
	assert.NoError(t, err)

	assert.Equal(t, len(res), len(batchPosts))

	res, err = db.Table(PostsTable).Select("name", "post", "user_id").RightJoin(UsersTable, "posts.user_id", "=", "users.id").Get()
	assert.NoError(t, err)

	assert.Equal(t, len(res), len(batchUsers))

	res, err = db.Table(UsersTable).Select("name", "post", "user_id").FullJoin(PostsTable, "users.id", "=", "posts.user_id").Get()
	assert.NoError(t, err)

	assert.Equal(t, len(res), len(batchUsers))

	res, err = db.Table(UsersTable).Select("name", "post", "user_id").FullJoin(PostsTable, "users.id", "=", "posts.user_id").Get()
	assert.NoError(t, err)

	assert.Equal(t, len(res), len(batchUsers))

	db.Truncate(PostsTable)
	db.Truncate(UsersTable)
}
