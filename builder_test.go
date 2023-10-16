package buildsqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

const (
	TestTable    = "test"
	PostsTable   = "test_posts"
	UsersTable   = "test_users"
	TestUserName = "Dead Beaf"
)

type DataStruct struct {
	Foo string
	Bar string
	Baz *int64
}

var (
	dbConnInfo = fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		"localhost", 5432, "postgres", "postgres", "postgres")

	dataMap = map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": int64(123)}

	baz  = int64(123)
	data = DataStruct{
		Foo: "foo foo foo",
		Bar: "bar bar bar",
		Baz: &baz,
	}

	db = NewDb(NewConnection("postgres", dbConnInfo))
)

func TestMain(m *testing.M) {
	_, err := db.Sql().Exec("create table if not exists test_users (id serial primary key, name varchar(128) not null, points integer)")
	if err != nil {
		panic(err)
	}

	_, err = db.Sql().Exec("create table if not exists test_posts (id serial primary key, title varchar(128) not null, post text, user_id integer, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
	if err != nil {
		panic(err)
	}

	_, err = db.Sql().Exec("create table if not exists test (id serial primary key, foo varchar(128) not null, bar varchar(128) not null, baz integer)")
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

// TestNewConnectionFromDB tests the NewConnectionFromDB function.
//
// This function takes a testing.T object as a parameter and creates a new connection
// from a database. It then asserts that the created connection's "db" field is equal
// to the provided sql.DB object.
func TestNewConnectionFromDB(t *testing.T) {
	conn := NewConnectionFromDb(&sql.DB{})
	require.Equal(t, conn.db, &sql.DB{})
}

// TestSelectAndLimit is a test function that tests the SelectAndLimit functionality.
//
// It truncates the TestTable, inserts data into it, performs a select query with specific columns and a limit of 15,
// and scans the results into a slice of DataStruct.
// Finally, it asserts that the expected data is equal to the scanned data, and that the length of the testStructs slice is 2.
func TestSelectAndLimit(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	err = db.Table(TestTable).Insert(data)
	require.NoError(t, err)

	err = db.Table(TestTable).Insert(data)
	require.NoError(t, err)

	toScan := DataStruct{}
	qDb := db.Table(TestTable).Select("foo", "bar")
	var testStructs []DataStruct
	err = qDb.AddSelect("baz").Limit(15).EachToStruct(func(rows *sql.Rows) error {
		err = db.Next(rows, &toScan)
		if err != nil {
			return err
		}

		testStructs = append(testStructs, toScan)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, data, toScan)
	require.Len(t, testStructs, 2)

	for _, strct := range testStructs {
		require.Equal(t, data.Foo, strct.Foo)
		require.Equal(t, data.Bar, strct.Bar)
		require.Equal(t, data.Baz, strct.Baz)
	}

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

func TestDB_EachToStruct_RetErr(t *testing.T) {
	err := db.Table(TestTable).Select("foo", "bar").EachToStruct(func(rows *sql.Rows) error {
		return errors.New("some err")
	})
	require.EqualError(t, err, "some err")
}

func TestDB_ChunkCountErr(t *testing.T) {
	err := db.Table("").Chunk(&DataStruct{}, 1, func(rows []any) bool {
		return false
	})
	require.EqualError(t, err, "pq: zero-length delimited identifier at or near \"\"\"\"")
}

func TestInsert(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	err = db.Table(TestTable).Insert(data)
	require.NoError(t, err)

	res, err := db.Table(TestTable).Select("foo", "bar", "baz").Get()
	require.NoError(t, err)

	for k, mapVal := range dataMap {
		for _, v := range res {
			require.Equal(t, v[k], mapVal)
		}
	}

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

var batchDataStruct = []DataStruct{
	{Foo: "foo foo foo", Bar: "bar bar bar", Baz: &baz},
	{Foo: "foo foo foo foo", Bar: "bar bar bar bar", Baz: &baz},
	{Foo: "foo foo foo foo foo", Bar: "bar bar bar bar bar", Baz: &baz},
}

func TestInsertBatchSelectMultiple(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	err = db.Table(TestTable).InsertBatch(batchDataStruct)
	require.NoError(t, err)

	dataStruct := DataStruct{}
	var dataStructs []DataStruct
	err = db.Table(TestTable).Select("foo", "bar", "baz").OrderBy("foo", "ASC").
		EachToStruct(func(rows *sql.Rows) error {
			err = db.Next(rows, &dataStruct)
			if err != nil {
				return err
			}

			dataStructs = append(dataStructs, dataStruct)
			return nil
		})
	require.NoError(t, err)

	for mapKey, dStruct := range dataStructs {
		require.Equal(t, batchDataStruct[mapKey], dStruct)
	}

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

func TestWhereOnly(t *testing.T) {
	var cmp = "foo foo foo"

	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	err = db.Table(TestTable).InsertBatch(batchDataStruct)
	require.NoError(t, err)
	res, err := db.Table(TestTable).Select("foo", "bar", "baz").Where("foo", "=", cmp).Get()
	require.NoError(t, err)

	require.Equal(t, res[0]["foo"], cmp)

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

func TestWhereAndOr(t *testing.T) {
	var cmp = "foo foo foo"

	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	err = db.Table(TestTable).InsertBatch(batchDataStruct)
	require.NoError(t, err)
	res, err := db.Table(TestTable).Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").OrWhere("baz", "=", 123).Get()
	require.NoError(t, err)

	require.Equal(t, res[0]["foo"], cmp)

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

var batchUsers = []DataStructUser{
	{ID: int64(1), Name: "Alex Shmidt", Points: int64(123)},
	{ID: int64(2), Name: "Darth Vader", Points: int64(1234)},
	{ID: int64(3), Name: "Dead Beaf", Points: int64(12345)},
	{ID: int64(4), Name: "Dead Beaf", Points: int64(12345)},
}

type DataStructPost struct {
	ID        int64
	Title     string
	Post      *string
	UserID    *int64    `db:"user_id"`
	UpdatedAt time.Time `db:"updated_at"`
}

var userID, userIDTwo, userIDThree = int64(1), int64(2), int64(3)
var post = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
var timeNow = time.Now()

var batchPosts = []DataStructPost{
	{ID: 1, Title: "Lorem ipsum dolor sit amet,", Post: &post, UserID: &userID, UpdatedAt: timeNow},
	{ID: 2, Title: "Sed ut perspiciatis unde omnis iste natus", Post: &post, UserID: &userIDTwo, UpdatedAt: timeNow},
	{ID: 3, Title: "Ut enim ad minima veniam", Post: &post, UserID: &userIDThree, UpdatedAt: timeNow},
	{ID: 4, Title: "Lorem ipsum dolor sit amet,", Post: nil, UserID: nil, UpdatedAt: timeNow},
}

type DataStructUser struct {
	ID     int64
	Name   string
	Points int64
}

var batchUsersStruct = []DataStructUser{
	0: {ID: int64(1), Name: "Alex Shmidt", Points: int64(123)},
	1: {ID: int64(2), Name: "Darth Vader", Points: int64(1234)},
	2: {ID: int64(3), Name: "Dead Beaf", Points: int64(12345)},
	3: {ID: int64(4), Name: "Dead Beaf", Points: int64(12345)},
}

func TestJoins(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	_, err = db.Truncate(PostsTable)
	require.NoError(t, err)

	var posts []DataStructPost
	for _, v := range batchUsersStruct {
		id, err := db.Table(UsersTable).InsertGetId(v)
		require.NoError(t, err)

		iID := int64(id)
		posts = append(posts, DataStructPost{
			ID: v.ID, Title: "ttl", Post: &post, UserID: &iID,
		})
	}

	err = db.Table(PostsTable).InsertBatch(posts)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name", "post", "user_id").LeftJoin(
		PostsTable, UsersTable+".id", "=", PostsTable+".user_id").Get()
	require.NoError(t, err)

	for k, val := range res {
		require.Equal(t, val["name"], batchUsers[k].Name)
		require.Equal(t, val["user_id"], batchUsers[k].ID)
	}

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)

	_, err = db.Truncate(PostsTable)
	require.NoError(t, err)
}

var rowsToUpdate = []struct {
	insert DataStruct
	update DataStruct
}{
	{DataStruct{Foo: "foo foo foo", Bar: "bar bar bar", Baz: &baz}, DataStruct{Foo: "foo changed", Baz: nil}},
}

func TestUpdate(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	for _, obj := range rowsToUpdate {
		err := db.Table(TestTable).Insert(obj.insert)
		require.NoError(t, err)

		rows, err := db.Table(TestTable).Where("foo", "=", "foo foo foo").Update(obj.update)
		require.NoError(t, err)
		require.GreaterOrEqual(t, rows, int64(1))

		res, err := db.Table(TestTable).Select("foo").Where("foo", "=", obj.update.Foo).Get()
		require.NoError(t, err)
		require.Equal(t, obj.update.Foo, res[0]["foo"])
	}

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

var rowsToDelete = []struct {
	insert DataStruct
	where  map[string]interface{}
}{
	{DataStruct{Foo: "foo foo foo", Bar: "bar bar bar", Baz: &baz}, map[string]interface{}{"bar": 123}},
}

func TestDelete(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	for _, obj := range rowsToDelete {
		err := db.Table(TestTable).Insert(obj.insert)
		require.NoError(t, err)

		rows, err := db.Table(TestTable).Where("baz", "=", obj.where["bar"]).Delete()
		require.NoError(t, err)
		require.GreaterOrEqual(t, rows, int64(1))
	}
}

var incrDecr = []struct {
	insert  DataStruct
	incr    uint64
	incrRes uint64
	decr    uint64
	decrRes uint64
}{
	{DataStruct{Foo: "foo foo foo", Bar: "bar bar bar", Baz: &baz}, 3, 126, 1, 125},
}

func TestDB_Increment_Decrement(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	for _, obj := range incrDecr {
		err = db.Table(TestTable).Insert(obj.insert)
		require.NoError(t, err)

		_, err = db.Table(TestTable).Increment("baz", obj.incr)
		require.NoError(t, err)

		res, err := db.Table(TestTable).Select("baz").Where("baz", "=", obj.incrRes).Get()
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res), 1)
		require.Equal(t, res[0]["baz"], int64(obj.incrRes))

		_, err = db.Table(TestTable).Decrement("baz", obj.decr)
		require.NoError(t, err)

		res, err = db.Table(TestTable).Select("baz").Where("baz", "=", obj.decrRes).Get()
		require.NoError(t, err)

		require.GreaterOrEqual(t, len(res), 1)
		require.Equal(t, res[0]["baz"], int64(obj.decrRes))
	}

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

type DataStructID struct {
	ID  int64
	Foo string
	Bar string
	Baz int64
}

var rowsToReplace = []struct {
	insert   DataStructID
	conflict string
	replace  DataStructID
}{
	{DataStructID{ID: 1, Foo: "foo foo foo", Bar: "bar bar bar", Baz: 123}, "id", DataStructID{ID: 1, Foo: "baz baz baz", Bar: "bar bar bar", Baz: 123}},
}

func TestDB_Replace(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	for _, obj := range rowsToReplace {
		_, err := db.Table(TestTable).Replace(obj.insert, obj.conflict)
		require.NoError(t, err)

		rows, err := db.Table(TestTable).Replace(obj.replace, obj.conflict)
		require.NoError(t, err)
		require.GreaterOrEqual(t, rows, int64(1))

		res, err := db.Table(TestTable).Select("foo").Where("baz", "=", obj.replace.Baz).Get()
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res), 1)
		require.Equal(t, res[0]["foo"], obj.replace.Foo)
	}

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

type User struct {
	ID     int64
	Name   string
	Points int64
}

var userForUnion = User{ID: int64(1), Name: "Alex Shmidt", Points: int64(123)}

func TestDB_Union(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(TestTable).Insert(data)
	require.NoError(t, err)
	err = db.Table(UsersTable).Insert(userForUnion)
	require.NoError(t, err)

	union := db.Table(TestTable).Select("bar", "baz").Union()
	dataStruct := DataStruct{}
	var dataStructs []DataStruct
	err = union.Table(UsersTable).Select("name", "points").EachToStruct(func(rows *sql.Rows) error {
		err = union.Next(rows, &dataStruct)
		if err != nil {
			return err
		}

		dataStructs = append(dataStructs, dataStruct)
		return nil
	})
	require.NoError(t, err)
	for _, v := range dataStructs {
		require.Equal(t, *v.Baz, userForUnion.Points)
	}

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_InTransaction(t *testing.T) {
	var tests = map[string]struct {
		dataMap DataStruct
		res     interface{}
		err     error
	}{
		"transaction commit ok": {
			dataMap: data,
			res:     1,
			err:     nil,
		},
		"transaction commit ok int64": {
			dataMap: data,
			res:     int64(1),
			err:     nil,
		},
		"transaction commit ok uint64": {
			dataMap: data,
			res:     uint64(1),
			err:     nil,
		},
		"transaction commit ok map[string]interface{}": {
			dataMap: data,
			res:     map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": int64(123)},
			err:     nil,
		},
		"transaction commit ok []map[string]interface{}": {
			dataMap: data,
			res: []map[string]interface{}{
				{
					"foo": "foo foo foo", "bar": "bar bar bar", "baz": int64(123),
				},
			},
			err: nil,
		},
		"transaction early exit err": {
			dataMap: data,
			res:     0,
			err:     errors.New("some error"),
		},
		"transaction rollback": {
			dataMap: data,
			res:     0,
			err:     nil,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			_, err := db.Truncate(TestTable)
			require.NoError(t, err)

			defer func() {
				_, err = db.Truncate(TestTable)
				require.NoError(t, err)
			}()

			err = db.InTransaction(func() (any, error) {
				err = db.Table(TestTable).Insert(tt.dataMap)

				return tt.res, tt.err
			})

			if tt.err != nil {
				require.Error(t, tt.err, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDB_HasTable(t *testing.T) {
	tblExists, err := db.HasTable("public", PostsTable)
	require.NoError(t, err)
	require.True(t, tblExists)
}

func TestDB_HasColumns(t *testing.T) {
	colsExists, err := db.HasColumns("public", PostsTable, "title", "user_id")
	require.NoError(t, err)
	require.True(t, colsExists)
}

func TestDB_First(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	err = db.Table(TestTable).Insert(data)
	require.NoError(t, err)

	// write concurrent row to order and get the only 1st
	bazz := int64(1234)
	err = db.Table(TestTable).Insert(DataStruct{
		Foo: "foo foo foo 2",
		Bar: "bar bar bar 2",
		Baz: &bazz,
	})
	require.NoError(t, err)

	dataStruct := &DataStruct{}
	err = db.Table(TestTable).Select("baz").OrderBy("baz", "desc").OrderBy("foo", "desc").First(dataStruct)
	require.NoError(t, err)
	require.Equal(t, *dataStruct.Baz, int64(1234))

	err = db.Table(TestTable).Select("baz").OrderBy("baz", "desc").OrderBy("fo", "desc").First(dataStruct)
	require.Error(t, err)

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

func TestDB_Find(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	id, err := db.Table(TestTable).InsertGetId(data)
	require.NoError(t, err)

	dataStruct := &DataStructID{}
	err = db.Table(TestTable).Find(dataStruct, id)
	require.NoError(t, err)
	require.Equal(t, dataStruct.Foo, data.Foo)
	require.Equal(t, dataStruct.Bar, data.Bar)
	require.Equal(t, dataStruct.Baz, *data.Baz)

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}

func TestDB_WhereExists(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	dataStruct := &DataStructUser{}
	er := db.Table(UsersTable).Select("name").WhereExists(
		db.Table(UsersTable).Select("name").Where("points", ">=", int64(12345)),
	).First(dataStruct)
	require.NoError(t, er)
	require.Equal(t, TestUserName, dataStruct.Name)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_WhereNotExists(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	dataStruct := &DataStructUser{}
	er := db.Table(UsersTable).Select("name").WhereNotExists(
		db.Table(UsersTable).Select("name").Where("points", ">=", int64(12345)),
	).First(dataStruct)
	require.NoError(t, er)
	require.Equal(t, TestUserName, dataStruct.Name)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Value(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	dataStruct := &DataStructUser{}
	err = db.Table(UsersTable).OrderBy("points", "desc").Value(dataStruct, "name")
	require.NoError(t, err)
	require.Equal(t, TestUserName, dataStruct.Name)

	err = db.Table(UsersTable).OrderBy("poin", "desc").Value(dataStruct, "name")
	require.Error(t, err)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Pluck(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)
	res, err := db.Table(UsersTable).Pluck("name")
	require.NoError(t, err)

	for k, v := range res {
		resVal := v.(string)
		require.Equal(t, batchUsers[k].Name, resVal)
	}

	_, err = db.Table("nonexistent").Pluck("name")
	require.Error(t, err)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_PluckMap(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)
	res, err := db.Table(UsersTable).PluckMap("name", "points")
	require.NoError(t, err)

	for k, m := range res {
		for key, value := range m {
			keyVal := key.(string)
			valueVal := value.(int64)
			require.Equal(t, batchUsers[k].Name, keyVal)
			require.Equal(t, batchUsers[k].Points, valueVal)
		}
	}

	_, err = db.Table("nonexistent").PluckMap("name", "points")
	require.Error(t, err)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Exists(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	prepared := db.Table(UsersTable).Select("name").Where("points", ">=", int64(12345))

	exists, err := prepared.Exists()
	require.NoError(t, err)

	doesntEx, err := prepared.DoesntExists()
	require.NoError(t, err)

	require.True(t, exists, "The record must exist at this state of db data")
	require.False(t, doesntEx, "The record must exist at this state of db data")

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Count(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	cnt, err := db.Table(UsersTable).Count()
	require.NoError(t, err)

	require.Equalf(t, int64(len(batchUsers)), cnt, "want: %d, got: %d", len(batchUsers), cnt)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Avg(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	avg, err := db.Table(UsersTable).Avg("points")
	require.NoError(t, err)

	var cntBatch float64
	for _, v := range batchUsers {
		cntBatch += float64(v.Points) / float64(len(batchUsers))
	}

	require.Equalf(t, cntBatch, avg, "want: %d, got: %d", cntBatch, avg)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_MinMax(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	mn, err := db.Table(UsersTable).Min("points")
	require.NoError(t, err)

	mx, err := db.Table(UsersTable).Max("points")
	require.NoError(t, err)

	var max float64
	var min = float64(123456)
	for _, v := range batchUsers {
		val := float64(v.Points)
		if val > max {
			max = val
		}
		if val < min {
			min = val
		}
	}

	require.Equalf(t, mn, min, "want: %d, got: %d", mn, min)
	require.Equalf(t, mx, max, "want: %d, got: %d", mx, max)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Sum(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	sum, err := db.Table(UsersTable).Sum("points")
	require.NoError(t, err)

	var cntBatch float64
	for _, v := range batchUsers {
		cntBatch += float64(v.Points)
	}

	require.Equalf(t, cntBatch, sum, "want: %d, got: %d", cntBatch, sum)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_GroupByHaving(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("points").GroupBy("points").Having("points", ">=", 123).Get()
	require.NoError(t, err)
	require.Equal(t, len(res), len(batchUsers)-1)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_HavingRaw(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("points").GroupBy("points").HavingRaw("points > 123").AndHavingRaw("points < 12345").OrHavingRaw("points = 0").Get()
	require.NoError(t, err)
	require.Equal(t, len(batchUsers)-3, len(res))

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_AllJoins(t *testing.T) {
	_, err := db.Truncate(PostsTable)
	require.NoError(t, err)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	err = db.Table(PostsTable).InsertBatch(batchPosts)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name", "post", "user_id").InnerJoin(PostsTable, UsersTable+".id",
		"=", PostsTable+".user_id").Get()
	require.NoError(t, err)

	require.Equal(t, len(res), len(batchPosts)-1)

	res, err = db.Table(PostsTable).Select("name", "post", "user_id").RightJoin(UsersTable, PostsTable+".user_id",
		"=", UsersTable+".id").Get()
	require.NoError(t, err)

	require.Equal(t, len(res), len(batchUsers))

	res, err = db.Table(UsersTable).Select("name", "post", "user_id").FullJoin(PostsTable, UsersTable+".id",
		"=", PostsTable+".user_id").Get()
	require.NoError(t, err)

	require.Equal(t, len(res), len(batchUsers)+1)

	res, err = db.Table(UsersTable).Select("name", "post", "user_id").FullJoin(PostsTable,
		UsersTable+".id", "=", PostsTable+".user_id").Get()
	require.NoError(t, err)

	require.Equal(t, len(res), len(batchUsers)+1)

	// note InRandomOrder check
	res, err = db.Table(UsersTable).Select("name", "post", "user_id").FullJoin(
		PostsTable, UsersTable+".id", "=", PostsTable+".user_id").InRandomOrder().Get()
	require.NoError(t, err)

	require.Equal(t, len(res), len(batchUsers)+1)

	_, err = db.Truncate(PostsTable)
	require.NoError(t, err)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_OrderByRaw(t *testing.T) {
	_, err := db.Truncate(PostsTable)
	require.NoError(t, err)

	err = db.Table(PostsTable).InsertBatch(batchPosts)
	require.NoError(t, err)

	dataStruct := &DataStructPost{}
	err = db.Table(PostsTable).Select("title").OrderByRaw("updated_at - created_at DESC").First(dataStruct)
	require.NoError(t, err)

	require.Equal(t, batchPosts[0].Title, dataStruct.Title)

	_, err = db.Truncate(PostsTable)
	require.NoError(t, err)
}

func TestDB_SelectRaw(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	dataStruct := &DataStructUser{}
	err = db.Table(UsersTable).SelectRaw("SUM(points) as points").First(dataStruct)
	require.NoError(t, err)

	var sum int64
	for _, v := range batchUsers {
		sum += v.Points
	}
	require.Equal(t, sum, dataStruct.Points)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_AndWhereBetween(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	dataStruct := &DataStructUser{}
	err = db.Table(UsersTable).Select("name").WhereBetween("points", 1233, 12345).
		OrWhereBetween("points", 123456, 67891023).AndWhereNotBetween("points", 12, 23).
		First(dataStruct)
	require.NoError(t, err)
	require.Equal(t, "Darth Vader", dataStruct.Name)

	err = db.Table(UsersTable).Select("name").WhereNotBetween("points", 12, 123).
		AndWhereBetween("points", 1233, 12345).OrWhereNotBetween("points", 12, 23).First(dataStruct)
	require.NoError(t, err)
	require.Equal(t, "Alex Shmidt", dataStruct.Name)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_WhereRaw(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name").WhereRaw("LENGTH(name) > 15").OrWhereRaw("points > 1234").Get()
	require.NoError(t, err)
	require.Equal(t, len(res), 2)

	cnt, err := db.Table(UsersTable).WhereRaw("points > 123").AndWhereRaw("points < 12345").Count()
	require.NoError(t, err)
	require.Equal(t, cnt, int64(1))

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Offset(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Offset(2).Limit(10).Get()
	require.NoError(t, err)
	require.Equal(t, len(res), 2)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Rename(t *testing.T) {
	tbl := "tbl1"
	tbl2 := "tbl2"
	_, err := db.DropIfExists(tbl, tbl2)
	require.NoError(t, err)

	_, err = db.Schema(tbl, func(table *Table) error {
		table.Increments("id")

		return nil
	})
	require.NoError(t, err)

	_, err = db.Rename(tbl, tbl2)
	require.NoError(t, err)

	exists, err := db.HasTable("public", tbl2)
	require.NoError(t, err)
	require.True(t, exists)

	_, err = db.Drop(tbl2)
	require.NoError(t, err)
}

func TestDB_WhereIn(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)
	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name").WhereIn("points", []int64{123, 1234}).OrWhereIn("id", []int64{1, 2}).Get()
	require.NoError(t, err)
	require.Equal(t, len(res), 2)

	res, err = db.Table(UsersTable).Select("name").WhereIn("points", []int64{123, 1234}).AndWhereIn("id", []int64{1, 2}).Get()
	require.NoError(t, err)
	require.Equal(t, len(res), 2)

	require.Panicsf(t, func() {
		_, _ = db.Table(UsersTable).Select("name").WhereIn("points", DataStruct{}).AndWhereIn("id", []int64{1, 2}).Get()
	}, "interfaceToSlice() given a non-slice type")

	require.Panicsf(t, func() {
		_, _ = db.Table(UsersTable).Select("name").WhereIn("points", []int64{123, 1234}).AndWhereIn("id", DataStruct{}).Get()
	}, "interfaceToSlice() given a non-slice type")

	require.Panicsf(t, func() {
		_, _ = db.Table(UsersTable).Select("name").WhereIn("points", []int64{123, 1234}).OrWhereIn("id", DataStruct{}).Get()
	}, "interfaceToSlice() given a non-slice type")

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_WhereNotIn(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)
	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name").WhereNotIn("points", []int64{123, 1234}).OrWhereNotIn("id", []int64{1, 2}).Get()
	require.NoError(t, err)
	require.Equal(t, len(res), 2)

	require.Panicsf(t, func() {
		_, _ = db.Table(UsersTable).Select("name").WhereNotIn("points", DataStruct{}).OrWhereNotIn("id", []int64{1, 2}).Get()
	}, "interfaceToSlice() given a non-slice type")

	require.Panicsf(t, func() {
		_, _ = db.Table(UsersTable).Select("name").WhereNotIn("points", []int64{123, 1234}).OrWhereNotIn("id", DataStruct{}).Get()
	}, "interfaceToSlice() given a non-slice type")

	res, err = db.Table(UsersTable).Select("name").WhereNotIn("points", []int64{123, 1234}).AndWhereNotIn("id", []int64{1, 2}).Get()
	require.NoError(t, err)
	require.Equal(t, len(res), 2)

	require.Panicsf(t, func() {
		_, _ = db.Table(UsersTable).Select("name").WhereNotIn("points", []int64{123, 1234}).AndWhereNotIn("id", DataStruct{}).Get()
	}, "interfaceToSlice() given a non-slice type")

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_WhereNull(t *testing.T) {
	_, err := db.Truncate(PostsTable)
	require.NoError(t, err)

	err = db.Table(PostsTable).InsertBatch(batchPosts)
	require.NoError(t, err)

	res, err := db.Table(PostsTable).Select("title").WhereNull("post").AndWhereNull("user_id").Get()
	db.Dump()
	require.NoError(t, err)
	require.Equal(t, len(res), 1)

	res, err = db.Table(PostsTable).Select("title").WhereNull("post").OrWhereNull("user_id").Get()
	require.NoError(t, err)
	require.Equal(t, len(res), 1)

	_, err = db.Truncate(PostsTable)
	require.NoError(t, err)
}

func TestDB_WhereNotNull(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name").WhereNotNull("points").AndWhereNotNull("name").Get()
	require.NoError(t, err)
	require.Equal(t, len(res), len(batchUsers))

	res, err = db.Table(UsersTable).Select("name").WhereNotNull("points").OrWhereNotNull("name").Get()
	require.NoError(t, err)
	require.Equal(t, len(res), len(batchUsers))

	res, err = db.Table(UsersTable).Select("name").Where("id", "=", 1).
		OrWhere("id", "=", 2).AndWhereNotNull("points").Get()
	require.NoError(t, err)
	require.Equal(t, len(res), 2)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_LockForUpdate(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name").LockForUpdate().Get()
	require.NoError(t, err)
	require.Equal(t, len(res), len(batchUsers))

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_UnionAll(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name").UnionAll().Table(UsersTable).Select("name").Get()
	require.NoError(t, err)
	require.Equal(t, len(res), len(batchUsers)*2)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_FullOuterJoin(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	_, err = db.Truncate(PostsTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	res, err := db.Table(UsersTable).Select("name").FullOuterJoin(PostsTable, UsersTable+".id", "=",
		PostsTable+".user_id").Get()
	require.NoError(t, err)
	require.Equal(t, len(res), len(batchUsers))

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_Chunk(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	var sumOfPoints int64
	dataStruct := &DataStructUser{}
	err = db.Table(UsersTable).Select("name", "points").Chunk(dataStruct, 2, func(users []any) bool {
		for _, v := range users {
			user := v.(DataStructUser)
			sumOfPoints += user.Points
		}

		return true
	})
	require.NoError(t, err)

	var initialSum int64
	for _, mm := range batchUsers {
		initialSum += mm.Points
	}
	require.Equal(t, sumOfPoints, initialSum)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_ChunkFalse(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)
	var sumOfPoints int64
	dataStruct := &DataStructUser{}
	err = db.Table(UsersTable).Select("name", "points").Chunk(dataStruct, 2, func(users []any) bool {
		for _, v := range users {
			if sumOfPoints > 0 {
				return false
			}

			user := v.(DataStructUser)
			sumOfPoints += user.Points
		}

		return true
	})

	require.NoError(t, err)
	require.Equal(t, sumOfPoints, batchUsers[0].Points)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_ChunkLessThanAmount(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	var sumOfPoints int64
	dataStruct := &DataStructUser{}
	err = db.Table(UsersTable).Select("name", "points").Chunk(dataStruct, int64(len(batchUsers)+1), func(users []any) bool {
		for _, v := range users {
			user := v.(DataStructUser)
			sumOfPoints += user.Points
		}

		return true
	})
	require.NoError(t, err)
	require.Greater(t, sumOfPoints, int64(0))

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_ChunkLessThenZeroErr(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	err = db.Table(UsersTable).InsertBatch(batchUsers)
	require.NoError(t, err)

	var sumOfPoints int64
	dataStruct := &DataStructUser{}
	err = db.Table(UsersTable).Select("name", "points").Chunk(dataStruct, int64(-1), func(users []any) bool {
		for _, v := range users {
			user := v.(DataStructUser)
			sumOfPoints += user.Points
		}

		return true
	})
	require.Errorf(t, err, "chunk can't be <= 0, your chunk is: -1")

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_ChunkBuilderTableErr(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)
	// reset prev set up table as we don't want to use Table to produce err
	db.Builder.table = ""
	err = db.InsertBatch(batchUsers)
	require.Error(t, err, errTableCallBeforeOp)

	_, err = db.Select("foo", "bar", "baz").Get()
	require.Error(t, err, errTableCallBeforeOp)

	err = db.Insert(dataMap)
	require.Error(t, err, errTableCallBeforeOp)

	_, err = db.InsertGetId(dataMap)
	require.Error(t, err, errTableCallBeforeOp)

	_, err = db.Update(dataMap)
	require.Error(t, err, errTableCallBeforeOp)

	_, err = db.Delete()
	require.Error(t, err, errTableCallBeforeOp)

	_, err = db.Replace(dataMap, "id")
	require.Error(t, err, errTableCallBeforeOp)

	_, err = db.Increment("clmn", 123)
	require.Error(t, err, errTableCallBeforeOp)

	_, err = db.Exists()
	require.Error(t, err, errTableCallBeforeOp)

	_, err = db.Table("nonexistent").Update(data)
	require.Error(t, err)

	_, err = db.Table("nonexistent").Delete()
	require.Error(t, err)

	_, err = db.Table("nonexistent").Increment("clmn", 123)
	require.Error(t, err)

	_, err = db.Table("nonexistent").Replace(data, "id")
	require.Error(t, err)

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_FirsNoRecordsErr(t *testing.T) {
	_, err := db.Truncate(UsersTable)
	require.NoError(t, err)

	dataStruct := &DataStructUser{}
	err = db.Table(TestTable).Select("baz").OrderBy("baz", "desc").
		OrderBy("foo", "desc").First(dataStruct)
	require.Errorf(t, err, "no records were produced by query: %s")

	_, err = db.Truncate(UsersTable)
	require.NoError(t, err)
}

func TestDB_ScanStruct(t *testing.T) {
	_, err := db.Truncate(TestTable)
	require.NoError(t, err)

	err = db.Table(TestTable).Insert(data)
	require.NoError(t, err)

	type TestStruct struct {
		FooSomeCol string `db:"foo"`
		Bar        string `db:"bar"`
		Baz        int64
		Title      string
	}

	testStruct := TestStruct{}
	err = db.Table(TestTable).Select("foo", "bar", "baz").ScanStruct(&testStruct)
	require.NoError(t, err)

	require.Equal(t, testStruct.FooSomeCol, data.Foo)
	require.Equal(t, testStruct.Bar, data.Bar)
	require.Equal(t, testStruct.Baz, *data.Baz)
	require.Empty(t, testStruct.Title)

	err = db.Table(TestTable).Insert(data)
	require.NoError(t, err)

	var testStructs []TestStruct
	err = db.Table(TestTable).Select("foo", "bar", "baz").EachToStruct(func(rows *sql.Rows) error {
		err = db.Next(rows, &testStruct)
		if err != nil {
			return err
		}

		testStructs = append(testStructs, testStruct)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, testStructs, 2)
	for _, val := range testStructs {
		require.Equal(t, val.FooSomeCol, data.Foo)
		require.Equal(t, val.Bar, data.Bar)
		require.Equal(t, val.Baz, *data.Baz)
		require.Empty(t, val.Title)
	}

	_, err = db.Truncate(TestTable)
	require.NoError(t, err)
}
