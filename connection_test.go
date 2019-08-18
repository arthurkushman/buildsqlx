package arsqlx

import (
	"fmt"
	_ "github.com/lib/pq" // here
	"testing"
)

type Test struct {
	Foo string `db:"foo"`
	Bar string `db:"bar"`
	Baz int64  `db:"baz"`
}

var db = NewDb(NewConnection("postgres", "user=postgres dbname=postgres password=postgres sslmode=disable"))

func TestSelectAndLimit(t *testing.T) {
	test := Test{}
	qDb := db.Table("test").Select("foo", "bar")

	res, err := qDb.AddSelect("baz").Limit(15).Get(&test)

	if err != nil {
		t.Fatal(err)
	}

	for _, v := range res {
		fmt.Printf("struct: %v", v)
	}
}

func TestInsert(t *testing.T) {
	err := db.Table("test").Insert(map[string]interface{}{"foo": "blaaaaa 123", "bar": "bzzzzzzzzzzz", "baz": 1234})

	if err != nil {
		t.Fatal(err)
	}

}

func TestSelectRaw(t *testing.T) {

}

func TestWhereAndOr(t *testing.T) {

}
