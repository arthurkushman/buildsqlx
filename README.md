# arsqlx
Go Active Record library for postgresql

[![Go Report Card](https://goreportcard.com/badge/github.com/arthurkushman/arsqlx)](https://goreportcard.com/report/github.com/arthurkushman/arsqlx)
[![GoDoc](https://github.com/golang/gddo/blob/c782c79e0a3c3282dacdaaebeff9e6fd99cb2919/gddo-server/assets/status.svg)](https://godoc.org/github.com/arthurkushman/arsqlx)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Selects, Ordering, Grouping, Limit & Offset

You may not always want to select all columns from a database table. Using the select method, you can specify a custom select clause for the query:

```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"arsqlx"
) 

var db = arsqlx.NewDb(arsqlx.NewConnection("postgres", "user=postgres dbname=postgres password=postgres sslmode=disable"))

qDb := db.Table("table1").Select("foo", "bar")

// If you already have a query builder instance and you wish to add a column to its existing select clause, you may use the addSelect method:
res, err := qDb.AddSelect("baz").GroupBy("foo").OrderBy("bar", "DESC").Limit(15).Offset(5).Get()
```

## Where, AndWhere, OrWhere clauses
You may use the where method on a query builder instance to add where clauses to the query. 
The most basic call to where requires three arguments. 
The first argument is the name of the column. 
The second argument is an operator, which can be any of the database's supported operators. 
Finally, the third argument is the value to evaluate against the column.

```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"arsqlx"
)

res, err := db.Table("table1").Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").OrWhere("baz", "=", 123).Get()
```

You may chain where constraints together as well as add or clauses to the query. 
The orWhere method accepts the same arguments as the where method.

## WhereIn / WhereNotIn / OrWhereIn / OrWhereNotIn

res, err := db.Table("table1").WhereIn("id", []int64{1, 2, 3}).OrWhereIn("name", []string{"John", "Paul"}).Get()

## WhereNull / WhereNotNull / WhereNull / WhereNotNull 

res, err := db.Table("table1").WhereNull("name").OrWhereNotNull("title").Get()

## Joins
```go
res, err := db.Table("users").Select("name", "post", "user_id").LeftJoin("posts", "users.id", "=", "posts.user_id").Get()
```

## Inserts

The query builder also provides an insert method for inserting records into the database table. 
The insert method accepts a map of column names and values:

```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"arsqlx"
)

// insert without getting id
err := db.Table("table1").Insert(map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": int64(123)})

// insert returning id
id, err := db.Table("table1").InsertGetId(map[string]interface{}{"foo": "foo foo foo", "bar": "bar bar bar", "baz": int64(123)})

// batch insert 
err := db.Table("table1").InsertBatch([]map[string]interface{}{
                                    	0: {"foo": "foo foo foo", "bar": "bar bar bar", "baz": 123},
                                    	1: {"foo": "foo foo foo foo", "bar": "bar bar bar bar", "baz": 1234},
                                    	2: {"foo": "foo foo foo foo foo", "bar": "bar bar bar bar bar", "baz": 12345},
                                    })
```

## Drop, Truncate, Rename
```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"arsqlx"
)

db.Drop("table_name")

db.DropIfExists("table_name")

db.Truncate("table_name")

db.Rename("table_name1", "table_name2")
```