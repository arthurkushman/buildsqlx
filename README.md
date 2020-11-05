# buildsqlx
Go Database query builder library [![Tweet](http://jpillora.com/github-twitter-button/img/tweet.png)](https://twitter.com/intent/tweet?text=Go%20database%20query%20builder%20library%20&url=https://github.com/arthurkushman/buildsqlx&hashtags=go,golang,sql,builder,postgresql,sql-builder,developers)

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/arthurkushman/buildsqlx)](https://goreportcard.com/report/github.com/arthurkushman/buildsqlx)
[![Build and run](https://github.com/arthurkushman/buildsqlx/workflows/Build%20and%20run/badge.svg)](https://github.com/arthurkushman/buildsqlx/actions)
[![GoDoc](https://github.com/golang/gddo/blob/c782c79e0a3c3282dacdaaebeff9e6fd99cb2919/gddo-server/assets/status.svg)](https://godoc.org/github.com/arthurkushman/buildsqlx)
[![codecov](https://codecov.io/gh/arthurkushman/buildsqlx/branch/master/graph/badge.svg)](https://codecov.io/gh/arthurkushman/buildsqlx)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

* [Installation](#user-content-installation)
* [Selects, Ordering, Limit & Offset](#user-content-selects-ordering-limit--offset)
* [GroupBy / Having](#user-content-groupby--having)
* [Where, AndWhere, OrWhere clauses](#user-content-where-andwhere-orwhere-clauses)
* [WhereIn / WhereNotIn](#user-content-wherein--wherenotin)
* [WhereNull / WhereNotNull](#user-content-wherenull--wherenotnull)
* [Left / Right / Cross / Inner / Left Outer Joins](#user-content-left--right--cross--inner--left-outer-joins)
* [Inserts](#user-content-inserts)
* [Updates](#user-content-updates)
* [Delete](#user-content-delete)
* [Drop, Truncate, Rename](#user-content-drop-truncate-rename)
* [Increment & Decrement](#user-content-increment--decrement)
* [Union / Union All](#user-content-union--union-all)
* [Transaction mode](#user-content-transaction-mode)
* [Dump, Dd](#user-content-dump-dd)
* [Check if table exists](#user-content-check-if-table-exists)
* [Check if columns exist in a table within schema](#user-content-check-if-columns-exist-in-a-table-within-schema)
* [Retrieving A Single Row / Column From A Table](#user-content-retrieving-a-single-row--column-from-a-table)
* [WhereExists / WhereNotExists](#user-content-whereexists--wherenotexists)
* [Determining If Records Exist](#user-content-determining-if-records-exist)
* [Aggregates](#user-content-aggregates)
* [Create table](#user-content-create-table)
* [Add / Modify / Drop columns](#user-content-add--modify--drop-columns)
* [Chunking Results](#user-content-chunking-results)

## Installation
```bash
go get -u github.com/arthurkushman/buildsqlx
```

## Selects, Ordering, Limit & Offset

You may not always want to select all columns from a database table. Using the select method, you can specify a custom select clause for the query:

```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"github.com/arthurkushman/buildsqlx"
) 

var db = buildsqlx.NewDb(buildsqlx.NewConnection("postgres", "user=postgres dbname=postgres password=postgres sslmode=disable"))

func main() {
    qDb := db.Table("posts").Select("title", "body")

    // If you already have a query builder instance and you wish to add a column to its existing select clause, you may use the addSelect method:
    res, err := qDb.AddSelect("points").GroupBy("topic").OrderBy("points", "DESC").Limit(15).Offset(5).Get()
}
```

### InRandomOrder
```go
res, err = db.Table("users").Select("name", "post", "user_id").InRandomOrder().Get()
```

## GroupBy / Having
The GroupBy and Having methods may be used to group the query results. 
The having method's signature is similar to that of the where method:
```go
res, err := db.table("users").GroupBy("account_id").Having("account_id", ">", 100).Get()
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
	"github.com/arthurkushman/buildsqlx"
)

func main() {
    res, err := db.Table("table1").Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").OrWhere("baz", "=", 123).Get()
}
```

You may chain where constraints together as well as add or clauses to the query. 
The orWhere method accepts the same arguments as the where method.

## WhereIn / WhereNotIn 
The whereIn method verifies that a given column's value is contained within the given slice:
```go
res, err := db.Table("table1").WhereIn("id", []int64{1, 2, 3}).OrWhereIn("name", []string{"John", "Paul"}).Get()
```

## WhereNull / WhereNotNull  
The whereNull method verifies that the value of the given column is NULL:
```go
res, err := db.Table("posts").WhereNull("points").OrWhereNotNull("title").Get()
```

## Left / Right / Cross / Inner / Left Outer Joins
The query builder may also be used to write join statements. 
To perform a basic "inner join", you may use the InnerJoin method on a query builder instance. 
The first argument passed to the join method is the name of the table you need to join to, 
while the remaining arguments specify the column constraints for the join. 
You can even join to multiple tables in a single query:
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
	"github.com/arthurkushman/buildsqlx"
)

func main() {
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
}
```

## Updates
In addition to inserting records into the database, 
the query builder can also update existing records using the update method. 
The update method, like the insert method, accepts a slice of column and value pairs containing the columns to be updated. 
You may constrain the update query using where clauses:
```go
rows, err := db.Table("posts").Where("points", ">", 3).Update(map[string]interface{}{"title": "awesome"})
```

## Delete
The query builder may also be used to delete records from the table via the delete method. 
You may constrain delete statements by adding where clauses before calling the delete method:
```go
rows, err := db.Table("posts").Where("points", "=", 123).Delete()
```

## Drop, Truncate, Rename
```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"github.com/arthurkushman/buildsqlx"
)

func main() {
    db.Drop("table_name")

    db.DropIfExists("table_name")

    db.Truncate("table_name")

    db.Rename("table_name1", "table_name2")
}
```

## Increment & Decrement

The query builder also provides convenient methods for incrementing or decrementing the value of a given column. 
This is a shortcut, providing a more expressive and terse interface compared to manually writing the update statement.

Both of these methods accept 2 arguments: the column to modify, a second argument to control the amount by which the column should be incremented or decremented:

```go
db.Table("users").Increment("votes", 3)

db.Table("users").Decrement("votes", 1)
```

## Union / Union All
The query builder also provides a quick way to "union" two queries together. 
For example, you may create an initial query and use the union method to union it with a second query:
```go
union := db.Table("posts").Select("title", "likes").Union()
res, err := union.Table("users").Select("name", "points").Get()

// or if UNION ALL is of need
// union := db.Table("posts").Select("title", "likes").UnionAll()
```

## Transaction mode
You can run arbitrary queries mixed with any code in transaction mode getting an error and as a result rollback if something went wrong
or committed if everything is ok:  
```go
err := db.InTransaction(func() (interface{}, error) {
    return db.Table("users").Select("name", "post", "user_id").Get()
})
```

## Dump, Dd
You may use the Dd or Dump methods while building a query to dump the query bindings and SQL. 
The dd method will display the debug information and then stop executing the request. 
The dump method will display the debug information but allow the request to keep executing:
```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"github.com/arthurkushman/buildsqlx"
)

func main() {
	// to print raw sql query to stdout 
	db.Table("table_name").Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").Dump()
	
	// or to print to stdout and exit a.k.a dump and die
	db.Table("table_name").Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").Dd() 
}
```

## Check if table exists
```go
tblExists, err := db.HasTable("public", "posts")
```

## Check if columns exist in a table within schema
```go
colsExists, err := db.HasColumns("public", "posts", "title", "user_id")
```

## Retrieving A Single Row / Column From A Table
If you just need to retrieve a single row from the database table, you may use the `First` func. 
This method will return a single `map[string]interface{}`:
```go
res, err := db.Table("posts").Select("title").OrderBy("created_at", "desc").First()

// usage ex: res["title"]
```
If you don't even need an entire row, you may extract a single value from a record using the `Value` method. 
This method will return the value of the column directly:
```go
res, err := db.Table("users").OrderBy("points", "desc").Value("name")

// res -> "Alex Shmidt"
```

## WhereExists / WhereNotExists
The whereExists method allows you to write where exists SQL clauses. 
The whereExists method accepts a *DB argument, 
which will receive a query builder instance allowing you to define the query that should be placed inside of the "exists" clause:
```go
res, er := db.Table("users").Select("name").WhereExists(
    db.Table("users").Select("name").Where("points", ">=", int64(12345)),
).First()
```
Any query that is of need to build one can place inside `WhereExists` clause/func.

## WhereBetween / WhereNotBetween
The whereBetween func verifies that a column's value is between two values:
```go
res, err := db.Table(UsersTable).Select("name").WhereBetween("points", 1233, 12345).Get()
```

The whereNotBetween func verifies that a column's value lies outside of two values:
```go
res, err := db.Table(UsersTable).Select("name").WhereNotBetween("points", 123, 123456).Get()
```

## Determining If Records Exist
Instead of using the count method to determine if any records exist that match your query's constraints, 
you may use the exists and doesntExist methods:
```go
exists, err := db.Table(UsersTable).Select("name").Where("points", ">=", int64(12345)).Exists()
// use an inverse DoesntExists() if needed
```

## Aggregates
The query builder also provides a variety of aggregate methods such as Count, Max, Min, Avg, and Sum. 
You may call any of these methods after constructing your query:
```go
cnt, err := db.Table(UsersTable).WHere("points", ">=", 1234).Count()

avg, err := db.Table(UsersTable).Avg("points")

mx, err := db.Table(UsersTable).Max("points")

mn, err := db.Table(UsersTable).Min("points")

sum, err := db.Table(UsersTable).Sum("points")
```

## Create table
To create a new database table, use the CreateTable method. 
The Schema method accepts two arguments. 
The first is the name of the table, while the second is an anonymous function/closure which receives a Table struct that may be used to define the new table:
```go
res, err := db.Schema("big_tbl", func(table *Table) {
    table.Increments("id")
    table.String("title", 128).Default("The quick brown fox jumped over the lazy dog").Unique("idx_ttl")
    table.SmallInt("cnt").Default(1)
    table.Integer("points").NotNull()
    table.BigInt("likes").Index("idx_likes")
    table.Text("comment").Comment("user comment").Collation("de_DE")
    table.DblPrecision("likes_to_points").Default(0.0)
    table.Char("tag", 10)
    table.DateTime("created_at", true)
    table.DateTimeTz("updated_at", true)		
    table.Decimal("tax", 2, 2)
    table.TsVector("body")
    table.TsQuery("body_query")		
    table.Jsonb("settings")
    table.Point("pt")
    table.Polygon("poly")		
    table.TableComment("big table for big data")		
})

// to make a foreign key constraint from another table
_, err = db.Schema("tbl_to_ref", func(table *Table) {
    table.Increments("id")
    table.Integer("big_tbl_id").ForeignKey("fk_idx_big_tbl_id", "big_tbl", "id")
    // to add index on existing column just repeat stmt + index e.g.:
    table.Char("tag", 10).Index("idx_tag")
    table.Rename("settings", "options")
})	
```

## Add / Modify / Drop columns
The Table structure in the Schema's 2nd argument may be used to update existing tables. Just the way you've been created it.
The Change method allows you to modify some existing column types to a new type or modify the column's attributes.
```go
res, err := db.Schema("tbl_name", func(table *Table) {
    table.String("title", 128).Change()
})
```
Use DropColumn method to remove any column:
```go
res, err := db.Schema("tbl_name", func(table *Table) {
    table.DropColumn("deleted_at")
    // To drop an index on the column    
    table.DropIndex("idx_title")
})
```

## Chunking Results
If you need to work with thousands of database records, consider using the chunk method. 
This method retrieves a small chunk of the results at a time and feeds each chunk into a closure for processing.
```go
err = db.Table("user_achievements").Select("points").Where("id", "=", id).Chunk(100, func(users []map[string]interface{}) bool {
    for _, m := range users {
        if val, ok := m["points"];ok {
            pointsCalc += diffFormula(val.(int64))
        }
        // or you can return false here to stop running chunks 
    }
    return true
})
```

PS Why use buildsqlx? Because it is simple and fast, yet versatile. 
The performance achieved because of structs conversion lack, as all that you need is just a columns - u can get it from an associated array/map while the conversion itself and it's processing eats more CPU/memory resources.