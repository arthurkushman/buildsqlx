# arsqlx
Go Active Record library for postgresql

[![Go Report Card](https://goreportcard.com/badge/github.com/arthurkushman/arsqlx)](https://goreportcard.com/report/github.com/arthurkushman/arsqlx)
[![GoDoc](https://github.com/golang/gddo/blob/c782c79e0a3c3282dacdaaebeff9e6fd99cb2919/gddo-server/assets/status.svg)](https://godoc.org/github.com/arthurkushman/arsqlx)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Selects, Ordering, Limit & Offset

You may not always want to select all columns from a database table. Using the select method, you can specify a custom select clause for the query:

```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"arsqlx"
) 

var db = arsqlx.NewDb(arsqlx.NewConnection("postgres", "user=postgres dbname=postgres password=postgres sslmode=disable"))

func main() {
    qDb := db.Table("table1").Select("foo", "bar")

    // If you already have a query builder instance and you wish to add a column to its existing select clause, you may use the addSelect method:
    res, err := qDb.AddSelect("baz").GroupBy("foo").OrderBy("bar", "DESC").Limit(15).Offset(5).Get()
}
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
	"arsqlx"
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
res, err := db.Table("table1").WhereNull("name").OrWhereNotNull("title").Get()
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
	"arsqlx"
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
rows, err := db.Table(TestTable).Where("foo", "=", "foo foo foo").Update(map[string]interface{}{"foo": "foo changed"})
```

## Delete
The query builder may also be used to delete records from the table via the delete method. 
You may constrain delete statements by adding where clauses before calling the delete method:
```go
rows, err := db.Table(TestTable).Where("baz", "=", 123).Delete()
```

## Drop, Truncate, Rename
```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"arsqlx"
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

## Dump, Dd
You may use the Dd or Dump methods while building a query to dump the query bindings and SQL. 
The dd method will display the debug information and then stop executing the request. 
The dump method will display the debug information but allow the request to keep executing:
```go
package yourpackage

import (
	_ "github.com/lib/pq"
	"arsqlx"
)

func main() {
	// to print raw sql query to stdout 
	db.Table("table_name").Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").Dump()
	
	// or to print to stdout and exit a.k.a dump and die
	db.Table("table_name").Select("foo", "bar", "baz").Where("foo", "=", cmp).AndWhere("bar", "!=", "foo").Dd() 
}
```