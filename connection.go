package buildsqlx

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq" // to prepare PostgreSQL driver
)

// Connection encloses DB struct
type Connection struct {
	db *sql.DB
}

// NewConnection returns pre-defined Connection structure
func NewConnection(driverName, dataSourceName string) *Connection {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Fatalln(err)
	}

	return &Connection{db: db}
}

// NewConnectionFromDb returns re-defined Connection structure created via db handle with connection(s)
func NewConnectionFromDb(db *sql.DB) *Connection {
	return &Connection{db: db}
}
