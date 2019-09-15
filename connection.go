package buildsqlx

import (
	"database/sql"
	_ "github.com/lib/pq" // to prepare PostgreSQL driver
	"log"
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
