package arsqlx

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
)

type Connection struct {
	db *sql.DB
}

func NewConnection(driverName, dataSourceName string) *Connection {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Fatalln(err)
	}

	return &Connection{db: db}
}
