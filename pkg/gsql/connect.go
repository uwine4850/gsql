package gsql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"gsql/pkg/gsql/engine"
)

type Connector struct {
	Db engine.SqlEngine
}

func (conn Connector) Connect() (*sql.DB, error) {
	if db, err := sql.Open("mysql", conn.Db.Init()); err != nil {
		return nil, err
	} else {
		return db, err
	}
}
