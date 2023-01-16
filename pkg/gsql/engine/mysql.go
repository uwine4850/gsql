package engine

import "fmt"

type MySqlConnector struct {
	Username string
	Password string
	Addr     string
	Database string
}

func (i *MySqlConnector) Init() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", i.Username, i.Password, i.Addr, i.Database)
}
