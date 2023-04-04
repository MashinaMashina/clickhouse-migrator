package main

import (
	"io"
	"strings"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
)

//go:generate go run github.com/vektra/mockery/v2@v2.23.1 --name Driver
type Driver interface {
	database.Driver
}

func init() {
	database.Register("clickhouse2", &MigrationDriver{&clickhouse.ClickHouse{}})
}

type MigrationDriver struct {
	database.Driver
}

func (d *MigrationDriver) Open(dsn string) (database.Driver, error) {
	driver, err := d.Driver.Open(dsn)
	if err != nil {
		return nil, err
	}

	return &MigrationDriver{
		driver,
	}, err
}

func (d *MigrationDriver) Run(r io.Reader) error {
	content, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	r = strings.NewReader(replaceText(string(content)))
	return d.Driver.Run(r)
}
