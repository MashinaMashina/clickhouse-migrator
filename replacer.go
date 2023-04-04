package main

import (
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
)

//go:generate go run github.com/vektra/mockery/v2@v2.23.1 --name Driver
type Driver interface {
	database.Driver
}

var isDistributed = regexp.MustCompile(`(?is)engine = distributed\(.+?,.+?,(.+?),.+?\)\s*;`)
var tableOrder = regexp.MustCompile(`(?is)create table (?:if not exists |)(.+?)\(.+?order by\s*\(([^\)]+?)\)`)

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

	str := string(content)

	str = strings.NewReplacer(
		"ON CLUSTER '{cluster}'", "",
		"on cluster '{cluster}'", "",
		"defaultdb.", "",
	).Replace(str)

	if distMatch := isDistributed.FindAllStringSubmatch(str, -1); len(distMatch) > 0 {
		ordersMatch := tableOrder.FindAllStringSubmatch(str, -1)

		orders := make(map[string]string, len(ordersMatch))
		for _, order := range ordersMatch {
			orders[strings.TrimSpace(order[1])] = order[2]
		}

		replace := make([]string, 0, len(distMatch)*2)
		for _, dist := range distMatch {
			table := strings.Trim(dist[1], "\r\n\t '\"")
			if order, ok := orders[table]; ok {
				replace = append(replace,
					dist[0], fmt.Sprintf("engine = MergeTree order by (%s);", order),
				)
			}
		}

		str = strings.NewReplacer(replace...).Replace(str)
	}

	return d.Driver.Run(strings.NewReader(str))
}
