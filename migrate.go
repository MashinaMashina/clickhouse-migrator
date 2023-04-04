package main

import (
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/urfave/cli/v2"
)

func Migrate(ctx *cli.Context) error {
	source := ctx.String("source")
	if !strings.Contains(source, "://") {
		source = "file://" + source
	}

	dsn := "clickhouse2://" + ctx.String("dsn")

	migrator, err := migrate.New(source, dsn)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}

	return migrator.Up()
}
