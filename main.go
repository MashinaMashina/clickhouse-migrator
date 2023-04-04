package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name: "clickhouse-migrator",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "source",
				Value: ".",
				Usage: "migration files path",
			},
			&cli.StringFlag{
				Name:  "dsn",
				Value: "localhost:9000?x-multi-statement=true",
				Usage: "host:port?username=user&password=password&database=clicks&x-multi-statement=true",
			},
		},
		Action: Copy,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
