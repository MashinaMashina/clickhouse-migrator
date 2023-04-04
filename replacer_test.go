package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"clickhouse-migrator/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestReplacer(t *testing.T) {
	input, err := os.ReadFile("./testdata/input.sql")
	if err != nil {
		require.NoError(t, err, "opening file ./testdata/input.sql")
		return
	}

	output, err := os.ReadFile("./testdata/output.sql")
	if err != nil {
		require.NoError(t, err, "opening file ./testdata/output.sql")
		return
	}

	driver := mocks.NewDriver(t)
	driver.On("Run", mock.Anything).Return(func(body io.Reader) error {
		b, err := io.ReadAll(body)

		require.NoError(t, err)
		require.Equal(t, string(output), string(b))

		return err
	})

	r := bytes.NewReader(input)

	migrator := &MigrationDriver{driver}
	migrator.Run(r)
}

func TestPrepareMessage(t *testing.T) {
	cases := []struct {
		In  string
		Out string
	}{
		{
			"",
			"",
		},
		{
			"-- ALTER TABLE products_v1  DROP INDEX orders_n_idx;",
			"-- ALTER TABLE products_v1  DROP INDEX orders_n_idx;",
		},
		{
			"-- alter table category_products  add index idx_days_of_month_mask days_of_month_mask type minmax granularity 1;",
			"-- alter table category_products  add index i ... days_of_month_mask type minmax granularity 1;",
		},
	}

	for _, testCase := range cases {
		out := prepareBadCommentMessage(testCase.In)

		require.Equal(t, testCase.Out, out)
	}
}
