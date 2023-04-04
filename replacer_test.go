package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"ch-migrator/mocks"
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
