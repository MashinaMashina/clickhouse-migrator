package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/urfave/cli/v2"
)

var ErrNotFoundFiles = fmt.Errorf("not found sql files")

func Copy(ctx *cli.Context) error {
	source, err := filepath.Abs(ctx.String("source"))
	if err != nil {
		return fmt.Errorf("getting absolute path: %w", err)
	}

	files, err := os.ReadDir(source)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", source, err)
	}

	if len(files) == 0 {
		return fmt.Errorf("%w: %s", ErrNotFoundFiles, source)
	}

	targetDir := fmt.Sprintf("%s/modify", source)
	if _, err = os.Stat(targetDir); errors.Is(err, os.ErrNotExist) {
		if err = os.Mkdir(targetDir, 0777); err != nil {
			return fmt.Errorf("create new dir: %w", err)
		}
	}

	foundFiles := false
	for _, file := range files {
		if file.IsDir() || path.Ext(file.Name()) != ".sql" {
			continue
		}

		foundFiles = true

		bytes, err := os.ReadFile(fmt.Sprintf("%s/%s", source, file.Name()))
		if err != nil {
			return fmt.Errorf("read %s: %w", file.Name(), err)
		}

		content := replaceText(string(bytes))

		err = os.WriteFile(fmt.Sprintf("%s/%s", targetDir, file.Name()), []byte(content), 0777)
		if err != nil {
			return fmt.Errorf("write file %s: %w", file.Name(), err)
		}
	}

	if !foundFiles {
		return fmt.Errorf("%w: %s", ErrNotFoundFiles, source)
	}

	log.Printf("success")
	return nil
}
