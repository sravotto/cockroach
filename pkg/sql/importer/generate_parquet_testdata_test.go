// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

var rewriteParquetTestData = envutil.EnvOrDefaultBool("COCKROACH_REWRITE_PARQUET_TESTDATA", false)

type parquetTestFiles struct {
	files, gzipFiles, snappyFiles, filesWithDups []string
	filesUsingWildcard                           []string
}

func getParquetTestFiles(numFiles int) parquetTestFiles {
	var testFiles parquetTestFiles
	suffix := ""
	if util.RaceEnabled {
		suffix = "-race"
	}
	for i := 0; i < numFiles; i++ {
		testFiles.files = append(testFiles.files, fmt.Sprintf(`'nodelocal://1/%s'`, fmt.Sprintf("data-%d%s.parquet", i, suffix)))
		testFiles.gzipFiles = append(testFiles.gzipFiles, fmt.Sprintf(`'nodelocal://1/%s'`, fmt.Sprintf("data-%d-gzip%s.parquet", i, suffix)))
		testFiles.snappyFiles = append(testFiles.snappyFiles, fmt.Sprintf(`'nodelocal://1/%s'`, fmt.Sprintf("data-%d-snappy%s.parquet", i, suffix)))
		testFiles.filesWithDups = append(testFiles.filesWithDups, fmt.Sprintf(`'nodelocal://1/%s'`, fmt.Sprintf("data-%d-dup%s.parquet", i, suffix)))
	}

	wildcardFileName := "data-[0-9]"
	testFiles.filesUsingWildcard = append(testFiles.filesUsingWildcard, fmt.Sprintf(`'nodelocal://1/%s%s.parquet'`, wildcardFileName, suffix))

	return testFiles
}

func makeParquetData(
	t testing.TB, numFiles, rowsPerFile, numRaceFiles, rowsPerRaceFile int,
) parquetTestFiles {
	if rewriteParquetTestData {
		dir := datapathutils.TestDataPath(t, "parquet")
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(dir, 0777); err != nil {
			t.Fatal(err)
		}

		generateParquetFiles(t, dir, numFiles, rowsPerFile, "")
		generateParquetFiles(t, dir, numRaceFiles, rowsPerRaceFile, "-race")
	}

	if util.RaceEnabled {
		return getParquetTestFiles(numRaceFiles)
	}
	return getParquetTestFiles(numFiles)
}

// TestGenerateParquetTestData generates test data files when COCKROACH_REWRITE_PARQUET_TESTDATA=true.
// Run with: COCKROACH_REWRITE_PARQUET_TESTDATA=true ./dev test pkg/sql/importer -f TestGenerateParquetTestData
func TestGenerateParquetTestData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if !rewriteParquetTestData {
		t.Skip("Set COCKROACH_REWRITE_PARQUET_TESTDATA=true to generate test data")
	}

	dir := datapathutils.TestDataPath(t, "parquet")

	// Clean up old test files
	oldFiles := []string{
		"data-*.parquet", "data-*.parquet.gz", "empty*.parquet",
	}
	for _, pattern := range oldFiles {
		matches, _ := filepath.Glob(filepath.Join(dir, pattern))
		for _, match := range matches {
			_ = os.Remove(match)
		}
	}

	t.Logf("Generating Parquet test data in %s", dir)

	// Generate regular files and race files
	generateParquetFiles(t, dir, 5, 1000, "" /* suffix */)
	generateParquetFiles(t, dir, 3, 16, "-race")

	t.Logf("✅ Test data generation complete!")
	t.Logf("Generated files:")
	matches, _ := filepath.Glob(filepath.Join(dir, "data-*.parquet*"))
	for _, match := range matches {
		info, _ := os.Stat(match)
		t.Logf("  - %s (%d bytes)", filepath.Base(match), info.Size())
	}
}

func generateParquetFiles(t testing.TB, dir string, numFiles, rowsPerFile int, suffix string) {
	for fn := 0; fn < numFiles; fn++ {
		// Generate rows for this file
		var aValues []int64
		var bValues []string
		for i := 0; i < rowsPerFile; i++ {
			x := fn*rowsPerFile + i
			aValues = append(aValues, int64(x))
			bValues = append(bValues, string(rune('A'+x%26)))
		}

		// Regular uncompressed file
		fileName := filepath.Join(dir, fmt.Sprintf("data-%d%s.parquet", fn, suffix))
		writeParquetFile(t, fileName, aValues, bValues, compress.Codecs.Uncompressed)

		// Gzip compressed file (internal Parquet compression)
		gzipName := filepath.Join(dir, fmt.Sprintf("data-%d-gzip%s.parquet", fn, suffix))
		writeParquetFile(t, gzipName, aValues, bValues, compress.Codecs.Gzip)

		// Snappy compressed file (internal Parquet compression)
		snappyName := filepath.Join(dir, fmt.Sprintf("data-%d-snappy%s.parquet", fn, suffix))
		writeParquetFile(t, snappyName, aValues, bValues, compress.Codecs.Snappy)

		// Duplicate key file (all rows have a=1)
		dupA := make([]int64, rowsPerFile)
		for i := range dupA {
			dupA[i] = 1
		}
		dupName := filepath.Join(dir, fmt.Sprintf("data-%d-dup%s.parquet", fn, suffix))
		writeParquetFile(t, dupName, dupA, bValues, compress.Codecs.Uncompressed)
	}

	// TODO: Create empty.parquet file
	// Apache Arrow's pqarrow library has issues creating 0-row files
	// For now, create it manually or use a pre-existing one
	t.Logf("Note: empty.parquet must be created manually")
}

func writeParquetFile(
	t testing.TB, fileName string, aValues []int64, bValues []string, codec compress.Compression,
) {
	require.Equal(t, len(aValues), len(bValues), "aValues and bValues must have same length")

	// Create Arrow schema matching our test table (a INT8, b STRING)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "b", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	// Write to buffer first
	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(codec))
	pw, err := pqarrow.NewFileWriter(schema, &buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	// Build Arrow record
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer recordBuilder.Release()

	int64Builder := recordBuilder.Field(0).(*array.Int64Builder)
	stringBuilder := recordBuilder.Field(1).(*array.StringBuilder)

	for i := range aValues {
		int64Builder.Append(aValues[i])
		stringBuilder.Append(bValues[i])
	}

	record := recordBuilder.NewRecord()
	defer record.Release()

	require.NoError(t, pw.Write(record))
	require.NoError(t, pw.Close())

	// Write buffer to file
	require.NoError(t, os.WriteFile(fileName, buf.Bytes(), 0644))
	t.Logf("Created %s (%d rows, %s)", filepath.Base(fileName), len(aValues), codec)
}

