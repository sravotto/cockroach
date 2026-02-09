// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// BenchmarkParquetImport benchmarks IMPORT INTO ... PARQUET DATA performance.
//
// This benchmark measures end-to-end import throughput including:
// - Parquet file reading and decompression
// - Type conversion and validation
// - KV generation
// - Index building
//
// Generates ~25 MiB of Parquet data (similar to CSV benchmark) with 1M rows.
//
// Run with:
//   ./dev bench pkg/sql/importer -f BenchmarkParquetImport
//
// Example results:
// BenchmarkParquetImport/uncompressed-16    1    X ns/op    Y.YY MB/s
// BenchmarkParquetImport/gzip-16            1    X ns/op    Y.YY MB/s
// BenchmarkParquetImport/snappy-16          1    X ns/op    Y.YY MB/s
func BenchmarkParquetImport(b *testing.B) {
	defer log.Scope(b).Close(b)
	skip.UnderShort(b, "skipping long benchmark")
	skip.UnderRace(b, "takes >1min under race")

	// Benchmark different compression formats
	testCases := []struct {
		name  string
		codec compress.Compression
	}{
		{"uncompressed", compress.Codecs.Uncompressed},
		{"gzip", compress.Codecs.Gzip},
		{"snappy", compress.Codecs.Snappy},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkParquetImport(b, tc.codec)
		})
	}
}

func benchmarkParquetImport(b *testing.B, codec compress.Compression) {
	const nodes = 3
	ctx := context.Background()

	baseDir, cleanup := testutils.TempDir(b)
	defer cleanup()

	// Every row (int, string) is ~25 bytes (similar to CSV).
	// Generate 1M rows = ~25 MiB of data.
	numRows := 1 * 1024 * 1024

	// Generate Parquet file
	parquetData := generateParquetData(b, numRows, codec)

	// Write to temp file
	f, err := os.CreateTemp(baseDir, "bench_data*.parquet")
	require.NoError(b, err)
	testFileName := filepath.Base(f.Name())
	_, err = f.Write(parquetData)
	require.NoError(b, err)
	require.NoError(b, f.Close())

	// Set bytes for MB/s calculation
	b.SetBytes(int64(len(parquetData)))

	// Start cluster
	tc := serverutils.StartCluster(b, nodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			ExternalIODir: baseDir,
		},
	})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Reset timer after setup
	b.ResetTimer()

	// Create table
	sqlDB.Exec(b, `CREATE TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))`)

	// Execute import
	sqlDB.Exec(b, fmt.Sprintf(`IMPORT INTO t PARQUET DATA ('nodelocal://1/%s')`, testFileName))

	b.StopTimer()
}

// generateParquetData generates a Parquet file in memory with the specified number of rows.
// Schema: (a INT64, b STRING) - same as CSV benchmark.
func generateParquetData(b *testing.B, numRows int, codec compress.Compression) []byte {
	// Create Arrow schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "b", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	// Write to buffer
	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(codec))
	pw, err := pqarrow.NewFileWriter(schema, &buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(b, err)

	// Build record in batches to avoid excessive memory usage
	const batchSize = 100000
	for offset := 0; offset < numRows; offset += batchSize {
		rowsInBatch := batchSize
		if offset+batchSize > numRows {
			rowsInBatch = numRows - offset
		}

		recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		int64Builder := recordBuilder.Field(0).(*array.Int64Builder)
		stringBuilder := recordBuilder.Field(1).(*array.StringBuilder)

		for i := 0; i < rowsInBatch; i++ {
			x := offset + i
			int64Builder.Append(int64(x))
			// Generate string similar to CSV: cycle through alphabet
			stringBuilder.Append(string(rune('A' + x%26)))
		}

		record := recordBuilder.NewRecord()
		require.NoError(b, pw.Write(record))
		record.Release()
		recordBuilder.Release()
	}

	require.NoError(b, pw.Close())
	return buf.Bytes()
}
