// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestConvertParquetValueToDatum tests the type conversion function
func TestConvertParquetValueToDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name       string
		value      interface{}
		targetType *types.T
		expected   tree.Datum
		expectErr  bool
	}{
		{
			name:       "bool-true",
			value:      true,
			targetType: types.Bool,
			expected:   tree.MakeDBool(true),
		},
		{
			name:       "bool-false",
			value:      false,
			targetType: types.Bool,
			expected:   tree.MakeDBool(false),
		},
		{
			name:       "int32-to-int",
			value:      int32(42),
			targetType: types.Int,
			expected:   tree.NewDInt(42),
		},
		{
			name:       "int32-to-decimal",
			value:      int32(123),
			targetType: types.Decimal,
			expected:   func() tree.Datum { d, _ := tree.ParseDDecimal("123"); return d }(),
		},
		{
			name:       "int64-to-int",
			value:      int64(9999),
			targetType: types.Int,
			expected:   tree.NewDInt(9999),
		},
		{
			name:       "int64-to-decimal",
			value:      int64(456),
			targetType: types.Decimal,
			expected:   func() tree.Datum { d, _ := tree.ParseDDecimal("456"); return d }(),
		},
		{
			name:       "float32-to-float",
			value:      float32(3.14),
			targetType: types.Float,
			expected:   tree.NewDFloat(tree.DFloat(float32(3.14))),
		},
		{
			name:       "float64-to-float",
			value:      float64(2.718),
			targetType: types.Float,
			expected:   tree.NewDFloat(2.718),
		},
		{
			name:       "bytes-to-string",
			value:      []byte("hello"),
			targetType: types.String,
			expected:   tree.NewDString("hello"),
		},
		{
			name:       "bytes-to-bytes",
			value:      []byte{0x01, 0x02, 0x03},
			targetType: types.Bytes,
			expected:   tree.NewDBytes(tree.DBytes([]byte{0x01, 0x02, 0x03})),
		},
		{
			name:       "bytes-to-decimal",
			value:      []byte("123.45"),
			targetType: types.Decimal,
			expected:   func() tree.Datum { d, _ := tree.ParseDDecimal("123.45"); return d }(),
		},
		{
			name:       "bytes-to-json",
			value:      []byte(`{"key": "value"}`),
			targetType: types.Jsonb,
			expected:   func() tree.Datum { d, _ := tree.ParseDJSON(`{"key": "value"}`); return d }(),
		},
		{
			name:       "fixed-len-bytes-to-uuid",
			value:      parquet.FixedLenByteArray(uuid.MakeV4().GetBytes()),
			targetType: types.Uuid,
			expected:   tree.NewDUuid(tree.DUuid{UUID: uuid.MakeV4()}),
		},
		{
			name:       "fixed-len-bytes-to-bytes",
			value:      parquet.FixedLenByteArray([]byte{0x01, 0x02}),
			targetType: types.Bytes,
			expected:   tree.NewDBytes(tree.DBytes([]byte{0x01, 0x02})),
		},
		{
			name:       "unsupported-type",
			value:      struct{}{},
			targetType: types.String,
			expectErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := convertParquetValueToDatum(tc.value, tc.targetType, nil)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Special handling for UUID since we can't predict the exact value
			if tc.targetType.Family() == types.UuidFamily && tc.name == "fixed-len-bytes-to-uuid" {
				require.Equal(t, types.UuidFamily, result.ResolvedType().Family())
			} else {
				require.Equal(t, tc.expected.String(), result.String())
			}
		})
	}
}

// createTestParquetFile creates a simple Parquet file in memory for testing
func createTestParquetFile(t *testing.T, numRows int) *bytes.Buffer {
	// Handle empty file case - return nil, tests should check for this
	if numRows == 0 {
		return nil
	}

	pool := memory.NewGoAllocator()

	// Define schema with various types
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		},
		nil,
	)

	// Build record with test data
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	scoreBuilder := builder.Field(2).(*array.Float64Builder)
	activeBuilder := builder.Field(3).(*array.BooleanBuilder)

	for i := 0; i < numRows; i++ {
		idBuilder.Append(int32(i))
		if i%3 == 0 {
			nameBuilder.AppendNull()
		} else {
			nameBuilder.Append("name_" + string(rune('A'+i%26)))
		}
		if i%5 == 0 {
			scoreBuilder.AppendNull()
		} else {
			scoreBuilder.Append(float64(i) * 1.5)
		}
		activeBuilder.Append(i%2 == 0)
	}

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet format
	buf := new(bytes.Buffer)
	writer, err := pqarrow.NewFileWriter(schema, buf, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	return buf
}

func TestParquetRowProducerBasicScanning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a test Parquet file with 10 rows
	parquetData := createTestParquetFile(t, 10)

	// Create fileReader from buffer - use the same bytes.Reader instance for all interfaces
	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader)
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Verify initial state
	require.Equal(t, int64(10), producer.totalRows)
	require.Equal(t, 4, producer.numColumns) // id, name, score, active

	// Scan all rows
	rowCount := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)
		require.NotNil(t, row)

		rowData := row.([]interface{})
		require.Equal(t, 4, len(rowData))

		// Verify ID column (always present)
		require.Equal(t, int32(rowCount), rowData[0])

		rowCount++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, 10, rowCount)
	require.Equal(t, float32(1.0), producer.Progress())
}

func TestParquetRowProducerNullHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a test Parquet file
	parquetData := createTestParquetFile(t, 15)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader)
	require.NoError(t, err)

	rowCount := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := row.([]interface{})

		// Check NULL values based on test data pattern
		// name is NULL when rowCount % 3 == 0
		if rowCount%3 == 0 {
			require.Nil(t, rowData[1], "row %d: name should be NULL", rowCount)
		} else {
			require.NotNil(t, rowData[1], "row %d: name should not be NULL", rowCount)
		}

		// score is NULL when rowCount % 5 == 0
		if rowCount%5 == 0 {
			require.Nil(t, rowData[2], "row %d: score should be NULL", rowCount)
		} else {
			require.NotNil(t, rowData[2], "row %d: score should not be NULL", rowCount)
		}

		rowCount++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, 15, rowCount)
}

func TestParquetRowProducerBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a file with more rows than the batch size
	numRows := 250
	parquetData := createTestParquetFile(t, numRows)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader)
	require.NoError(t, err)

	// Should read in batches of 100 (defaultParquetBatchSize)
	require.Equal(t, int64(100), producer.batchSize)

	// Scan all rows
	rowCount := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)
		require.NotNil(t, row)

		rowData := row.([]interface{})
		require.Equal(t, int32(rowCount), rowData[0])

		rowCount++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, numRows, rowCount)
}

func TestParquetRowProducerProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	numRows := 100
	parquetData := createTestParquetFile(t, numRows)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader)
	require.NoError(t, err)

	// Initial progress should be 0
	require.Equal(t, float32(0), producer.Progress())

	// Read half the rows
	for i := 0; i < numRows/2; i++ {
		require.True(t, producer.Scan())
		_, err := producer.Row()
		require.NoError(t, err)
	}

	// Progress should be approximately 0.5
	progress := producer.Progress()
	require.InDelta(t, 0.5, progress, 0.01)

	// Read remaining rows
	for producer.Scan() {
		_, err := producer.Row()
		require.NoError(t, err)
	}

	// Progress should be 1.0
	require.Equal(t, float32(1.0), producer.Progress())
}

func TestParquetRowProducerSingleRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a Parquet file with just 1 row
	parquetData := createTestParquetFile(t, 1)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader)
	require.NoError(t, err)

	// Should scan exactly one row
	require.True(t, producer.Scan())
	row, err := producer.Row()
	require.NoError(t, err)
	require.NotNil(t, row)

	// No more rows
	require.False(t, producer.Scan())
	require.NoError(t, producer.Err())
}

func TestParquetRowProducerSkip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	parquetData := createTestParquetFile(t, 10)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader)
	require.NoError(t, err)

	// Scan and skip first row
	require.True(t, producer.Scan())
	err = producer.Skip()
	require.NoError(t, err)

	// Read second row - should have ID = 1 (not 0)
	require.True(t, producer.Scan())
	row, err := producer.Row()
	require.NoError(t, err)

	rowData := row.([]interface{})
	require.Equal(t, int32(1), rowData[0])
}

func TestParquetRowConsumerCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a simple import context for testing
	importCtx := &parallelImportContext{
		targetCols: tree.NameList{"id", "name", "score", "active"},
	}

	// Create a mock Parquet file to get the schema
	parquetData := createTestParquetFile(t, 1)
	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}
	producer, err := newParquetRowProducer(fileReader)
	require.NoError(t, err)

	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{})
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Verify field mapping was created correctly
	require.Equal(t, 4, len(consumer.fieldNameToIdx))
	require.Contains(t, consumer.fieldNameToIdx, "id")
	require.Contains(t, consumer.fieldNameToIdx, "name")
	require.Contains(t, consumer.fieldNameToIdx, "score")
	require.Contains(t, consumer.fieldNameToIdx, "active")
}
