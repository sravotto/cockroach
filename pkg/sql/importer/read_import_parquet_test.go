// Copyright 2025 The Cockroach Authors.
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
	"time"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// viewToRow is a test helper that extracts values from a parquetRowView into []interface{}.
// This allows tests to continue using the simple []interface{} format.
func viewToRow(rowData interface{}) []interface{} {
	view, ok := rowData.(*parquetRowView)
	if !ok {
		panic(fmt.Sprintf("expected *parquetRowView, got %T", rowData))
	}

	rowIdx := view.rowIndex
	row := make([]interface{}, view.numColumns)

	for colIdx := 0; colIdx < view.numColumns; colIdx++ {
		batch := view.batches[colIdx]
		if batch == nil {
			row[colIdx] = nil
			continue
		}

		if batch.isNull[rowIdx] {
			row[colIdx] = nil
			continue
		}

		// Extract typed value
		switch batch.physicalType {
		case parquet.Types.Boolean:
			row[colIdx] = batch.boolValues[rowIdx]
		case parquet.Types.Int32:
			row[colIdx] = batch.int32Values[rowIdx]
		case parquet.Types.Int64:
			row[colIdx] = batch.int64Values[rowIdx]
		case parquet.Types.Float:
			row[colIdx] = batch.float32Values[rowIdx]
		case parquet.Types.Double:
			row[colIdx] = batch.float64Values[rowIdx]
		case parquet.Types.ByteArray:
			row[colIdx] = batch.byteArrayValues[rowIdx]
		case parquet.Types.FixedLenByteArray:
			row[colIdx] = batch.fixedLenByteArrayValues[rowIdx]
		}
	}

	return row
}

// TestConvertParquetValueToDatum tests the type conversion function
func TestConvertParquetValueToDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name          string
		value         interface{}
		targetType    *types.T
		logicalType   schema.LogicalType
		convertedType schema.ConvertedType
		expected      tree.Datum
		expectErr     bool
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

		// Date logical types
		{
			name:          "int32-date-logical-type",
			value:         int32(18262), // 2020-01-01
			targetType:    types.Date,
			logicalType:   &schema.DateLogicalType{},
			convertedType: schema.ConvertedTypes.None,
			expected: func() tree.Datum {
				d, _ := pgdate.MakeDateFromUnixEpoch(18262)
				return tree.NewDDate(d)
			}(),
		},
		{
			name:          "int32-date-converted-type",
			value:         int32(0), // 1970-01-01 (epoch)
			targetType:    types.Date,
			logicalType:   nil,
			convertedType: schema.ConvertedTypes.Date,
			expected: func() tree.Datum {
				d, _ := pgdate.MakeDateFromUnixEpoch(0)
				return tree.NewDDate(d)
			}(),
		},

		// Time logical types
		{
			name:          "int32-time-millis-logical-type",
			value:         int32(36000000), // 10:00:00
			targetType:    types.Time,
			logicalType:   schema.NewTimeLogicalType(false, schema.TimeUnitMillis),
			convertedType: schema.ConvertedTypes.None,
			expected:      tree.MakeDTime(timeofday.TimeOfDay(36000000000)), // 36000000ms * 1000 = 36000000000µs
		},
		{
			name:          "int32-time-millis-converted-type",
			value:         int32(3600000), // 01:00:00
			targetType:    types.Time,
			logicalType:   nil,
			convertedType: schema.ConvertedTypes.TimeMillis,
			expected:      tree.MakeDTime(timeofday.TimeOfDay(3600000000)), // 3600000ms * 1000 = 3600000000µs
		},
		{
			name:          "int64-time-micros-logical-type",
			value:         int64(36000000000), // 10:00:00
			targetType:    types.Time,
			logicalType:   schema.NewTimeLogicalType(false, schema.TimeUnitMicros),
			convertedType: schema.ConvertedTypes.None,
			expected:      tree.MakeDTime(timeofday.TimeOfDay(36000000000)),
		},
		{
			name:          "int64-time-micros-converted-type",
			value:         int64(3600000000), // 01:00:00
			targetType:    types.Time,
			logicalType:   nil,
			convertedType: schema.ConvertedTypes.TimeMicros,
			expected:      tree.MakeDTime(timeofday.TimeOfDay(3600000000)),
		},

		// Timestamp logical types
		{
			name:          "int64-timestamp-millis-logical-type",
			value:         int64(1577836800000), // 2020-01-01 00:00:00 UTC
			targetType:    types.TimestampTZ,
			logicalType:   schema.NewTimestampLogicalType(true, schema.TimeUnitMillis),
			convertedType: schema.ConvertedTypes.None,
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:          "int64-timestamp-micros-logical-type",
			value:         int64(1577836800000000), // 2020-01-01 00:00:00 UTC
			targetType:    types.TimestampTZ,
			logicalType:   schema.NewTimestampLogicalType(true, schema.TimeUnitMicros),
			convertedType: schema.ConvertedTypes.None,
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:          "int64-timestamp-millis-converted-type",
			value:         int64(1577836800000), // 2020-01-01 00:00:00 UTC
			targetType:    types.TimestampTZ,
			logicalType:   nil,
			convertedType: schema.ConvertedTypes.TimestampMillis,
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:          "int64-timestamp-micros-converted-type",
			value:         int64(1577836800000000), // 2020-01-01 00:00:00 UTC
			targetType:    types.TimestampTZ,
			logicalType:   nil,
			convertedType: schema.ConvertedTypes.TimestampMicros,
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},

		// Decimal logical types
		{
			name:          "int32-decimal-scale-2",
			value:         int32(12345), // 123.45
			targetType:    types.Decimal,
			logicalType:   schema.NewDecimalLogicalType(10, 2),
			convertedType: schema.ConvertedTypes.None,
			expected:      func() tree.Datum { d, _ := tree.ParseDDecimal("123.45"); return d }(),
		},
		{
			name:          "int32-decimal-scale-0",
			value:         int32(12345),
			targetType:    types.Decimal,
			logicalType:   schema.NewDecimalLogicalType(10, 0),
			convertedType: schema.ConvertedTypes.None,
			expected:      func() tree.Datum { d, _ := tree.ParseDDecimal("12345"); return d }(),
		},
		{
			name:          "int64-decimal-scale-4",
			value:         int64(123456789), // 12345.6789
			targetType:    types.Decimal,
			logicalType:   schema.NewDecimalLogicalType(20, 4),
			convertedType: schema.ConvertedTypes.None,
			expected:      func() tree.Datum { d, _ := tree.ParseDDecimal("12345.6789"); return d }(),
		},
		{
			name:          "int32-decimal-negative",
			value:         int32(-12345), // -123.45
			targetType:    types.Decimal,
			logicalType:   schema.NewDecimalLogicalType(10, 2),
			convertedType: schema.ConvertedTypes.None,
			expected:      func() tree.Datum { d, _ := tree.ParseDDecimal("-123.45"); return d }(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logicalType := tc.logicalType
			convertedType := tc.convertedType
			// If no explicit convertedType was set (has zero/default value), default to None
			// Note: The zero value is UTF8 (0), so we need to explicitly set to None
			if tc.logicalType == nil && tc.convertedType == 0 {
				convertedType = schema.ConvertedTypes.None
			}
			result, err := convertParquetValueToDatum(tc.value, tc.targetType, logicalType, convertedType)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Special handling for UUID since we can't predict the exact value
			if tc.targetType.Family() == types.UuidFamily && tc.name == "fixed-len-bytes-to-uuid" {
				require.Equal(t, types.UuidFamily, result.ResolvedType().Family())
			} else if result.ResolvedType().Family() == types.BytesFamily {
				// For bytes, compare the actual byte values
				expectedBytes := tree.MustBeDBytes(tc.expected)
				resultBytes := tree.MustBeDBytes(result)
				require.Equal(t, []byte(expectedBytes), []byte(resultBytes))
			} else if result.ResolvedType().Family() == types.DecimalFamily {
				// For decimals, compare decimal values directly
				expectedDec := tree.MustBeDDecimal(tc.expected)
				resultDec := tree.MustBeDDecimal(result)
				require.Equal(t, expectedDec.String(), resultDec.String())
			} else {
				require.Equal(t, tc.expected.String(), result.String())
			}
		})
	}
}

// createTestParquetFile creates a simple Parquet file in memory for testing
func createTestParquetFile(t *testing.T, numRows int, compression compress.Compression) *bytes.Buffer {
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

	// Write to Parquet format with specified compression
	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compression))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
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

	// Test all supported compression codecs
	testCases := []struct {
		name        string
		compression compress.Compression
	}{
		{"Uncompressed", compress.Codecs.Uncompressed},
		{"Gzip", compress.Codecs.Gzip},
		{"Snappy", compress.Codecs.Snappy},
		{"Zstd", compress.Codecs.Zstd},
		{"Brotli", compress.Codecs.Brotli},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test Parquet file with 10 rows and specified compression
			parquetData := createTestParquetFile(t, 10, tc.compression)

			// Create fileReader from buffer - use the same bytes.Reader instance for all interfaces
			reader := bytes.NewReader(parquetData.Bytes())
			fileReader := &fileReader{
				Reader:   reader,
				ReaderAt: reader,
				Seeker:   reader,
				total:    int64(parquetData.Len()),
			}

			producer, err := newParquetRowProducer(fileReader, nil)
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

				rowData := viewToRow(row)
				require.Equal(t, 4, len(rowData))

				// Verify ID column (always present)
				require.Equal(t, int32(rowCount), rowData[0])

				rowCount++
			}

			require.NoError(t, producer.Err())
			require.Equal(t, 10, rowCount)
			require.Equal(t, float32(1.0), producer.Progress())
		})
	}
}

func TestParquetRowProducerNullHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a test Parquet file
	parquetData := createTestParquetFile(t, 15, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	rowCount := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := viewToRow(row)

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
	parquetData := createTestParquetFile(t, numRows, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Should read in batches of 100 (defaultParquetBatchSize)
	require.Equal(t, int64(100), producer.batchSize)

	// Scan all rows
	rowCount := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)
		require.NotNil(t, row)

		rowData := viewToRow(row)
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
	parquetData := createTestParquetFile(t, numRows, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
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
	parquetData := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
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

	parquetData := createTestParquetFile(t, 10, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Scan and skip first row
	require.True(t, producer.Scan())
	err = producer.Skip()
	require.NoError(t, err)

	// Read second row - should have ID = 1 (not 0)
	require.True(t, producer.Scan())
	row, err := producer.Row()
	require.NoError(t, err)

	rowData := viewToRow(row)
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
	parquetData := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}
	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Verify field mapping was created correctly
	require.Equal(t, 4, len(consumer.fieldNameToIdx))
	require.Contains(t, consumer.fieldNameToIdx, "id")
	require.Contains(t, consumer.fieldNameToIdx, "name")
	require.Contains(t, consumer.fieldNameToIdx, "score")
	require.Contains(t, consumer.fieldNameToIdx, "active")
}

// TestParquetAutomaticColumnMapping tests automatic column mapping when no columns are specified.
func TestParquetAutomaticColumnMapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a mock table descriptor with columns matching the Parquet schema
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true},
			{Name: "score", ID: 3, Type: types.Float, Nullable: true},
			{Name: "active", ID: 4, Type: types.Bool, Nullable: true},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name", "score", "active"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	// Create import context with NO target columns (automatic mapping)
	importCtx := &parallelImportContext{
		targetCols: tree.NameList{}, // Empty - should use all visible columns
		tableDesc:  tableDesc,
	}

	// Create a mock Parquet file to get the schema
	parquetData := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}
	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Verify field mapping was created automatically for all visible columns
	require.Equal(t, 4, len(consumer.fieldNameToIdx))
	require.Contains(t, consumer.fieldNameToIdx, "id")
	require.Contains(t, consumer.fieldNameToIdx, "name")
	require.Contains(t, consumer.fieldNameToIdx, "score")
	require.Contains(t, consumer.fieldNameToIdx, "active")

	// Verify the mapping indices are correct
	require.Equal(t, 0, consumer.fieldNameToIdx["id"])
	require.Equal(t, 1, consumer.fieldNameToIdx["name"])
	require.Equal(t, 2, consumer.fieldNameToIdx["score"])
	require.Equal(t, 3, consumer.fieldNameToIdx["active"])
}

// TestParquetMissingRequiredColumn tests that validation fails when a required column is missing.
func TestParquetMissingRequiredColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a mock table descriptor with a required column not in the Parquet file
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true},
			{Name: "email", ID: 3, Type: types.String, Nullable: false}, // Required but not in Parquet
			{Name: "score", ID: 4, Type: types.Float, Nullable: true},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name", "email", "score"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	// Create import context with automatic mapping
	importCtx := &parallelImportContext{
		targetCols: tree.NameList{}, // Empty - should use all visible columns
		tableDesc:  tableDesc,
	}

	// Create a mock Parquet file (has id, name, score, active - but NOT email)
	parquetData := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}
	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Should fail because "email" is required (non-nullable, no default) but missing from Parquet
	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.Error(t, err)
	require.Nil(t, consumer)
	require.Contains(t, err.Error(), "email")
	require.Contains(t, err.Error(), "non-nullable, no default")
}

// TestParquetMultipleFloat64Columns tests that multiple float64 columns
// don't interfere with each other due to buffer reuse.
func TestParquetMultipleFloat64Columns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	// Mimic the Titanic dataset structure with Age and Fare as float64
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "PassengerId", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "Age", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "Fare", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	ageBuilder := builder.Field(1).(*array.Float64Builder)
	fareBuilder := builder.Field(2).(*array.Float64Builder)

	// PassengerId, Age, Fare
	idBuilder.Append(1)
	ageBuilder.Append(32.0)
	fareBuilder.Append(7.75)

	idBuilder.Append(2)
	ageBuilder.Append(26.0)
	fareBuilder.Append(79.20)

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet format
	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back using our importer
	reader := bytes.NewReader(buf.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Verify we can read the data correctly
	require.Equal(t, int64(2), producer.totalRows)
	require.Equal(t, 3, producer.numColumns)

	// Scan and verify each row
	rowNum := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := viewToRow(row)
		require.Equal(t, 3, len(rowData))

		if rowNum == 0 {
			// Row 1: PassengerId=1, Age=32.0, Fare=7.75
			require.Equal(t, int64(1), rowData[0])
			require.Equal(t, float64(32.0), rowData[1], "Age should be 32.0, not %v", rowData[1])
			require.Equal(t, float64(7.75), rowData[2], "Fare should be 7.75, not %v", rowData[2])
		} else if rowNum == 1 {
			// Row 2: PassengerId=2, Age=26.0, Fare=79.20
			require.Equal(t, int64(2), rowData[0])
			require.Equal(t, float64(26.0), rowData[1], "Age should be 26.0, not %v", rowData[1])
			require.Equal(t, float64(79.20), rowData[2], "Fare should be 79.20, not %v", rowData[2])
		}

		rowNum++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, 2, rowNum)
}

// TestParquetMultipleFloat64ColumnsLargeFile tests buffer reuse across batches
func TestParquetMultipleFloat64ColumnsLargeFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "PassengerId", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "Age", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "Fare", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	ageBuilder := builder.Field(1).(*array.Float64Builder)
	fareBuilder := builder.Field(2).(*array.Float64Builder)

	// Create 250 rows to force multiple batches (batch size is 100)
	for i := int64(0); i < 250; i++ {
		idBuilder.Append(i + 1)
		ageBuilder.Append(float64(20 + i))        // Age: 20, 21, 22, ..., 269
		fareBuilder.Append(10.0 + float64(i)*0.5) // Fare: 10.0, 10.5, 11.0, ..., 134.5
	}

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back using our importer
	reader := bytes.NewReader(buf.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Verify all rows
	rowNum := int64(0)
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := viewToRow(row)
		expectedAge := float64(20 + rowNum)
		expectedFare := 10.0 + float64(rowNum)*0.5

		require.Equal(t, rowNum+1, rowData[0], "Row %d: wrong PassengerId", rowNum)
		require.Equal(t, expectedAge, rowData[1], "Row %d: Age should be %.1f, got %v", rowNum, expectedAge, rowData[1])
		require.Equal(t, expectedFare, rowData[2], "Row %d: Fare should be %.2f, got %v", rowNum, expectedFare, rowData[2])

		rowNum++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, int64(250), rowNum)
}

// TestParquetStrictValidation tests strict_validation mode for Parquet imports.
func TestParquetStrictValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	// Create Parquet file with extra column not in table
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "extra_column", Type: arrow.BinaryTypes.String, Nullable: true}, // Extra column
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	extraBuilder := builder.Field(2).(*array.StringBuilder)

	// Add test data
	idBuilder.Append(1)
	nameBuilder.Append("test")
	extraBuilder.Append("extra_value")

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet format
	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Create table with only id and name columns (missing extra_column)
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true},
		},
		NextColumnID: 3,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name"},
				ColumnIDs:   []descpb.ColumnID{1, 2},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	// Test 1: Non-strict mode (default) - should succeed and skip extra_column
	t.Run("NonStrict", func(t *testing.T) {
		importCtx := &parallelImportContext{
			targetCols: tree.NameList{}, // Empty - should use all visible columns
			tableDesc:  tableDesc,
		}

		reader := bytes.NewReader(buf.Bytes())
		fileReader := &fileReader{
			Reader:   reader,
			ReaderAt: reader,
			Seeker:   reader,
			total:    int64(buf.Len()),
		}
		producer, err := newParquetRowProducer(fileReader, nil)
		require.NoError(t, err)

		consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
		require.NoError(t, err)
		require.NotNil(t, consumer)
		require.False(t, consumer.strict)

		// Verify field mapping - extra_column should not be in mapping
		require.Equal(t, 2, len(consumer.fieldNameToIdx))
		require.Contains(t, consumer.fieldNameToIdx, "id")
		require.Contains(t, consumer.fieldNameToIdx, "name")
		require.NotContains(t, consumer.fieldNameToIdx, "extra_column")
	})

	// Test 2: Strict mode - should have strict flag set
	t.Run("StrictFlagSet", func(t *testing.T) {
		importCtx := &parallelImportContext{
			targetCols: tree.NameList{},
			tableDesc:  tableDesc,
		}

		reader := bytes.NewReader(buf.Bytes())
		fileReader := &fileReader{
			Reader:   reader,
			ReaderAt: reader,
			Seeker:   reader,
			total:    int64(buf.Len()),
		}
		producer, err := newParquetRowProducer(fileReader, nil)
		require.NoError(t, err)

		consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, true)
		require.NoError(t, err)

		// Verify strict mode is enabled
		require.True(t, consumer.strict)

		// Verify field mapping still doesn't include extra_column (same as non-strict)
		require.Equal(t, 2, len(consumer.fieldNameToIdx))
		require.Contains(t, consumer.fieldNameToIdx, "id")
		require.Contains(t, consumer.fieldNameToIdx, "name")
		require.NotContains(t, consumer.fieldNameToIdx, "extra_column")
	})
}

// TestParquetReadTitanicFile reads the Titanic Parquet file from testdata
func TestParquetReadTitanicFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Open the Titanic data file from testdata
	dir := datapathutils.TestDataPath(t, "parquet")
	f, err := os.Open(filepath.Join(dir, "titanic.parquet"))
	require.NoError(t, err)
	defer f.Close()

	// Get file size for the fileReader
	stat, err := f.Stat()
	require.NoError(t, err)

	// Create fileReader
	fileReader := &fileReader{
		Reader:   f,
		ReaderAt: f,
		Seeker:   f,
		total:    stat.Size(),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)

	t.Logf("Total rows: %d", producer.totalRows)
	t.Logf("Num columns: %d", producer.numColumns)
	t.Logf("Total row groups: %d", producer.totalRowGroups)

	// Check rows per row group
	for i := 0; i < producer.totalRowGroups; i++ {
		rg := producer.reader.RowGroup(i)
		t.Logf("Row group %d: %d rows", i, rg.NumRows())
	}

	// Print schema
	for i := 0; i < producer.numColumns; i++ {
		col := producer.reader.MetaData().Schema.Column(i)
		t.Logf("Column %d: %s (type: %s, logical: %v, converted: %v)", i, col.Name(), col.PhysicalType(), col.LogicalType(), col.ConvertedType())
	}

	// Read all rows and collect the last 5
	lastRows := make([][]interface{}, 0, 5)
	rowCount := int64(0)

	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := viewToRow(row)

		// Keep only last 5 rows
		if len(lastRows) >= 5 {
			lastRows = lastRows[1:]
		}
		lastRows = append(lastRows, rowData)
		rowCount++
	}

	require.NoError(t, producer.Err())
	t.Logf("Total rows read: %d", rowCount)

	// Print the last 5 rows with detailed Age/Fare info
	t.Logf("\nLast %d rows:", len(lastRows))
	for i, rowData := range lastRows {
		rowNum := rowCount - int64(len(lastRows)) + int64(i)

		// Specifically check columns 5 (Age) and 9 (Fare) if they exist
		if producer.numColumns > 9 {
			passengerId := rowData[0]
			age := rowData[5]
			fare := rowData[9]
			t.Logf("Row %d: PassengerId=%v, Age=%v, Fare=%v", rowNum, passengerId, age, fare)
		} else {
			t.Logf("Row %d: %v", rowNum, rowData)
		}
	}

	// Expected values based on CSV:
	// Row 890 (PassengerId 891): Age should be 32.0, Fare should be 7.75
	t.Logf("\nExpected for Row 890: Age=32.0, Fare=7.75")
	if len(lastRows) > 0 {
		lastRow := lastRows[len(lastRows)-1]
		age := lastRow[5]
		fare := lastRow[9]
		if age != nil {
			ageVal := age.(float64)
			fareVal := fare.(float64)
			t.Logf("Actual:   Age=%.2f, Fare=%.2f", ageVal, fareVal)
			if ageVal == 7.75 {
				t.Errorf("BUG DETECTED: Age shows Fare value (7.75 instead of 32.0)")
			}
		}
	}
}
