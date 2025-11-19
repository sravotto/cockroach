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
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestParquetColumnBatchBooleanType tests batch reading for boolean columns
func TestParquetColumnBatchBooleanType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	boolBuilder := builder.Field(0).(*array.BooleanBuilder)
	boolBuilder.Append(true)
	boolBuilder.AppendNull()
	boolBuilder.Append(false)
	boolBuilder.Append(true)

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	// Read the Parquet file
	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)

	rowGroup := parquetReader.RowGroup(0)
	colReader, err := rowGroup.Column(0)
	require.NoError(t, err)

	buffers := newParquetBatchBuffers(4)
	batch := newParquetColumnBatch(colReader, buffers, 4)

	require.Equal(t, parquet.Types.Boolean, batch.physicalType)
	require.Equal(t, int64(4), batch.rowCount)

	err = batch.Read()
	require.NoError(t, err)

	// Check values and nulls
	require.False(t, batch.isNull[0])
	require.True(t, batch.boolValues[0])

	require.True(t, batch.isNull[1])

	require.False(t, batch.isNull[2])
	require.False(t, batch.boolValues[2])

	require.False(t, batch.isNull[3])
	require.True(t, batch.boolValues[3])
}

// TestParquetColumnBatchInt32Type tests batch reading for int32 columns
func TestParquetColumnBatchInt32Type(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	int32Builder := builder.Field(0).(*array.Int32Builder)
	int32Builder.Append(42)
	int32Builder.Append(100)
	int32Builder.AppendNull()
	int32Builder.Append(-5)

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)

	rowGroup := parquetReader.RowGroup(0)
	colReader, err := rowGroup.Column(0)
	require.NoError(t, err)

	buffers := newParquetBatchBuffers(4)
	batch := newParquetColumnBatch(colReader, buffers, 4)

	require.Equal(t, parquet.Types.Int32, batch.physicalType)

	err = batch.Read()
	require.NoError(t, err)

	require.False(t, batch.isNull[0])
	require.Equal(t, int32(42), batch.int32Values[0])

	require.False(t, batch.isNull[1])
	require.Equal(t, int32(100), batch.int32Values[1])

	require.True(t, batch.isNull[2])

	require.False(t, batch.isNull[3])
	require.Equal(t, int32(-5), batch.int32Values[3])
}

// TestParquetColumnBatchInt64Type tests batch reading for int64 columns
func TestParquetColumnBatchInt64Type(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bignum", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	int64Builder := builder.Field(0).(*array.Int64Builder)
	int64Builder.Append(9223372036854775807) // Max int64
	int64Builder.AppendNull()
	int64Builder.Append(-9223372036854775808) // Min int64

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)

	rowGroup := parquetReader.RowGroup(0)
	colReader, err := rowGroup.Column(0)
	require.NoError(t, err)

	buffers := newParquetBatchBuffers(3)
	batch := newParquetColumnBatch(colReader, buffers, 3)

	err = batch.Read()
	require.NoError(t, err)

	require.False(t, batch.isNull[0])
	require.Equal(t, int64(9223372036854775807), batch.int64Values[0])

	require.True(t, batch.isNull[1])

	require.False(t, batch.isNull[2])
	require.Equal(t, int64(-9223372036854775808), batch.int64Values[2])
}

// TestParquetColumnBatchFloat64Type tests batch reading for float64 columns
func TestParquetColumnBatchFloat64Type(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	floatBuilder := builder.Field(0).(*array.Float64Builder)
	floatBuilder.Append(3.14159)
	floatBuilder.Append(2.71828)
	floatBuilder.AppendNull()

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)

	rowGroup := parquetReader.RowGroup(0)
	colReader, err := rowGroup.Column(0)
	require.NoError(t, err)

	buffers := newParquetBatchBuffers(3)
	batch := newParquetColumnBatch(colReader, buffers, 3)

	err = batch.Read()
	require.NoError(t, err)

	require.False(t, batch.isNull[0])
	require.InDelta(t, 3.14159, batch.float64Values[0], 0.00001)

	require.False(t, batch.isNull[1])
	require.InDelta(t, 2.71828, batch.float64Values[1], 0.00001)

	require.True(t, batch.isNull[2])
}

// TestParquetColumnBatchByteArrayType tests batch reading for byte array columns
func TestParquetColumnBatchByteArrayType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	strBuilder := builder.Field(0).(*array.StringBuilder)
	strBuilder.Append("hello")
	strBuilder.AppendNull()
	strBuilder.Append("world")

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)

	rowGroup := parquetReader.RowGroup(0)
	colReader, err := rowGroup.Column(0)
	require.NoError(t, err)

	buffers := newParquetBatchBuffers(3)
	batch := newParquetColumnBatch(colReader, buffers, 3)

	err = batch.Read()
	require.NoError(t, err)

	require.False(t, batch.isNull[0])
	require.Equal(t, []byte("hello"), batch.byteArrayValues[0])

	require.True(t, batch.isNull[1])

	require.False(t, batch.isNull[2])
	require.Equal(t, []byte("world"), batch.byteArrayValues[2])
}

// TestParquetColumnBatchNonNullable tests reading a non-nullable column
func TestParquetColumnBatchNonNullable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	int32Builder := builder.Field(0).(*array.Int32Builder)
	int32Builder.Append(1)
	int32Builder.Append(2)
	int32Builder.Append(3)

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)

	rowGroup := parquetReader.RowGroup(0)
	colReader, err := rowGroup.Column(0)
	require.NoError(t, err)

	buffers := newParquetBatchBuffers(3)
	batch := newParquetColumnBatch(colReader, buffers, 3)

	// Non-nullable column should have maxDefLevel == 0
	require.Equal(t, int16(0), batch.maxDefLevel)

	err = batch.Read()
	require.NoError(t, err)

	// All values should be non-null
	for i := 0; i < 3; i++ {
		require.False(t, batch.isNull[i])
	}

	require.Equal(t, int32(1), batch.int32Values[0])
	require.Equal(t, int32(2), batch.int32Values[1])
	require.Equal(t, int32(3), batch.int32Values[2])
}

// TestParquetRowView tests the parquetRowView structure
func TestParquetRowView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a simple test case with two columns
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)

	idBuilder.Append(1)
	nameBuilder.Append("Alice")

	idBuilder.Append(2)
	nameBuilder.AppendNull()

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)

	rowGroup := parquetReader.RowGroup(0)

	buffers := newParquetBatchBuffers(2)

	// Read both columns
	colReader0, err := rowGroup.Column(0)
	require.NoError(t, err)
	batch0 := newParquetColumnBatch(colReader0, buffers, 2)
	require.NoError(t, batch0.Read())

	colReader1, err := rowGroup.Column(1)
	require.NoError(t, err)
	batch1 := newParquetColumnBatch(colReader1, buffers, 2)
	require.NoError(t, batch1.Read())

	// Create views for each row
	batches := []*parquetColumnBatch{batch0, batch1}

	// View for row 0
	view0 := &parquetRowView{
		batches:    batches,
		numColumns: 2,
		rowIndex:   0,
	}

	require.False(t, view0.batches[0].isNull[view0.rowIndex])
	require.Equal(t, int32(1), view0.batches[0].int32Values[view0.rowIndex])

	require.False(t, view0.batches[1].isNull[view0.rowIndex])
	require.Equal(t, []byte("Alice"), view0.batches[1].byteArrayValues[view0.rowIndex])

	// View for row 1
	view1 := &parquetRowView{
		batches:    batches,
		numColumns: 2,
		rowIndex:   1,
	}

	require.False(t, view1.batches[0].isNull[view1.rowIndex])
	require.Equal(t, int32(2), view1.batches[0].int32Values[view1.rowIndex])

	require.True(t, view1.batches[1].isNull[view1.rowIndex])
}
