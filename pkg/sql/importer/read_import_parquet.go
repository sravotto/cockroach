// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// parquetColumnBatch stores a batch of values for a single column in typed form.
// This avoids boxing values into interface{} until absolutely necessary.
type parquetColumnBatch struct {
	physicalType parquet.Type
	rowCount     int64 // Number of rows in this batch

	// Reference to the column's metadata (cached at file-open time).
	// This includes the conversion function determined once per column.
	metadata *parquetColumnMetadata

	// Only one of these slices will be populated, based on physicalType.
	// All slices have length rowCount (expanded to include nulls).
	boolValues              []bool
	int32Values             []int32
	int64Values             []int64
	float32Values           []float32
	float64Values           []float64
	byteArrayValues         [][]byte // Copied bytes
	fixedLenByteArrayValues []parquet.FixedLenByteArray

	// Null tracking - one entry per row
	isNull []bool // [rowIdx] -> is this row null?
}

// parquetRowView is a lightweight view into a row of columnar data.
// Instead of allocating a new []interface{} for each row, we return this view
// which allows the consumer to read typed values directly from column batches.
// The view captures references to the current batches to remain valid even if
// the producer refills its buffer.
type parquetRowView struct {
	batches    []*parquetColumnBatch // Snapshot of column batches
	numColumns int                   // Total number of columns
	rowIndex   int64                 // Index within the batches
}

// parquetConversionFunc is a function that converts a Parquet value to a CRDB datum.
// The function is determined once per column based on the Parquet file's type annotations
// (LogicalType for modern files, ConvertedType for legacy files).
type parquetConversionFunc func(value interface{}, targetType *types.T, metadata *parquetColumnMetadata) (tree.Datum, error)

// parquetColumnMetadata caches immutable schema information for a column.
// This is populated once when the file is opened and reused across all batches.
type parquetColumnMetadata struct {
	logicalType   schema.LogicalType
	convertedType schema.ConvertedType
	converter     parquetConversionFunc // Pre-determined conversion strategy
}

// parquetRowProducer implements importRowProducer for Parquet files.
// It reads columnar data in batches and provides row-oriented access via views.
type parquetRowProducer struct {
	reader *file.Reader // Apache Arrow Parquet file reader

	// Row group tracking
	currentRowGroup   int   // Which row group we're currently reading
	totalRowGroups    int   // Total number of row groups in file
	rowsInGroup       int64 // Number of rows in current row group
	currentRowInGroup int64 // Current position within row group

	// Column selection - which Parquet columns to actually read
	columnsToRead []int                          // Parquet column indices to read
	numColumns    int                            // Total number of columns in Parquet file
	columnReaders map[int]file.ColumnChunkReader // Maps Parquet column index -> reader

	// Cached schema metadata (populated once per file, used for all batches)
	columnMetadata map[int]*parquetColumnMetadata // Maps Parquet column index -> metadata

	// Batching configuration
	batchSize int64 // Number of rows to read in a single batch

	// Value buffers for reading batchSize values at a time
	// We allocate these once per row group to avoid repeated allocations
	boolBuf    []bool
	int32Buf   []int32
	int64Buf   []int64
	float32Buf []float32
	float64Buf []float64

	defLevels []int16 // Definition levels (for NULL handling)
	repLevels []int16 // Repetition levels (for arrays)

	// Column batches - stores typed data for each column (sparse array)
	// Only columns in columnsToRead will have non-nil batches
	columnBatches      []*parquetColumnBatch // [column index]
	bufferedRowCount   int64                 // Number of rows currently buffered
	currentBufferedRow int64                 // Which buffered row to return next

	// Progress tracking
	totalRows     int64 // Total rows across all row groups
	rowsProcessed int64 // Rows processed so far

	err error
}

// parquetRowConsumer implements importRowConsumer for Parquet files.
type parquetRowConsumer struct {
	importCtx      *parallelImportContext
	fieldNameToIdx map[string]int // Maps Parquet column name -> table column index
	reader         *file.Reader   // Parquet file reader (for accessing schema)
	strict         bool           // If true, error on columns in Parquet not in table
}

// newParquetInputReader creates a new Parquet input converter.
func newParquetInputReader(
	semaCtx *tree.SemaContext,
	kvCh chan row.KVBatch,
	walltime int64,
	parallelism int,
	tableDesc catalog.TableDescriptor,
	targetCols tree.NameList,
	evalCtx *eval.Context,
	seqChunkProvider *row.SeqChunkProvider,
	db *kv.DB,
	parquetOpts roachpb.ParquetOptions,
) (*parquetInputReader, error) {
	// Setup parallel import context (same as other importers)
	importCtx := &parallelImportContext{
		walltime:         walltime,
		numWorkers:       parallelism,
		batchSize:        parallelImporterReaderBatchSize,
		evalCtx:          evalCtx,
		semaCtx:          semaCtx,
		tableDesc:        tableDesc,
		targetCols:       targetCols,
		kvCh:             kvCh,
		seqChunkProvider: seqChunkProvider,
		db:               db,
	}

	return &parquetInputReader{
		importCtx: importCtx,
		opts:      parquetOpts,
	}, nil
}

type parquetInputReader struct {
	importCtx *parallelImportContext
	opts      roachpb.ParquetOptions
}

var _ inputConverter = &parquetInputReader{}

// readFiles implements the inputConverter interface.
func (p *parquetInputReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
	user username.SQLUsername,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, p.readFile,
		makeExternalStorage, user)
}

// readFile processes a single Parquet file.
func (p *parquetInputReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	// Create file context
	fileCtx := &importFileContext{
		source:   inputIdx,
		skip:     resumePos,
		rejected: rejected,
	}

	// Create row producer - it will open the file and determine which columns to read
	producer, err := newParquetRowProducer(input, p.importCtx)
	if err != nil {
		return err
	}

	// Create row consumer with schema mapping
	consumer, err := newParquetRowConsumer(p.importCtx, producer, fileCtx, p.opts.StrictMode)
	if err != nil {
		return err
	}

	// Process rows using the standard parallel import pipeline
	return runParallelImport(ctx, p.importCtx, fileCtx, producer, consumer)
}

// Default batch size for reading Parquet rows
const defaultParquetBatchSize = 100

// newParquetRowProducer creates a new Parquet row producer from a fileReader.
// It opens the Parquet file and determines which columns to read based on importCtx.
// If importCtx is nil, all columns are read (useful for tests).
//
// Thread Safety Note:
// Apache Arrow's Parquet reader uses ReadAt to read column chunks, potentially from
// multiple goroutines. This is safe because fileReader's underlying ResumingReader
// supports concurrent ReadAt calls. See fileReader's documentation for details.
func newParquetRowProducer(input *fileReader, importCtx *parallelImportContext) (*parquetRowProducer, error) {

	// Open Parquet file
	reader, err := file.NewParquetReader(input)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Parquet file")
	}

	// Count total rows across all row groups for progress tracking
	totalRows := int64(0)
	totalRowGroups := reader.NumRowGroups()
	for i := 0; i < totalRowGroups; i++ {
		totalRows += reader.RowGroup(i).NumRows()
	}

	numColumns := reader.MetaData().Schema.NumColumns()
	batchSize := int64(defaultParquetBatchSize)

	// Determine which columns to read
	var columnsToRead []int
	if importCtx == nil {
		// No import context (test mode) - read all columns
		columnsToRead = make([]int, numColumns)
		for i := 0; i < numColumns; i++ {
			columnsToRead[i] = i
		}
	} else {
		// Determine which Parquet columns to read based on target table columns
		parquetSchema := reader.MetaData().Schema
		columnsToRead = determineColumnsToRead(importCtx, parquetSchema)
	}

	// Cache schema metadata once per file for all columns we'll read.
	// This avoids repeated schema lookups in the hot path and determines the
	// conversion strategy (LogicalType vs ConvertedType) once per column.
	columnMetadata := make(map[int]*parquetColumnMetadata)
	parquetSchema := reader.MetaData().Schema
	for _, colIdx := range columnsToRead {
		col := parquetSchema.Column(colIdx)
		columnMetadata[colIdx] = &parquetColumnMetadata{
			logicalType:   col.LogicalType(),
			convertedType: col.ConvertedType(),
			converter:     determineConverter(col),
		}
	}

	return &parquetRowProducer{
		reader:          reader,
		currentRowGroup: -1, // Start at -1 so first advance goes to 0
		totalRowGroups:  totalRowGroups,
		columnsToRead:   columnsToRead,
		numColumns:      numColumns,
		columnMetadata:  columnMetadata,
		totalRows:       totalRows,
		rowsProcessed:   0,
		batchSize:       batchSize,

		// Allocate buffers for reading batchSize values
		boolBuf:    make([]bool, batchSize),
		int32Buf:   make([]int32, batchSize),
		int64Buf:   make([]int64, batchSize),
		float32Buf: make([]float32, batchSize),
		float64Buf: make([]float64, batchSize),
		defLevels:  make([]int16, batchSize),
		repLevels:  make([]int16, batchSize),

		// Initialize column batches (sparse array - only read columns will have batches)
		columnBatches:      make([]*parquetColumnBatch, numColumns),
		bufferedRowCount:   0,
		currentBufferedRow: 0,
	}, nil
}

// Scan advances to the next row. Returns false when no more rows.
func (p *parquetRowProducer) Scan() bool {
	if p.err != nil {
		return false
	}

	// First, check if we have buffered rows available
	// This is important because fillBuffer() may have read ahead
	if p.currentBufferedRow < p.bufferedRowCount {
		return true
	}

	// Buffer is empty - check if we've exhausted all row groups
	if p.currentRowGroup >= p.totalRowGroups {
		return false
	}

	// If we need to start a new row group (buffer is empty and current row group exhausted)
	if p.columnReaders == nil || p.currentRowInGroup >= p.rowsInGroup {
		if err := p.advanceToNextRowGroup(); err != nil {
			p.err = err
			return false
		}

		// Check again after advancing
		if p.currentRowGroup >= p.totalRowGroups {
			return false
		}
	}

	// We're in a valid row group with more rows to read
	// Fill the buffer so Row() or Skip() can consume from it
	if p.currentRowInGroup < p.rowsInGroup {
		if err := p.fillBuffer(); err != nil {
			p.err = err
			return false
		}
		return true
	}

	// No buffered rows and no more rows available
	return false
}

// advanceToNextRowGroup sets up column readers for the next row group.
func (p *parquetRowProducer) advanceToNextRowGroup() error {
	// Move to next row group
	p.currentRowGroup++

	if p.currentRowGroup >= p.totalRowGroups {
		return nil // No more row groups
	}

	// Get the row group reader
	rowGroup := p.reader.RowGroup(p.currentRowGroup)
	p.rowsInGroup = rowGroup.NumRows()
	p.currentRowInGroup = 0

	// Set up column chunk readers only for columns we need to read
	p.columnReaders = make(map[int]file.ColumnChunkReader, len(p.columnsToRead))
	for _, colIdx := range p.columnsToRead {
		colReader, err := rowGroup.Column(colIdx)
		if err != nil {
			return errors.Wrapf(err, "failed to get column reader for column %d", colIdx)
		}
		p.columnReaders[colIdx] = colReader
	}

	return nil
}

// fillBuffer reads up to batchSize rows from needed columns and stores them in columnBatches.
// This method reads N values from each column at once in typed form, avoiding boxing until needed.
// Only columns in columnsToRead are actually read; other columns remain nil.
func (p *parquetRowProducer) fillBuffer() error {
	// Determine how many rows to read in this batch
	// (might be less than batchSize at end of row group)
	rowsRemaining := p.rowsInGroup - p.currentRowInGroup
	rowsToRead := p.batchSize
	if rowsToRead > rowsRemaining {
		rowsToRead = rowsRemaining
	}

	if rowsToRead <= 0 {
		return errors.New("fillBuffer called with no rows remaining")
	}

	// Read rowsToRead values only from columns we need
	for _, colIdx := range p.columnsToRead {
		colReader := p.columnReaders[colIdx]

		// Read a batch of typed values from this column
		batch, err := p.readBatchFromColumn(colReader, colIdx, rowsToRead)
		if err != nil {
			return errors.Wrapf(err, "failed to read batch from column %d", colIdx)
		}

		// Store the batch for this column (sparse array - only needed columns populated)
		p.columnBatches[colIdx] = batch
	}

	// Update buffer state
	p.bufferedRowCount = rowsToRead
	p.currentBufferedRow = 0

	// Increment currentRowInGroup by the number of rows we just read
	p.currentRowInGroup += rowsToRead

	return nil
}

// Row returns a lightweight view of the current row.
// Instead of boxing values into []interface{}, we return a view that allows
// the consumer to read typed values directly from column batches.
// Scan() must be called before Row() to ensure the buffer is filled.
func (p *parquetRowProducer) Row() (interface{}, error) {
	if p.err != nil {
		return nil, p.err
	}

	// Scan() should have already filled the buffer, but check just in case
	if p.currentBufferedRow >= p.bufferedRowCount {
		return nil, errors.New("Row() called without successful Scan()")
	}

	// Capture current batch references to make the view independent of producer state
	// This ensures the view remains valid even if the producer refills its buffers
	batchesCopy := make([]*parquetColumnBatch, len(p.columnBatches))
	copy(batchesCopy, p.columnBatches)

	// Return a view into the current row
	view := &parquetRowView{
		batches:    batchesCopy,
		numColumns: p.numColumns,
		rowIndex:   p.currentBufferedRow,
	}

	// Move to next buffered row and update progress
	p.currentBufferedRow++
	p.rowsProcessed++
	return view, nil
}

// readBatchFromColumn reads a batch of values from a column chunk reader.
// Returns a parquetColumnBatch with typed values, avoiding boxing into interface{}.
func (p *parquetRowProducer) readBatchFromColumn(
	colReader file.ColumnChunkReader,
	colIdx int,
	rowsToRead int64,
) (*parquetColumnBatch, error) {
	descriptor := colReader.Descriptor()
	maxDefLevel := descriptor.MaxDefinitionLevel()

	// Use cached metadata from file-level schema lookup instead of per-batch descriptor calls
	metadata := p.columnMetadata[colIdx]

	batch := &parquetColumnBatch{
		physicalType: colReader.Type(),
		rowCount:     rowsToRead,
		metadata:     metadata,
		isNull:       make([]bool, rowsToRead),
	}

	// Helper to expand compacted values into full array with null markers.
	// For nullable columns, valuesRead < numRead because NULLs don't have values in the buffer.
	expandValues := func(numRead int64, valuesRead int, defLevels []int16, copyValue func(rowIdx int, valIdx int)) error {
		if numRead != rowsToRead {
			return errors.Newf("expected %d rows, got %d", rowsToRead, numRead)
		}
		valIdx := 0
		for i := int64(0); i < numRead; i++ {
			if maxDefLevel > 0 && defLevels[i] < maxDefLevel {
				batch.isNull[i] = true
			} else {
				if valIdx >= valuesRead {
					return errors.Newf("values index %d exceeds valuesRead %d", valIdx, valuesRead)
				}
				batch.isNull[i] = false
				copyValue(int(i), valIdx)
				valIdx++
			}
		}
		return nil
	}

	// Determine the physical type and use the appropriate typed reader
	switch colReader.Type() {
	case parquet.Types.Boolean:
		reader := colReader.(*file.BooleanColumnChunkReader)
		numRead, valuesRead, err := reader.ReadBatch(rowsToRead, p.boolBuf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, err
		}
		batch.boolValues = make([]bool, rowsToRead)
		if err := expandValues(numRead, valuesRead, p.defLevels, func(rowIdx, valIdx int) {
			batch.boolValues[rowIdx] = p.boolBuf[valIdx]
		}); err != nil {
			return nil, err
		}

	case parquet.Types.Int32:
		reader := colReader.(*file.Int32ColumnChunkReader)
		numRead, valuesRead, err := reader.ReadBatch(rowsToRead, p.int32Buf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, err
		}
		batch.int32Values = make([]int32, rowsToRead)
		if err := expandValues(numRead, valuesRead, p.defLevels, func(rowIdx, valIdx int) {
			batch.int32Values[rowIdx] = p.int32Buf[valIdx]
		}); err != nil {
			return nil, err
		}

	case parquet.Types.Int64:
		reader := colReader.(*file.Int64ColumnChunkReader)
		numRead, valuesRead, err := reader.ReadBatch(rowsToRead, p.int64Buf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, err
		}
		batch.int64Values = make([]int64, rowsToRead)
		if err := expandValues(numRead, valuesRead, p.defLevels, func(rowIdx, valIdx int) {
			batch.int64Values[rowIdx] = p.int64Buf[valIdx]
		}); err != nil {
			return nil, err
		}

	case parquet.Types.Float:
		reader := colReader.(*file.Float32ColumnChunkReader)
		numRead, valuesRead, err := reader.ReadBatch(rowsToRead, p.float32Buf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, err
		}
		batch.float32Values = make([]float32, rowsToRead)
		if err := expandValues(numRead, valuesRead, p.defLevels, func(rowIdx, valIdx int) {
			batch.float32Values[rowIdx] = p.float32Buf[valIdx]
		}); err != nil {
			return nil, err
		}

	case parquet.Types.Double:
		reader := colReader.(*file.Float64ColumnChunkReader)
		numRead, valuesRead, err := reader.ReadBatch(rowsToRead, p.float64Buf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, err
		}
		batch.float64Values = make([]float64, rowsToRead)
		if err := expandValues(numRead, valuesRead, p.defLevels, func(rowIdx, valIdx int) {
			batch.float64Values[rowIdx] = p.float64Buf[valIdx]
		}); err != nil {
			return nil, err
		}

	case parquet.Types.ByteArray:
		reader := colReader.(*file.ByteArrayColumnChunkReader)
		byteArrayBuf := make([]parquet.ByteArray, rowsToRead)
		numRead, valuesRead, err := reader.ReadBatch(rowsToRead, byteArrayBuf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, err
		}
		batch.byteArrayValues = make([][]byte, rowsToRead)
		if err := expandValues(numRead, valuesRead, p.defLevels, func(rowIdx, valIdx int) {
			// Copy bytes since the buffer may be reused
			copied := make([]byte, len(byteArrayBuf[valIdx]))
			copy(copied, byteArrayBuf[valIdx])
			batch.byteArrayValues[rowIdx] = copied
		}); err != nil {
			return nil, err
		}

	case parquet.Types.FixedLenByteArray:
		reader := colReader.(*file.FixedLenByteArrayColumnChunkReader)
		fixedBuf := make([]parquet.FixedLenByteArray, rowsToRead)
		numRead, valuesRead, err := reader.ReadBatch(rowsToRead, fixedBuf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, err
		}
		batch.fixedLenByteArrayValues = make([]parquet.FixedLenByteArray, rowsToRead)
		if err := expandValues(numRead, valuesRead, p.defLevels, func(rowIdx, valIdx int) {
			batch.fixedLenByteArrayValues[rowIdx] = fixedBuf[valIdx]
		}); err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unsupported Parquet type: %v", colReader.Type())
	}

	return batch, nil
}

// Err returns any error encountered during scanning.
func (p *parquetRowProducer) Err() error {
	return p.err
}

// Skip skips the current row.
func (p *parquetRowProducer) Skip() error {
	// Advance the buffered row position to skip this row
	if p.currentBufferedRow < p.bufferedRowCount {
		p.currentBufferedRow++
		p.rowsProcessed++
	}
	return nil
}

// Progress returns the fraction of the file that has been processed.
func (p *parquetRowProducer) Progress() float32 {
	if p.totalRows == 0 {
		return 0
	}
	return float32(p.rowsProcessed) / float32(p.totalRows)
}

// determineColumnsToRead determines which Parquet column indices need to be read
// based on the target table columns. Returns the list of Parquet column indices.
func determineColumnsToRead(
	importCtx *parallelImportContext,
	parquetSchema *schema.Schema,
) []int {
	// Determine which table columns we're importing into
	var targetCols []string
	if importCtx.tableDesc == nil {
		// No table descriptor (test mode) - use targetCols as-is
		for _, col := range importCtx.targetCols {
			targetCols = append(targetCols, strings.ToLower(string(col)))
		}
	} else {
		visibleCols := importCtx.tableDesc.VisibleColumns()
		// If targetCols is empty, use all visible columns (automatic mapping)
		if len(importCtx.targetCols) == 0 {
			for _, col := range visibleCols {
				targetCols = append(targetCols, strings.ToLower(col.GetName()))
			}
		} else {
			// Use explicitly specified columns
			for _, col := range importCtx.targetCols {
				targetCols = append(targetCols, strings.ToLower(string(col)))
			}
		}
	}

	// Build set of target column names for quick lookup
	targetColSet := make(map[string]bool)
	for _, colName := range targetCols {
		targetColSet[colName] = true
	}

	// Find which Parquet columns match our target columns
	var columnsToRead []int
	for parquetColIdx := 0; parquetColIdx < parquetSchema.NumColumns(); parquetColIdx++ {
		parquetCol := parquetSchema.Column(parquetColIdx)
		parquetColName := strings.ToLower(parquetCol.Name())

		// If this Parquet column is in our target set, we need to read it
		if targetColSet[parquetColName] {
			columnsToRead = append(columnsToRead, parquetColIdx)
		}
	}

	return columnsToRead
}

// validateAndBuildColumnMapping validates the Parquet schema against the table schema
// and builds a mapping from Parquet column names to table column indices.
func validateAndBuildColumnMapping(
	importCtx *parallelImportContext,
	producer *parquetRowProducer,
) (map[string]int, error) {
	parquetSchema := producer.reader.MetaData().Schema

	// Build a map of Parquet column names (lowercase) for quick lookup
	parquetColNames := make(map[string]bool)
	for parquetColIdx := 0; parquetColIdx < parquetSchema.NumColumns(); parquetColIdx++ {
		parquetCol := parquetSchema.Column(parquetColIdx)
		parquetColNames[strings.ToLower(parquetCol.Name())] = true
	}
	fieldNameToIdx := make(map[string]int)

	if importCtx.tableDesc == nil {
		// No table descriptor (test mode) - use targetCols as-is
		for i, col := range importCtx.targetCols {
			fieldNameToIdx[strings.ToLower(string(col))] = i
		}
		return fieldNameToIdx, nil
	}

	visibleCols := importCtx.tableDesc.VisibleColumns()

	// Determine which table columns to use
	var targetCols []string
	// If targetCols is empty, use all visible columns (automatic mapping)
	if len(importCtx.targetCols) == 0 {
		for _, col := range visibleCols {
			targetCols = append(targetCols, col.GetName())
		}
	} else {
		// Use explicitly specified columns
		for _, col := range importCtx.targetCols {
			targetCols = append(targetCols, string(col))
		}
	}

	// Build the mapping: parquet column name (lowercase) -> table column index
	for i, colName := range targetCols {
		fieldNameToIdx[strings.ToLower(colName)] = i
	}

	// Validate type compatibility ONLY for columns we're actually reading.
	for _, parquetColIdx := range producer.columnsToRead {
		parquetCol := parquetSchema.Column(parquetColIdx)
		parquetColName := parquetCol.Name()

		// Find corresponding table column
		tableColIdx, found := fieldNameToIdx[strings.ToLower(parquetColName)]
		if !found {
			// This shouldn't happen since columnsToRead was determined based on
			// the target columns, but handle it defensively.
			continue
		}

		// Get target column type from the table descriptor
		targetCol := visibleCols[tableColIdx]
		targetType := targetCol.GetType()

		// Validate type compatibility
		if err := validateParquetTypeCompatibility(parquetCol, targetType); err != nil {
			return nil, errors.Wrapf(err, "column %q", parquetColName)
		}
	}

	// Validate that all required table columns are present in the Parquet file
	// Required = non-nullable columns without defaults
	for i, colName := range targetCols {
		targetCol := visibleCols[i]

		// Check if this column exists in the Parquet file (case-insensitive)
		if !parquetColNames[strings.ToLower(colName)] {
			// Column missing from Parquet file - check if it's required
			if !targetCol.IsNullable() && !targetCol.HasDefault() && !targetCol.IsComputed() {
				return nil, errors.Newf(
					"required table column %q (non-nullable, no default) not found in Parquet file",
					colName,
				)
			}
			// Optional column (nullable or has default) - will be set to NULL or default value
		}
	}

	return fieldNameToIdx, nil
}

// newParquetRowConsumer creates a consumer that converts Parquet rows to datums.
func newParquetRowConsumer(
	importCtx *parallelImportContext,
	producer *parquetRowProducer,
	fileCtx *importFileContext,
	strict bool,
) (*parquetRowConsumer, error) {

	// Validate schema and build column mapping
	fieldNameToIdx, err := validateAndBuildColumnMapping(importCtx, producer)
	if err != nil {
		return nil, err
	}

	return &parquetRowConsumer{
		importCtx:      importCtx,
		fieldNameToIdx: fieldNameToIdx,
		reader:         producer.reader,
		strict:         strict,
	}, nil
}

// FillDatums converts a Parquet row to CockroachDB datums.
// It reads typed values directly from column batches, avoiding boxing into interface{}.
func (c *parquetRowConsumer) FillDatums(
	ctx context.Context,
	rowData interface{},
	rowNum int64,
	conv *row.DatumRowConverter,
) error {
	// rowData is a *parquetRowView from parquetRowProducer.Row()
	view, ok := rowData.(*parquetRowView)
	if !ok {
		return errors.Errorf("expected *parquetRowView, got %T", rowData)
	}

	rowIdx := view.rowIndex

	// For each column in the Parquet file, find the corresponding table column
	for parquetColIdx := 0; parquetColIdx < view.numColumns; parquetColIdx++ {
		// Skip columns we didn't read
		batch := view.batches[parquetColIdx]
		if batch == nil {
			continue
		}

		// Get Parquet column metadata from schema
		col := c.reader.MetaData().Schema.Column(parquetColIdx)
		parquetColName := col.Name()

		// Map to table column index
		tableColIdx, found := c.fieldNameToIdx[strings.ToLower(parquetColName)]
		if !found {
			// Column in Parquet file not in table
			if c.strict {
				return newImportRowError(
					errors.Newf("column %q in Parquet file is not in the target table", parquetColName),
					fmt.Sprintf("row %d", rowNum), rowNum)
			}
			// In non-strict mode, skip extra columns
			continue
		}

		// Extract typed value from batch and convert to datum
		var datum tree.Datum
		if batch.isNull[rowIdx] {
			datum = tree.DNull
		} else {
			// Get typed value based on physical type
			var value interface{}
			switch batch.physicalType {
			case parquet.Types.Boolean:
				value = batch.boolValues[rowIdx]
			case parquet.Types.Int32:
				value = batch.int32Values[rowIdx]
			case parquet.Types.Int64:
				value = batch.int64Values[rowIdx]
			case parquet.Types.Float:
				value = batch.float32Values[rowIdx]
			case parquet.Types.Double:
				value = batch.float64Values[rowIdx]
			case parquet.Types.ByteArray:
				value = batch.byteArrayValues[rowIdx]
			case parquet.Types.FixedLenByteArray:
				value = batch.fixedLenByteArrayValues[rowIdx]
			default:
				return newImportRowError(
					errors.Errorf("unsupported Parquet type: %v", batch.physicalType),
					fmt.Sprintf("row %d", rowNum), rowNum)
			}

			// Convert Parquet value to datum using the pre-determined conversion function
			targetType := conv.VisibleColTypes[tableColIdx]
			var err error
			datum, err = batch.metadata.converter(value, targetType, batch.metadata)
			if err != nil {
				return newImportRowError(err, fmt.Sprintf("row %d", rowNum), rowNum)
			}
		}

		// Set datum in converter
		conv.Datums[tableColIdx] = datum
	}

	// Set any nil datums to DNull (for columns not in Parquet file)
	// This is critical - uninitialized datums will cause crashes!
	for i := range conv.Datums {
		if conv.Datums[i] == nil {
			conv.Datums[i] = tree.DNull
		}
	}

	return nil
}

// Helper functions for common Parquet type conversions.
// These are used by both LogicalType and ConvertedType conversion paths.

// convertDateFromInt32 converts an int32 value representing days since Unix epoch to a Date datum.
func convertDateFromInt32(v int32) (tree.Datum, error) {
	d, err := pgdate.MakeDateFromUnixEpoch(int64(v))
	if err != nil {
		return nil, err
	}
	return tree.NewDDate(d), nil
}

// convertTimeMillisFromInt32 converts an int32 value representing milliseconds since midnight to a Time datum.
func convertTimeMillisFromInt32(v int32) (tree.Datum, error) {
	micros := int64(v) * 1000 // Convert milliseconds to microseconds
	return tree.MakeDTime(timeofday.TimeOfDay(micros)), nil
}

// convertTimeMicrosFromInt64 converts an int64 value representing microseconds since midnight to a Time datum.
func convertTimeMicrosFromInt64(v int64) (tree.Datum, error) {
	return tree.MakeDTime(timeofday.TimeOfDay(v)), nil
}

// convertTimeNanosFromInt64 converts an int64 value representing nanoseconds since midnight to a Time datum.
func convertTimeNanosFromInt64(v int64) (tree.Datum, error) {
	micros := v / 1000 // Convert nanoseconds to microseconds
	return tree.MakeDTime(timeofday.TimeOfDay(micros)), nil
}

// convertTimestampMillisFromInt64 converts an int64 value representing milliseconds since Unix epoch to a TimestampTZ datum.
func convertTimestampMillisFromInt64(v int64) (tree.Datum, error) {
	ts := time.Unix(v/1000, (v%1000)*1000000).UTC()
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

// convertTimestampMicrosFromInt64 converts an int64 value representing microseconds since Unix epoch to a TimestampTZ datum.
func convertTimestampMicrosFromInt64(v int64) (tree.Datum, error) {
	ts := time.Unix(v/1000000, (v%1000000)*1000).UTC()
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

// convertTimestampNanosFromInt64 converts an int64 value representing nanoseconds since Unix epoch to a TimestampTZ datum.
func convertTimestampNanosFromInt64(v int64) (tree.Datum, error) {
	ts := time.Unix(v/1000000000, v%1000000000).UTC()
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

// formatDecimalFromInt32 formats an int32 value as a decimal string with the given scale.
func formatDecimalFromInt32(v int32, scale int32) string {
	if scale == 0 {
		return fmt.Sprintf("%d", v)
	}

	isNegative := v < 0
	absVal := v
	if isNegative {
		absVal = -v
	}

	divisor := int32(1)
	for i := int32(0); i < scale; i++ {
		divisor *= 10
	}
	intPart := absVal / divisor
	fracPart := absVal % divisor

	if isNegative {
		return fmt.Sprintf("-%d.%0*d", intPart, scale, fracPart)
	}
	return fmt.Sprintf("%d.%0*d", intPart, scale, fracPart)
}

// formatDecimalFromInt64 formats an int64 value as a decimal string with the given scale.
func formatDecimalFromInt64(v int64, scale int32) string {
	if scale == 0 {
		return fmt.Sprintf("%d", v)
	}

	isNegative := v < 0
	absVal := v
	if isNegative {
		absVal = -v
	}

	divisor := int64(1)
	for i := int32(0); i < scale; i++ {
		divisor *= 10
	}
	intPart := absVal / divisor
	fracPart := absVal % divisor

	if isNegative {
		return fmt.Sprintf("-%d.%0*d", intPart, scale, fracPart)
	}
	return fmt.Sprintf("%d.%0*d", intPart, scale, fracPart)
}

// convertBytesBasedOnTargetType converts a byte array to a datum based on the target column type.
// This handles the common fallback logic when no specific logical/converted type is available.
func convertBytesBasedOnTargetType(v []byte, targetType *types.T) (tree.Datum, error) {
	switch targetType.Family() {
	case types.StringFamily:
		return tree.NewDString(string(v)), nil
	case types.BytesFamily:
		return tree.NewDBytes(tree.DBytes(v)), nil
	case types.TimestampFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		ts, _, err := tree.ParseDTimestamp(nil, string(v), time.Microsecond)
		if err != nil {
			return nil, err
		}
		return ts, nil
	case types.DecimalFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		return tree.ParseDDecimal(string(v))
	case types.JsonFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		return tree.ParseDJSON(string(v))
	default:
		return tree.NewDString(string(v)), nil
	}
}

// convertWithLogicalType handles conversion using modern Parquet LogicalType annotations.
// This is used for files written by recent tools (Apache Arrow, modern Spark, etc.).
func convertWithLogicalType(value interface{}, targetType *types.T, metadata *parquetColumnMetadata) (tree.Datum, error) {
	switch v := value.(type) {
	case bool:
		return tree.MakeDBool(tree.DBool(v)), nil

	case int32:
		if metadata.logicalType != nil {
			switch lt := metadata.logicalType.(type) {
			case *schema.DateLogicalType:
				return convertDateFromInt32(v)
			case *schema.TimeLogicalType:
				if lt.TimeUnit() == schema.TimeUnitMillis {
					return convertTimeMillisFromInt32(v)
				}
			case *schema.DecimalLogicalType:
				decStr := formatDecimalFromInt32(v, lt.Scale())
				return tree.ParseDDecimal(decStr)
			}
		}
		// Fallback: check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%d", v))
		}
		return tree.NewDInt(tree.DInt(v)), nil

	case int64:
		if metadata.logicalType != nil {
			switch lt := metadata.logicalType.(type) {
			case *schema.TimestampLogicalType:
				switch lt.TimeUnit() {
				case schema.TimeUnitMillis:
					return convertTimestampMillisFromInt64(v)
				case schema.TimeUnitMicros:
					return convertTimestampMicrosFromInt64(v)
				case schema.TimeUnitNanos:
					return convertTimestampNanosFromInt64(v)
				}
			case *schema.TimeLogicalType:
				switch lt.TimeUnit() {
				case schema.TimeUnitMicros:
					return convertTimeMicrosFromInt64(v)
				case schema.TimeUnitNanos:
					return convertTimeNanosFromInt64(v)
				}
			case *schema.DecimalLogicalType:
				decStr := formatDecimalFromInt64(v, lt.Scale())
				return tree.ParseDDecimal(decStr)
			}
		}
		// Fallback: check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%d", v))
		}
		return tree.NewDInt(tree.DInt(v)), nil

	case float32:
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%g", v))
		}
		return tree.NewDFloat(tree.DFloat(v)), nil

	case float64:
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%g", v))
		}
		return tree.NewDFloat(tree.DFloat(v)), nil

	case []byte:
		// Check for String logical type
		if metadata.logicalType != nil {
			if _, ok := metadata.logicalType.(*schema.StringLogicalType); ok {
				return tree.NewDString(string(v)), nil
			}
			if _, ok := metadata.logicalType.(*schema.JSONLogicalType); ok {
				if len(v) == 0 {
					return tree.DNull, nil
				}
				return tree.ParseDJSON(string(v))
			}
		}

		// Fall back to target column type
		return convertBytesBasedOnTargetType(v, targetType)

	case parquet.FixedLenByteArray:
		if targetType.Family() == types.UuidFamily {
			uid, err := uuid.FromBytes(v)
			if err != nil {
				return nil, err
			}
			return tree.NewDUuid(tree.DUuid{UUID: uid}), nil
		}
		return tree.NewDBytes(tree.DBytes(v)), nil

	default:
		return nil, errors.Newf("unsupported Parquet value type: %T", value)
	}
}

// determineConverter selects the appropriate conversion function based on what type
// annotations are available in the Parquet file. Modern files use LogicalType,
// while older files use ConvertedType for backward compatibility.
func determineConverter(col *schema.Column) parquetConversionFunc {
	// Prefer LogicalType (modern annotation system) if available
	if col.LogicalType() != nil {
		return convertWithLogicalType
	}
	// Fall back to ConvertedType (legacy annotation system)
	return convertWithConvertedType
}

// validateWithLogicalType validates type compatibility using modern LogicalType annotations.
// This is used for files written by recent tools that use the modern Parquet type annotation system.
func validateWithLogicalType(physicalType parquet.Type, logicalType schema.LogicalType, targetType *types.T) error {
	switch physicalType {
	case parquet.Types.Boolean:
		// Boolean can only go to Bool
		if targetType.Family() != types.BoolFamily {
			return errors.Newf("boolean type cannot be converted to %s", targetType.Family())
		}

	case parquet.Types.Int32:
		// Check for special logical types first
		if logicalType != nil {
			if _, ok := logicalType.(*schema.DateLogicalType); ok {
				if targetType.Family() != types.DateFamily {
					return errors.Newf("Date logical type requires DATE target, got %s", targetType.Family())
				}
				return nil
			}
			if timeType, ok := logicalType.(*schema.TimeLogicalType); ok {
				if timeType.TimeUnit() == schema.TimeUnitMillis {
					if targetType.Family() != types.TimeFamily {
						return errors.Newf("Time logical type requires TIME target, got %s", targetType.Family())
					}
					return nil
				}
			}
			if _, ok := logicalType.(*schema.DecimalLogicalType); ok {
				if targetType.Family() != types.DecimalFamily && targetType.Family() != types.IntFamily {
					return errors.Newf("Decimal logical type requires DECIMAL or INT target, got %s", targetType.Family())
				}
				return nil
			}
		}

		// Plain int32 - can go to Int or Decimal
		if targetType.Family() != types.IntFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("int32 type can only be converted to INT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.Int64:
		// Check for special logical types
		if logicalType != nil {
			if _, ok := logicalType.(*schema.TimestampLogicalType); ok {
				if targetType.Family() != types.TimestampFamily && targetType.Family() != types.TimestampTZFamily {
					return errors.Newf("Timestamp logical type requires TIMESTAMP/TIMESTAMPTZ target, got %s", targetType.Family())
				}
				return nil
			}
			if _, ok := logicalType.(*schema.TimeLogicalType); ok {
				if targetType.Family() != types.TimeFamily {
					return errors.Newf("Time logical type requires TIME target, got %s", targetType.Family())
				}
				return nil
			}
			if _, ok := logicalType.(*schema.DecimalLogicalType); ok {
				if targetType.Family() != types.DecimalFamily && targetType.Family() != types.IntFamily {
					return errors.Newf("Decimal logical type requires DECIMAL or INT target, got %s", targetType.Family())
				}
				return nil
			}
		}

		// Plain int64 - can go to Int or Decimal
		if targetType.Family() != types.IntFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("int64 type can only be converted to INT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.Float:
		// Float32 can go to Float or Decimal
		if targetType.Family() != types.FloatFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("float type can only be converted to FLOAT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.Double:
		// Float64 can go to Float or Decimal
		if targetType.Family() != types.FloatFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("double type can only be converted to FLOAT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.ByteArray:
		// ByteArray is very flexible
		// Check for String logical type
		if logicalType != nil {
			if _, ok := logicalType.(*schema.StringLogicalType); ok {
				if targetType.Family() != types.StringFamily && targetType.Family() != types.BytesFamily {
					return errors.Newf("String logical type should target STRING or BYTES, got %s", targetType.Family())
				}
				return nil
			}
			if _, ok := logicalType.(*schema.JSONLogicalType); ok {
				if targetType.Family() != types.JsonFamily {
					return errors.Newf("JSON logical type requires JSONB target, got %s", targetType.Family())
				}
				return nil
			}
		}

		// Plain ByteArray can go to String, Bytes, or be parsed as Timestamp/Decimal/JSON
		// We allow these flexible conversions
		validFamilies := []types.Family{
			types.StringFamily,
			types.BytesFamily,
			types.TimestampFamily,
			types.TimestampTZFamily,
			types.DecimalFamily,
			types.JsonFamily,
		}
		for _, family := range validFamilies {
			if targetType.Family() == family {
				return nil
			}
		}
		return errors.Newf("byte array type cannot be converted to %s", targetType.Family())

	case parquet.Types.FixedLenByteArray:
		// FixedLenByteArray can go to UUID, Bytes, or String
		validFamilies := []types.Family{
			types.UuidFamily,
			types.BytesFamily,
			types.StringFamily,
		}
		for _, family := range validFamilies {
			if targetType.Family() == family {
				return nil
			}
		}
		return errors.Newf("fixed-length byte array type cannot be converted to %s", targetType.Family())

	default:
		return errors.Newf("unsupported Parquet physical type: %v", physicalType)
	}

	return nil
}

// validateParquetTypeCompatibility checks if a Parquet column can be converted to a target CockroachDB type.
// It dispatches to the appropriate validator based on the type annotation system used by the Parquet file.
func validateParquetTypeCompatibility(parquetCol *schema.Column, targetType *types.T) error {
	physicalType := parquetCol.PhysicalType()
	logicalType := parquetCol.LogicalType()
	convertedType := parquetCol.ConvertedType()

	// Prefer LogicalType (modern annotation system) if available
	if logicalType != nil {
		return validateWithLogicalType(physicalType, logicalType, targetType)
	}
	// Fall back to ConvertedType (legacy annotation system)
	return validateWithConvertedType(physicalType, convertedType, targetType)
}
