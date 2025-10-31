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
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// parquetRowProducer implements importRowProducer for Parquet files.
// It reads columnar data and reconstructs rows.
type parquetRowProducer struct {
	reader *file.Reader // Apache Arrow Parquet file reader

	// Row group tracking
	currentRowGroup   int   // Which row group we're currently reading
	totalRowGroups    int   // Total number of row groups in file
	rowsInGroup       int64 // Number of rows in current row group
	currentRowInGroup int64 // Current position within row group

	// Column readers for current row group
	columnReaders []file.ColumnChunkReader
	numColumns    int

	// Value buffers for reading one value at a time
	// We allocate these once per row group to avoid repeated allocations
	boolBuf    []bool
	int32Buf   []int32
	int64Buf   []int64
	float32Buf []float32
	float64Buf []float64
	bytesBuf   [][]byte

	defLevels []int16 // Definition levels (for NULL handling)
	repLevels []int16 // Repetition levels (for arrays)

	// Progress tracking
	totalRows     int64 // Total rows across all row groups
	rowsProcessed int64 // Rows processed so far

	err error
}

// parquetRowConsumer implements importRowConsumer for Parquet files.
type parquetRowConsumer struct {
	importCtx      *parallelImportContext
	fieldNameToIdx map[string]int // Maps Parquet column name -> table column index
	decoders       []interface{}  // Type decoders for each column (from util/parquet)
	reader         *file.Reader   // Parquet file reader (for accessing schema)
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
	}, nil
}

type parquetInputReader struct {
	importCtx *parallelImportContext
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

	// Create row producer
	// TODO: Get actual filename from context if needed for error messages
	producer, err := newParquetRowProducer(input, "")
	if err != nil {
		return err
	}

	// Create row consumer with schema mapping
	consumer, err := newParquetRowConsumer(p.importCtx, producer, fileCtx)
	if err != nil {
		return err
	}

	// Process rows using the standard parallel import pipeline
	return runParallelImport(ctx, p.importCtx, fileCtx, producer, consumer)
}

// newParquetRowProducer creates a new Parquet row producer from a fileReader.
func newParquetRowProducer(input *fileReader, filename string) (*parquetRowProducer, error) {

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

	// Log file metadata (using Dev.Infof for debugging)
	ctx := context.Background()
	log.Dev.Infof(ctx, "Parquet file %s: %d row groups, %d total rows, %d columns",
		filename, totalRowGroups, totalRows, numColumns)

	return &parquetRowProducer{
		reader:          reader,
		currentRowGroup: -1, // Start at -1 so first advance goes to 0
		totalRowGroups:  totalRowGroups,
		numColumns:      numColumns,
		totalRows:       totalRows,
		rowsProcessed:   0,

		// Allocate buffers for reading single values
		boolBuf:    make([]bool, 1),
		int32Buf:   make([]int32, 1),
		int64Buf:   make([]int64, 1),
		float32Buf: make([]float32, 1),
		float64Buf: make([]float64, 1),
		bytesBuf:   make([][]byte, 1),
		defLevels:  make([]int16, 1),
		repLevels:  make([]int16, 1),
	}, nil
}

// Scan advances to the next row. Returns false when no more rows.
func (p *parquetRowProducer) Scan() bool {
	if p.err != nil {
		log.Dev.Infof(context.Background(), "Parquet Scan() returning false due to error: %v", p.err)
		return false
	}

	// Check if we've exhausted all row groups
	if p.currentRowGroup >= p.totalRowGroups {
		log.Dev.Infof(context.Background(), "Parquet Scan() exhausted all row groups: %d/%d",
			p.currentRowGroup, p.totalRowGroups)
		return false
	}

	// If we need to start a new row group
	if p.columnReaders == nil || p.currentRowInGroup >= p.rowsInGroup {
		if err := p.advanceToNextRowGroup(); err != nil {
			p.err = err
			log.Dev.Infof(context.Background(), "Parquet Scan() error advancing row group: %v", err)
			return false
		}

		// Check again after advancing
		if p.currentRowGroup >= p.totalRowGroups {
			log.Dev.Infof(context.Background(), "Parquet Scan() no more row groups after advancing")
			return false
		}
	}

	// We have a row available in the current row group
	p.currentRowInGroup++
	p.rowsProcessed++

	if p.rowsProcessed%1000 == 0 {
		log.Dev.Infof(context.Background(), "Parquet Scan() processed %d rows", p.rowsProcessed)
	}

	return true
}

// advanceToNextRowGroup sets up column readers for the next row group.
func (p *parquetRowProducer) advanceToNextRowGroup() error {
	// Move to next row group
	p.currentRowGroup++

	log.Dev.Infof(context.Background(), "Parquet advanceToNextRowGroup: moving to row group %d/%d",
		p.currentRowGroup, p.totalRowGroups)

	if p.currentRowGroup >= p.totalRowGroups {
		log.Dev.Infof(context.Background(), "Parquet advanceToNextRowGroup: no more row groups")
		return nil // No more row groups
	}

	// Get the row group reader
	rowGroup := p.reader.RowGroup(p.currentRowGroup)
	p.rowsInGroup = rowGroup.NumRows()
	p.currentRowInGroup = 0

	log.Dev.Infof(context.Background(), "Parquet row group %d has %d rows", p.currentRowGroup, p.rowsInGroup)

	// Set up column chunk readers for all columns in this row group
	p.columnReaders = make([]file.ColumnChunkReader, p.numColumns)
	for colIdx := 0; colIdx < p.numColumns; colIdx++ {
		colReader, err := rowGroup.Column(colIdx)
		if err != nil {
			return errors.Wrapf(err, "failed to get column reader for column %d", colIdx)
		}
		p.columnReaders[colIdx] = colReader
	}

	log.Dev.Infof(context.Background(), "Parquet row group %d: set up %d column readers", p.currentRowGroup, len(p.columnReaders))

	return nil
}

// Row returns the current row as a slice of interface{} values.
// Each value is the raw Parquet value read from the column.
func (p *parquetRowProducer) Row() (interface{}, error) {
	if p.err != nil {
		return nil, p.err
	}

	// Allocate row to hold values from all columns
	row := make([]interface{}, p.numColumns)

	// Read one value from each column to reconstruct the row
	for colIdx := 0; colIdx < p.numColumns; colIdx++ {
		colReader := p.columnReaders[colIdx]

		// Read one value from this column
		// The value type depends on the Parquet physical type
		value, isNull, err := p.readValueFromColumn(colReader)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read value from column %d", colIdx)
		}

		if isNull {
			row[colIdx] = nil
		} else {
			row[colIdx] = value
		}
	}

	return row, nil
}

// readValueFromColumn reads a single value from a column chunk reader.
// Returns the value and whether it's NULL.
func (p *parquetRowProducer) readValueFromColumn(
	colReader file.ColumnChunkReader,
) (interface{}, bool, error) {
	// Determine the physical type and use the appropriate typed reader
	switch colReader.Type() {
	case parquet.Types.Boolean:
		reader := colReader.(*file.BooleanColumnChunkReader)
		numRead, _, err := reader.ReadBatch(1, p.boolBuf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, false, err
		}
		if numRead == 0 {
			return nil, false, errors.New("unexpected end of column")
		}
		// Definition level 0 means NULL (for optional columns)
		isNull := p.defLevels[0] == 0
		if isNull {
			return nil, true, nil
		}
		return p.boolBuf[0], false, nil

	case parquet.Types.Int32:
		reader := colReader.(*file.Int32ColumnChunkReader)
		numRead, _, err := reader.ReadBatch(1, p.int32Buf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, false, err
		}
		if numRead == 0 {
			return nil, false, errors.New("unexpected end of column")
		}
		isNull := p.defLevels[0] == 0
		if isNull {
			return nil, true, nil
		}
		return p.int32Buf[0], false, nil

	case parquet.Types.Int64:
		reader := colReader.(*file.Int64ColumnChunkReader)
		numRead, _, err := reader.ReadBatch(1, p.int64Buf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, false, err
		}
		if numRead == 0 {
			return nil, false, errors.New("unexpected end of column")
		}
		isNull := p.defLevels[0] == 0
		if isNull {
			return nil, true, nil
		}
		return p.int64Buf[0], false, nil

	case parquet.Types.Float:
		reader := colReader.(*file.Float32ColumnChunkReader)
		numRead, _, err := reader.ReadBatch(1, p.float32Buf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, false, err
		}
		if numRead == 0 {
			return nil, false, errors.New("unexpected end of column")
		}
		isNull := p.defLevels[0] == 0
		if isNull {
			return nil, true, nil
		}
		return p.float32Buf[0], false, nil

	case parquet.Types.Double:
		reader := colReader.(*file.Float64ColumnChunkReader)
		numRead, _, err := reader.ReadBatch(1, p.float64Buf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, false, err
		}
		if numRead == 0 {
			return nil, false, errors.New("unexpected end of column")
		}
		isNull := p.defLevels[0] == 0
		if isNull {
			return nil, true, nil
		}
		return p.float64Buf[0], false, nil

	case parquet.Types.ByteArray:
		reader := colReader.(*file.ByteArrayColumnChunkReader)
		// Create ByteArray buffer
		byteArrayBuf := make([]parquet.ByteArray, 1)
		numRead, _, err := reader.ReadBatch(1, byteArrayBuf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, false, err
		}
		if numRead == 0 {
			return nil, false, errors.New("unexpected end of column")
		}
		isNull := p.defLevels[0] == 0
		if isNull {
			return nil, true, nil
		}
		// Copy the bytes since the buffer may be reused
		result := make([]byte, len(byteArrayBuf[0]))
		copy(result, byteArrayBuf[0])
		return result, false, nil

	case parquet.Types.FixedLenByteArray:
		// Similar to ByteArray, but fixed length (e.g., UUIDs)
		reader := colReader.(*file.FixedLenByteArrayColumnChunkReader)
		fixedBuf := make([]parquet.FixedLenByteArray, 1)
		numRead, _, err := reader.ReadBatch(1, fixedBuf, p.defLevels, p.repLevels)
		if err != nil {
			return nil, false, err
		}
		if numRead == 0 {
			return nil, false, errors.New("unexpected end of column")
		}
		isNull := p.defLevels[0] == 0
		if isNull {
			return nil, true, nil
		}
		return fixedBuf[0], false, nil

	default:
		return nil, false, errors.Errorf("unsupported Parquet type: %v", colReader.Type())
	}
}

// Err returns any error encountered during scanning.
func (p *parquetRowProducer) Err() error {
	return p.err
}

// Skip skips the current row.
func (p *parquetRowProducer) Skip() error {
	// For Parquet, we've already read the row in Row(), so just continue
	return nil
}

// Progress returns the fraction of the file that has been processed.
func (p *parquetRowProducer) Progress() float32 {
	if p.totalRows == 0 {
		return 0
	}
	return float32(p.rowsProcessed) / float32(p.totalRows)
}

// newParquetRowConsumer creates a consumer that converts Parquet rows to datums.
func newParquetRowConsumer(
	importCtx *parallelImportContext,
	producer *parquetRowProducer,
	fileCtx *importFileContext,
) (*parquetRowConsumer, error) {
	// Build mapping from Parquet column names to table column indices
	fieldNameToIdx := make(map[string]int)
	for i, col := range importCtx.targetCols {
		// Case-insensitive matching, similar to Avro
		fieldNameToIdx[strings.ToLower(string(col))] = i
	}

	// TODO: Create decoders for each column based on Parquet schema
	// and target table column types
	// For now, this is a placeholder
	decoders := make([]interface{}, len(importCtx.targetCols))

	return &parquetRowConsumer{
		importCtx:      importCtx,
		fieldNameToIdx: fieldNameToIdx,
		decoders:       decoders,
		reader:         producer.reader,
	}, nil
}

// FillDatums converts a Parquet row to CockroachDB datums.
func (c *parquetRowConsumer) FillDatums(
	ctx context.Context,
	rowData interface{},
	rowNum int64,
	conv *row.DatumRowConverter,
) error {
	if rowNum == 1 {
		log.Dev.Infof(ctx, "Parquet FillDatums() called for first row, field mapping: %v", c.fieldNameToIdx)
	}

	// rowData is a []interface{} from parquetRowProducer.Row()
	parquetRow, ok := rowData.([]interface{})
	if !ok {
		return errors.Errorf("expected []interface{}, got %T", rowData)
	}

	if rowNum == 1 {
		log.Dev.Infof(ctx, "Parquet row has %d values", len(parquetRow))
	}

	// For each column in the Parquet file, find the corresponding table column
	for parquetColIdx, value := range parquetRow {
		// Get Parquet column name from schema
		parquetColName := c.reader.MetaData().Schema.Column(parquetColIdx).Name()

		// Map to table column index
		tableColIdx, found := c.fieldNameToIdx[strings.ToLower(parquetColName)]
		if !found {
			if rowNum == 1 {
				log.Dev.Infof(ctx, "Parquet column %s not found in table, skipping", parquetColName)
			}
			// Column in Parquet file not in table - skip or error based on strict mode
			continue
		}

		if rowNum == 1 {
			log.Dev.Infof(ctx, "Parquet column %s (idx %d) -> table column %d, value type: %T",
				parquetColName, parquetColIdx, tableColIdx, value)
		}

		// Convert Parquet value to CockroachDB datum
		var datum tree.Datum
		if value == nil {
			datum = tree.DNull
		} else {
			// Use the appropriate decoder based on the target column type
			targetType := conv.VisibleColTypes[tableColIdx]
			decoder := c.decoders[tableColIdx]

			// TODO: Actual type conversion using pkg/util/parquet decoders
			// For now, placeholder:
			var err error
			datum, err = convertParquetValueToDatum(value, targetType, decoder)
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

// convertParquetValueToDatum converts a raw Parquet value to a CRDB datum.
// This performs basic type conversions - more complex types (arrays, nested)
// would need additional handling.
func convertParquetValueToDatum(
	value interface{},
	targetType *types.T,
	decoder interface{},
) (tree.Datum, error) {
	// Type switch on the value type and convert to appropriate datum
	switch v := value.(type) {
	case bool:
		return tree.MakeDBool(tree.DBool(v)), nil
	case int32:
		// Check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%d", v))
		}
		return tree.NewDInt(tree.DInt(v)), nil
	case int64:
		// Check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%d", v))
		}
		return tree.NewDInt(tree.DInt(v)), nil
	case float32:
		// Check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%f", v))
		}
		return tree.NewDFloat(tree.DFloat(v)), nil
	case float64:
		// Check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%f", v))
		}
		return tree.NewDFloat(tree.DFloat(v)), nil
	case []byte:
		// ByteArray - could be string, timestamp, decimal, etc.
		// For now, treat as string (most common case)
		// TODO: Handle other types based on targetType
		switch targetType.Family() {
		case types.StringFamily:
			return tree.NewDString(string(v)), nil
		case types.BytesFamily:
			return tree.NewDBytes(tree.DBytes(v)), nil
		case types.TimestampFamily:
			// Parse as timestamp
			ts, _, err := tree.ParseDTimestamp(nil, string(v), time.Microsecond)
			if err != nil {
				return nil, err
			}
			return ts, nil
		case types.DecimalFamily:
			return tree.ParseDDecimal(string(v))
		case types.JsonFamily:
			return tree.ParseDJSON(string(v))
		default:
			// Default to string
			return tree.NewDString(string(v)), nil
		}
	case parquet.FixedLenByteArray:
		// Used for UUIDs, fixed-length types
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
