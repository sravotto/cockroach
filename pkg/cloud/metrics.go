// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// NilMetrics represents a nil metrics object.
var NilMetrics = (*Metrics)(nil)

// Metrics encapsulates the metrics tracking interactions with cloud storage
// providers.
type Metrics struct {
	// Readers counts the cloud storage readers opened.
	CreatedReaders *metric.Counter
	// OpenReaders is the number of currently open cloud readers.
	OpenReaders *metric.Gauge

	// Writers counts the cloud storage writers opened.
	CreatedWriters *metric.Counter
	// OpenReaders is the number of currently open cloud writers.
	OpenWriters *metric.Gauge

	// Listings counts the listing calls made to cloud storage.
	Listings *metric.Counter
	// ListingResults counts the listing results from cloud storage.
	ListingResults *metric.Counter

	// ConnsOpened, ConnsReused and TLSHandhakes track connection http info for cloud
	// storage when collecting this info is enabled.
	ConnsOpened, ConnsReused, TLSHandhakes *metric.Counter

	// NetMetrics tracks connection level metrics.
	NetMetrics *cidr.NetMetrics
}

// MakeMetrics returns a new instance of Metrics.
func MakeMetrics(cidrLookup *cidr.Lookup) metric.Struct {
	cloudReaders := metric.Metadata{
		Name:        "cloud.readers_opened",
		Help:        "Readers opened by all cloud operations",
		Measurement: "Files",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudReadBytes := metric.Metadata{
		Name:        "cloud.read_bytes",
		Help:        "Bytes read from all cloud operations",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudWriters := metric.Metadata{
		Name:        "cloud.writers_opened",
		Help:        "Writers opened by all cloud operations",
		Measurement: "files",
		Unit:        metric.Unit_BYTES,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudWriteBytes := metric.Metadata{
		Name:        "cloud.write_bytes",
		Help:        "Bytes written by all cloud operations",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	listings := metric.Metadata{
		Name:        "cloud.listings",
		Help:        "Listing operations by all cloud operations",
		Measurement: "Calls",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	listingResults := metric.Metadata{
		Name:        "cloud.listing_results",
		Help:        "Listing results by all cloud operations",
		Measurement: "Results",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	connsOpened := metric.Metadata{
		Name:        "cloud.conns_opened",
		Help:        "HTTP connections opened by cloud operations",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	connsReused := metric.Metadata{
		Name:        "cloud.conns_reused",
		Help:        "HTTP connections reused by cloud operations",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	tlsHandhakes := metric.Metadata{
		Name:        "cloud.tls_handshakes",
		Help:        "TLS handshakes done by cloud operations",
		Measurement: "Handshakes",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudOpenReaders := metric.Metadata{
		Name:        "cloud.open_readers",
		Help:        "Currently open readers for cloud IO",
		Measurement: "Readers",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	cloudOpenWriters := metric.Metadata{
		Name:        "cloud.open_writers",
		Help:        "Currently open writers for cloud IO",
		Measurement: "Writers",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	return &Metrics{
		CreatedReaders: metric.NewCounter(cloudReaders),
		OpenReaders:    metric.NewGauge(cloudOpenReaders),
		CreatedWriters: metric.NewCounter(cloudWriters),
		OpenWriters:    metric.NewGauge(cloudOpenWriters),
		Listings:       metric.NewCounter(listings),
		ListingResults: metric.NewCounter(listingResults),
		ConnsOpened:    metric.NewCounter(connsOpened),
		ConnsReused:    metric.NewCounter(connsReused),
		TLSHandhakes:   metric.NewCounter(tlsHandhakes),
		NetMetrics:     cidrLookup.MakeNetMetrics(cloudWriteBytes, cloudReadBytes, "cloud", "bucket", "client"),
	}
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct implements the metric.Struct interface.
func (m *Metrics) MetricStruct() {}

// Reader implements the ReadWriterInterceptor interface.
func (m *Metrics) Reader(
	_ context.Context, _ ExternalStorage, r ioctx.ReadCloserCtx,
) ioctx.ReadCloserCtx {
	if m == nil {
		return r
	}
	m.CreatedReaders.Inc(1)
	m.OpenReaders.Inc(1)
	return &metricsReader{
		ReadCloserCtx: r,
		m:             m,
	}
}

// Writer implements the ReadWriterInterceptor interface.
func (m *Metrics) Writer(_ context.Context, _ ExternalStorage, w io.WriteCloser) io.WriteCloser {
	if m == nil {
		return w
	}
	m.CreatedWriters.Inc(1)
	m.OpenWriters.Inc(1)
	return &metricsWriter{
		WriteCloser: w,
		m:           m,
	}
}

type metricsReader struct {
	ioctx.ReadCloserCtx
	m      *Metrics
	closed bool
	// Cached resuming reader for implementing seeking operations
	resumingReader *ResumingReader
	seekPos        int64 // Current seek position
}

// Close implements the ioctx.ReadCloserCtx interface.
func (mr *metricsReader) Close(ctx context.Context) error {
	if !mr.closed {
		mr.m.OpenReaders.Dec(1)
		mr.closed = true
	}

	return mr.ReadCloserCtx.Close(ctx)
}

// ReadAt implements io.ReaderAt for metricsReader when wrapping a ResumingReader.
// This allows Parquet and other formats that require random access to work with
// cloud storage backends without buffering the entire file to disk.
func (mr *metricsReader) ReadAt(p []byte, off int64) (n int, err error) {
	// Lazily check if underlying reader is a ResumingReader
	if mr.resumingReader == nil {
		mr.resumingReader, _ = mr.ReadCloserCtx.(*ResumingReader)
	}

	if mr.resumingReader == nil || mr.resumingReader.Opener == nil {
		return 0, errors.New("ReadAt not supported: underlying reader does not support range reads")
	}

	// Use the Opener to create a reader at the specified offset
	ctx := context.Background() // TODO: consider passing context through
	reader, _, err := mr.resumingReader.Opener(ctx, off)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	// Read the requested bytes
	return io.ReadFull(reader, p)
}

// Seek implements io.Seeker for metricsReader when wrapping a ResumingReader.
func (mr *metricsReader) Seek(offset int64, whence int) (int64, error) {
	// Lazily check if underlying reader is a ResumingReader
	if mr.resumingReader == nil {
		mr.resumingReader, _ = mr.ReadCloserCtx.(*ResumingReader)
	}

	if mr.resumingReader == nil {
		return 0, errors.New("Seek not supported: underlying reader is not seekable")
	}

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = mr.seekPos + offset
	case io.SeekEnd:
		if mr.resumingReader.Size == 0 {
			return 0, errors.New("Seek from end not supported: size unknown")
		}
		newPos = mr.resumingReader.Size + offset
	default:
		return 0, errors.Newf("invalid whence: %d", whence)
	}

	if newPos < 0 {
		return 0, errors.New("negative position")
	}

	mr.seekPos = newPos
	return newPos, nil
}

type metricsWriter struct {
	io.WriteCloser
	m      *Metrics
	closed bool
}

// Close implements the WriteCloser interface.
func (mw *metricsWriter) Close() error {
	if !mw.closed {
		mw.m.OpenWriters.Dec(1)
		mw.closed = true
	}
	return mw.WriteCloser.Close()
}
