// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/stretchr/testify/require"
)

// TestConvertParquetValueToDatumLegacy tests legacy ConvertedType conversions
func TestConvertParquetValueToDatumLegacy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name          string
		value         interface{}
		targetType    *types.T
		convertedType schema.ConvertedType
		expected      tree.Datum
		expectErr     bool
	}{
		// Basic types also work with ConvertedType (even if None)
		{
			name:          "bool-true",
			value:         true,
			targetType:    types.Bool,
			convertedType: schema.ConvertedTypes.None,
			expected:      tree.MakeDBool(true),
		},
		{
			name:          "int32-to-int",
			value:         int32(42),
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.None,
			expected:      tree.NewDInt(42),
		},
		{
			name:          "bytes-to-string",
			value:         []byte("hello"),
			targetType:    types.String,
			convertedType: schema.ConvertedTypes.None,
			expected:      tree.NewDString("hello"),
		},

		// ConvertedType-specific tests
		{
			name:          "int32-date-converted-type",
			value:         int32(0), // 1970-01-01 (epoch)
			targetType:    types.Date,
			convertedType: schema.ConvertedTypes.Date,
			expected: func() tree.Datum {
				d, _ := pgdate.MakeDateFromUnixEpoch(0)
				return tree.NewDDate(d)
			}(),
		},
		{
			name:          "int32-time-millis-converted-type",
			value:         int32(3600000), // 01:00:00
			targetType:    types.Time,
			convertedType: schema.ConvertedTypes.TimeMillis,
			expected:      tree.MakeDTime(timeofday.TimeOfDay(3600000000)),
		},
		{
			name:          "int64-time-micros-converted-type",
			value:         int64(3600000000), // 01:00:00
			targetType:    types.Time,
			convertedType: schema.ConvertedTypes.TimeMicros,
			expected:      tree.MakeDTime(timeofday.TimeOfDay(3600000000)),
		},
		{
			name:          "int64-timestamp-millis-converted-type",
			value:         int64(1577836800000), // 2020-01-01 00:00:00 UTC
			targetType:    types.TimestampTZ,
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
			convertedType: schema.ConvertedTypes.TimestampMicros,
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:          "bytes-utf8-converted-type",
			value:         []byte("hello"),
			targetType:    types.String,
			convertedType: schema.ConvertedTypes.UTF8,
			expected:      tree.NewDString("hello"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &parquetColumnMetadata{
				logicalType:   nil,
				convertedType: tc.convertedType,
			}

			result, err := convertWithConvertedType(tc.value, tc.targetType, metadata)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if result.ResolvedType().Family() == types.BytesFamily {
				expectedBytes := tree.MustBeDBytes(tc.expected)
				resultBytes := tree.MustBeDBytes(result)
				require.Equal(t, []byte(expectedBytes), []byte(resultBytes))
			} else if result.ResolvedType().Family() == types.DecimalFamily {
				expectedDec := tree.MustBeDDecimal(tc.expected)
				resultDec := tree.MustBeDDecimal(result)
				require.Equal(t, expectedDec.String(), resultDec.String())
			} else {
				require.Equal(t, tc.expected.String(), result.String())
			}
		})
	}
}
