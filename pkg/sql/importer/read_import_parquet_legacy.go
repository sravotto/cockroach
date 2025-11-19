// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"fmt"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// convertWithConvertedType handles conversion using legacy Parquet ConvertedType annotations.
// This is used for files written by older tools (pre-2017 Parquet, Spark 2.x, etc.).
// ConvertedType was the original type annotation system before LogicalType was introduced.
func convertWithConvertedType(
	value interface{}, targetType *types.T, metadata *parquetColumnMetadata,
) (tree.Datum, error) {
	switch v := value.(type) {
	case bool:
		return tree.MakeDBool(tree.DBool(v)), nil

	case int32:
		if metadata.convertedType != schema.ConvertedTypes.None {
			switch metadata.convertedType {
			case schema.ConvertedTypes.Date:
				return convertDateFromInt32(v)
			case schema.ConvertedTypes.TimeMillis:
				return convertTimeMillisFromInt32(v)
			case schema.ConvertedTypes.Decimal:
				// For ConvertedType.Decimal, scale information is not directly available
				// in the convertedType itself. We'd need to get it from the schema column.
				// For now, try to parse as decimal with target column's precision/scale.
				return tree.ParseDDecimal(fmt.Sprintf("%d", v))
			}
		}
		// Fallback: check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%d", v))
		}
		return tree.NewDInt(tree.DInt(v)), nil

	case int64:
		if metadata.convertedType != schema.ConvertedTypes.None {
			switch metadata.convertedType {
			case schema.ConvertedTypes.TimestampMillis:
				return convertTimestampMillisFromInt64(v)
			case schema.ConvertedTypes.TimestampMicros:
				return convertTimestampMicrosFromInt64(v)
			case schema.ConvertedTypes.TimeMicros:
				return convertTimeMicrosFromInt64(v)
			case schema.ConvertedTypes.Decimal:
				// Similar to int32 case - parse as decimal
				return tree.ParseDDecimal(fmt.Sprintf("%d", v))
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
		// Check for String/JSON converted types
		if metadata.convertedType != schema.ConvertedTypes.None {
			switch metadata.convertedType {
			case schema.ConvertedTypes.UTF8:
				return tree.NewDString(string(v)), nil
			case schema.ConvertedTypes.JSON:
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

// validateWithConvertedType validates type compatibility using legacy ConvertedType annotations.
// This is used for files written by older tools that use the legacy Parquet type annotation system.
func validateWithConvertedType(
	physicalType parquet.Type, convertedType schema.ConvertedType, targetType *types.T,
) error {
	switch physicalType {
	case parquet.Types.Boolean:
		// Boolean can only go to Bool
		if targetType.Family() != types.BoolFamily {
			return errors.Newf("boolean type cannot be converted to %s", targetType.Family())
		}

	case parquet.Types.Int32:
		// Check for special converted types
		if convertedType != schema.ConvertedTypes.None {
			switch convertedType {
			case schema.ConvertedTypes.Date:
				if targetType.Family() != types.DateFamily {
					return errors.Newf("Date converted type requires DATE target, got %s", targetType.Family())
				}
				return nil
			case schema.ConvertedTypes.TimeMillis:
				if targetType.Family() != types.TimeFamily {
					return errors.Newf("TimeMillis converted type requires TIME target, got %s", targetType.Family())
				}
				return nil
			case schema.ConvertedTypes.Decimal:
				if targetType.Family() != types.DecimalFamily && targetType.Family() != types.IntFamily {
					return errors.Newf("Decimal converted type requires DECIMAL or INT target, got %s", targetType.Family())
				}
				return nil
			}
		}

		// Plain int32 - can go to Int or Decimal
		if targetType.Family() != types.IntFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("int32 type can only be converted to INT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.Int64:
		// Check for special converted types
		if convertedType != schema.ConvertedTypes.None {
			switch convertedType {
			case schema.ConvertedTypes.TimestampMillis, schema.ConvertedTypes.TimestampMicros:
				if targetType.Family() != types.TimestampFamily && targetType.Family() != types.TimestampTZFamily {
					return errors.Newf("Timestamp converted type requires TIMESTAMP/TIMESTAMPTZ target, got %s", targetType.Family())
				}
				return nil
			case schema.ConvertedTypes.TimeMicros:
				if targetType.Family() != types.TimeFamily {
					return errors.Newf("TimeMicros converted type requires TIME target, got %s", targetType.Family())
				}
				return nil
			case schema.ConvertedTypes.Decimal:
				if targetType.Family() != types.DecimalFamily && targetType.Family() != types.IntFamily {
					return errors.Newf("Decimal converted type requires DECIMAL or INT target, got %s", targetType.Family())
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
		// Check for String/JSON converted types
		if convertedType != schema.ConvertedTypes.None {
			switch convertedType {
			case schema.ConvertedTypes.UTF8:
				if targetType.Family() != types.StringFamily && targetType.Family() != types.BytesFamily {
					return errors.Newf("UTF8 converted type should target STRING or BYTES, got %s", targetType.Family())
				}
				return nil
			case schema.ConvertedTypes.JSON:
				if targetType.Family() != types.JsonFamily {
					return errors.Newf("JSON converted type requires JSONB target, got %s", targetType.Family())
				}
				return nil
			}
		}

		// Plain ByteArray can go to String, Bytes, or be parsed as Timestamp/Decimal/JSON
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
