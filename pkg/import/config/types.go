package config

// ColumnType represents the Manticore column type for CSV mapping.
type ColumnType string

const (
	TypeField        ColumnType = "field"
	TypeFieldString  ColumnType = "field_string"
	TypeAttrUint     ColumnType = "attr_uint"
	TypeAttrBigint   ColumnType = "attr_bigint"
	TypeAttrFloat    ColumnType = "attr_float"
	TypeAttrBool     ColumnType = "attr_bool"
	TypeAttrTimestamp ColumnType = "attr_timestamp"
	TypeAttrString   ColumnType = "attr_string"
	TypeAttrMulti    ColumnType = "attr_multi"
	TypeAttrMulti64  ColumnType = "attr_multi_64"
	TypeAttrJSON     ColumnType = "attr_json"
)

// ValidTypes maps type strings to ColumnType constants.
var ValidTypes = map[string]ColumnType{
	"field":          TypeField,
	"field_string":   TypeFieldString,
	"attr_uint":      TypeAttrUint,
	"attr_bigint":    TypeAttrBigint,
	"attr_float":     TypeAttrFloat,
	"attr_bool":      TypeAttrBool,
	"attr_timestamp": TypeAttrTimestamp,
	"attr_string":    TypeAttrString,
	"attr_multi":     TypeAttrMulti,
	"attr_multi_64":  TypeAttrMulti64,
	"attr_json":      TypeAttrJSON,
}

// Column represents a single column mapping from the configuration.
type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

// IsStringType returns true if the column type should be quoted as a string in SQL.
func (t ColumnType) IsStringType() bool {
	switch t {
	case TypeField, TypeFieldString, TypeAttrString:
		return true
	default:
		return false
	}
}

// IsNumericType returns true if the column type is a numeric type.
func (t ColumnType) IsNumericType() bool {
	switch t {
	case TypeAttrUint, TypeAttrBigint, TypeAttrFloat, TypeAttrTimestamp:
		return true
	default:
		return false
	}
}
