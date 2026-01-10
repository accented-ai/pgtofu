package schema

import "strings"

type Hypertable struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`

	TimeColumnName    string `json:"time_column_name"`
	TimeColumnType    string `json:"time_column_type"`
	PartitionInterval string `json:"partition_interval"`

	SpacePartitions int      `json:"space_partitions,omitempty"`
	SpaceColumns    []string `json:"space_columns,omitempty"`

	CompressionEnabled  bool                 `json:"compression_enabled"`
	CompressionSettings *CompressionSettings `json:"compression_settings,omitempty"`
	RetentionPolicy     *RetentionPolicy     `json:"retention_policy,omitempty"`
	ChunkTimeInterval   string               `json:"chunk_time_interval,omitempty"`
	NumDimensions       int                  `json:"num_dimensions"`
}

type CompressionSettings struct {
	SegmentByColumns  []string        `json:"segment_by_columns,omitempty"`
	OrderByColumns    []OrderByColumn `json:"order_by_columns,omitempty"`
	ChunkTimeInterval string          `json:"chunk_time_interval,omitempty"`
}

type OrderByColumn struct {
	Column     string `json:"column"`
	Direction  string `json:"direction"`
	NullsOrder string `json:"nulls_order,omitempty"`
}

type RetentionPolicy struct {
	DropAfter        string `json:"drop_after"`
	ScheduleInterval string `json:"schedule_interval,omitempty"`
}

type ContinuousAggregate struct {
	Schema           string         `json:"schema"`
	ViewName         string         `json:"view_name"`
	HypertableSchema string         `json:"hypertable_schema"`
	HypertableName   string         `json:"hypertable_name"`
	Query            string         `json:"query"`
	RefreshPolicy    *RefreshPolicy `json:"refresh_policy,omitempty"`
	WithData         bool           `json:"with_data"`
	Materialized     bool           `json:"materialized"`
	Finalized        bool           `json:"finalized,omitempty"`
	Comment          string         `json:"comment,omitempty"`
	Indexes          []Index        `json:"indexes,omitempty"`
}

type RefreshPolicy struct {
	StartOffset      string `json:"start_offset"`
	EndOffset        string `json:"end_offset"`
	ScheduleInterval string `json:"schedule_interval"`
}

type CompressionPolicy struct {
	HypertableSchema string `json:"hypertable_schema"`
	HypertableName   string `json:"hypertable_name"`
	CompressAfter    string `json:"compress_after"`
	ScheduleInterval string `json:"schedule_interval,omitempty"`
}

func (h *Hypertable) QualifiedTableName() string {
	return QualifiedName(h.Schema, h.TableName)
}

func (ca *ContinuousAggregate) QualifiedViewName() string {
	return QualifiedName(ca.Schema, ca.ViewName)
}

func (ca *ContinuousAggregate) QualifiedHypertableName() string {
	return QualifiedName(ca.HypertableSchema, ca.HypertableName)
}

func (cs *CompressionSettings) SegmentByList() string {
	return strings.Join(cs.SegmentByColumns, ", ")
}

func (cs *CompressionSettings) OrderByList() string {
	if len(cs.OrderByColumns) == 0 {
		return ""
	}

	parts := make([]string, 0, len(cs.OrderByColumns))
	for _, col := range cs.OrderByColumns {
		part := col.Column
		if col.Direction != "" && col.Direction != "ASC" {
			part += " " + col.Direction
		}

		if col.NullsOrder != "" {
			part += " " + col.NullsOrder
		}

		parts = append(parts, part)
	}

	return strings.Join(parts, ", ")
}
