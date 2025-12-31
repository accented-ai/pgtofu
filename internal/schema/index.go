package schema

import (
	"strings"
)

const (
	IndexTypeBTree  = "btree"
	IndexTypeHash   = "hash"
	IndexTypeGIN    = "gin"
	IndexTypeGiST   = "gist"
	IndexTypeSPGiST = "spgist"
	IndexTypeBRIN   = "brin"
)

type Index struct {
	Schema    string   `json:"schema"`
	TableName string   `json:"table_name"`
	Name      string   `json:"name"`
	Columns   []string `json:"columns"`
	Type      string   `json:"type"`

	IsUnique   bool   `json:"is_unique"`
	IsPrimary  bool   `json:"is_primary"`
	Where      string `json:"where,omitempty"`
	Definition string `json:"definition"`

	IsExcludeConstraint bool              `json:"is_exclude_constraint,omitempty"`
	IncludeColumns      []string          `json:"include_columns,omitempty"`
	Tablespace          string            `json:"tablespace,omitempty"`
	StorageParams       map[string]string `json:"storage_params,omitempty"`
}

func (i *Index) QualifiedName() string {
	return QualifiedName(i.Schema, i.Name)
}

func (i *Index) QualifiedTableName() string {
	return QualifiedName(i.Schema, i.TableName)
}

func (i *Index) IsPartial() bool {
	return i.Where != ""
}

func (i *Index) IsExpression() bool {
	for _, col := range i.Columns {
		if strings.ContainsAny(col, "() ") {
			return true
		}
	}

	return false
}

func (i *Index) IsCoveringIndex() bool {
	return len(i.IncludeColumns) > 0
}

func (i *Index) ColumnList() string {
	return strings.Join(i.Columns, ", ")
}

func (i *Index) IncludeColumnList() string {
	return strings.Join(i.IncludeColumns, ", ")
}
