package schema

import (
	"fmt"
	"sort"
	"strings"
)

type Table struct {
	Schema            string             `json:"schema"`
	Name              string             `json:"name"`
	Columns           []Column           `json:"columns"`
	Constraints       []Constraint       `json:"constraints,omitempty"`
	Indexes           []Index            `json:"indexes,omitempty"`
	Comment           string             `json:"comment,omitempty"`
	Owner             string             `json:"owner,omitempty"`
	Tablespace        string             `json:"tablespace,omitempty"`
	PartitionStrategy *PartitionStrategy `json:"partition_strategy,omitempty"`
}

type PartitionStrategy struct {
	Type       string      `json:"type"`
	Columns    []string    `json:"columns"`
	Partitions []Partition `json:"partitions,omitempty"`
}

type Partition struct {
	Name       string `json:"name"`
	Definition string `json:"definition,omitempty"`
}

type Column struct {
	Name     string `json:"name"`
	DataType string `json:"data_type"`
	Position int    `json:"position"`

	IsNullable bool   `json:"is_nullable"`
	Default    string `json:"default,omitempty"`
	Comment    string `json:"comment,omitempty"`

	MaxLength *int `json:"max_length,omitempty"`
	Precision *int `json:"precision,omitempty"`
	Scale     *int `json:"scale,omitempty"`

	IsArray              bool   `json:"is_array,omitempty"`
	IsIdentity           bool   `json:"is_identity,omitempty"`
	IdentityGeneration   string `json:"identity_generation,omitempty"`
	IsGenerated          bool   `json:"is_generated,omitempty"`
	GenerationExpression string `json:"generation_expression,omitempty"`
}

const (
	ConstraintPrimaryKey = "PRIMARY KEY"
	ConstraintForeignKey = "FOREIGN KEY"
	ConstraintUnique     = "UNIQUE"
	ConstraintCheck      = "CHECK"
	ConstraintExclude    = "EXCLUDE"
)

type Constraint struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	Columns    []string `json:"columns"`
	Definition string   `json:"definition,omitempty"`

	ReferencedSchema  string   `json:"referenced_schema,omitempty"`
	ReferencedTable   string   `json:"referenced_table,omitempty"`
	ReferencedColumns []string `json:"referenced_columns,omitempty"`
	OnDelete          string   `json:"on_delete,omitempty"`
	OnUpdate          string   `json:"on_update,omitempty"`

	CheckExpression string `json:"check_expression,omitempty"`
	IndexName       string `json:"index_name,omitempty"`

	IsDeferrable      bool `json:"is_deferrable,omitempty"`
	InitiallyDeferred bool `json:"initially_deferred,omitempty"`
}

func (t *Table) QualifiedName() string {
	return QualifiedName(t.Schema, t.Name)
}

func (t *Table) GetColumn(name string) *Column {
	normalizedName := NormalizeIdentifier(name)
	for i := range t.Columns {
		if NormalizeIdentifier(t.Columns[i].Name) == normalizedName {
			return &t.Columns[i]
		}
	}

	return nil
}

func (t *Table) GetConstraint(name string) *Constraint {
	normalizedName := NormalizeIdentifier(name)
	for i := range t.Constraints {
		if NormalizeIdentifier(t.Constraints[i].Name) == normalizedName {
			return &t.Constraints[i]
		}
	}

	return nil
}

func (t *Table) GetPrimaryKey() *Constraint {
	for i := range t.Constraints {
		if t.Constraints[i].Type == ConstraintPrimaryKey {
			return &t.Constraints[i]
		}
	}

	return nil
}

func (t *Table) GetIndex(name string) *Index {
	normalizedName := NormalizeIdentifier(name)
	for i := range t.Indexes {
		if NormalizeIdentifier(t.Indexes[i].Name) == normalizedName {
			return &t.Indexes[i]
		}
	}

	return nil
}

func (t *Table) Sort() {
	sort.Slice(t.Columns, func(i, j int) bool {
		return t.Columns[i].Position < t.Columns[j].Position
	})

	constraintOrder := map[string]int{
		ConstraintPrimaryKey: 0,
		ConstraintForeignKey: 1,
		ConstraintUnique:     2,
		ConstraintCheck:      3,
		ConstraintExclude:    4,
	}

	sort.Slice(t.Constraints, func(i, j int) bool {
		orderI := constraintOrder[t.Constraints[i].Type]

		orderJ := constraintOrder[t.Constraints[j].Type]
		if orderI != orderJ {
			return orderI < orderJ
		}

		return t.Constraints[i].Name < t.Constraints[j].Name
	})

	sort.Slice(t.Indexes, func(i, j int) bool {
		return t.Indexes[i].Name < t.Indexes[j].Name
	})
}

func (c *Column) FullDataType() string {
	dt := c.DataType

	if c.MaxLength != nil && isCharacterType(dt) {
		dt = formatCharacterType(dt, *c.MaxLength)
	} else if c.Precision != nil {
		if c.Scale != nil && *c.Scale > 0 {
			dt = fmt.Sprintf("NUMERIC(%d, %d)", *c.Precision, *c.Scale)
		} else {
			dt = fmt.Sprintf("NUMERIC(%d)", *c.Precision)
		}
	}

	if c.IsArray && !strings.HasSuffix(strings.TrimSpace(dt), "[]") {
		dt += "[]"
	}

	return dt
}

func isCharacterType(dt string) bool {
	dt = strings.ToLower(dt)

	return strings.HasPrefix(dt, "character varying") ||
		strings.HasPrefix(dt, "varchar") ||
		strings.HasPrefix(dt, "char")
}

func formatCharacterType(dt string, length int) string {
	dt = strings.ToLower(dt)
	switch {
	case strings.HasPrefix(dt, "character varying"), strings.HasPrefix(dt, "varchar"):
		return fmt.Sprintf("VARCHAR(%d)", length)
	case strings.HasPrefix(dt, "char"):
		return fmt.Sprintf("CHAR(%d)", length)
	default:
		return dt
	}
}

func (c *Column) IsPrimaryKey(table *Table) bool {
	pk := table.GetPrimaryKey()
	if pk == nil {
		return false
	}

	normalizedName := NormalizeIdentifier(c.Name)
	for _, col := range pk.Columns {
		if NormalizeIdentifier(col) == normalizedName {
			return true
		}
	}

	return false
}

func (c *Constraint) QualifiedReferencedTable() string {
	return QualifiedName(c.ReferencedSchema, c.ReferencedTable)
}

func (c *Constraint) IsPrimaryKey() bool {
	return c.Type == ConstraintPrimaryKey
}

func (c *Constraint) IsForeignKey() bool {
	return c.Type == ConstraintForeignKey
}

func (c *Constraint) IsUnique() bool {
	return c.Type == ConstraintUnique
}

func (c *Constraint) IsCheck() bool {
	return c.Type == ConstraintCheck
}
