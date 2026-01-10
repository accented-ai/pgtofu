package schema

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	DefaultSchema = "public"
	SchemaVersion = "1.0"

	ForeignKey = "FOREIGN KEY"
	NoAction   = "NO ACTION"
)

type Database struct {
	Version      string `json:"version"`
	DatabaseName string `json:"database_name"`
	ExtractedAt  string `json:"extracted_at"`

	Schemas              []Schema              `json:"schemas,omitempty"`
	Extensions           []Extension           `json:"extensions,omitempty"`
	CustomTypes          []CustomType          `json:"custom_types,omitempty"`
	Sequences            []Sequence            `json:"sequences,omitempty"`
	Tables               []Table               `json:"tables"`
	Views                []View                `json:"views,omitempty"`
	MaterializedViews    []MaterializedView    `json:"materialized_views,omitempty"`
	Functions            []Function            `json:"functions,omitempty"`
	Triggers             []Trigger             `json:"triggers,omitempty"`
	Hypertables          []Hypertable          `json:"hypertables,omitempty"`
	ContinuousAggregates []ContinuousAggregate `json:"continuous_aggregates,omitempty"`
}

type Schema struct {
	Name string `json:"name"`
}

func (s *Schema) QualifiedName() string {
	return s.Name
}

type Extension struct {
	Name    string `json:"name"`
	Schema  string `json:"schema,omitempty"`
	Version string `json:"version,omitempty"`
	Comment string `json:"comment,omitempty"`
}

type CustomType struct {
	Schema     string   `json:"schema"`
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	Definition string   `json:"definition"`
	Values     []string `json:"values,omitempty"`
	Comment    string   `json:"comment,omitempty"`
}

type Sequence struct {
	Schema        string `json:"schema"`
	Name          string `json:"name"`
	DataType      string `json:"data_type"`
	StartValue    int64  `json:"start_value"`
	MinValue      int64  `json:"min_value"`
	MaxValue      int64  `json:"max_value"`
	Increment     int64  `json:"increment"`
	CacheSize     int64  `json:"cache_size"`
	IsCyclic      bool   `json:"is_cyclic"`
	OwnedByTable  string `json:"owned_by_table,omitempty"`
	OwnedByColumn string `json:"owned_by_column,omitempty"`
}

func (s *Sequence) QualifiedName() string {
	return QualifiedName(s.Schema, s.Name)
}

func (db *Database) MarshalJSON() ([]byte, error) {
	type Alias Database
	return json.MarshalIndent((*Alias)(db), "", "  ") //nolint:wrapcheck
}

func (db *Database) UnmarshalJSON(data []byte) error {
	type Alias Database
	return json.Unmarshal(data, (*Alias)(db)) //nolint:wrapcheck
}

func (db *Database) GetTable(schema, name string) *Table {
	schema = NormalizeSchemaName(schema)
	name = NormalizeIdentifier(name)

	for i := range db.Tables {
		if NormalizeSchemaName(db.Tables[i].Schema) == schema &&
			NormalizeIdentifier(db.Tables[i].Name) == name {
			return &db.Tables[i]
		}
	}

	return nil
}

func (db *Database) GetView(schema, name string) *View {
	schema = NormalizeSchemaName(schema)
	name = NormalizeIdentifier(name)

	for i := range db.Views {
		if NormalizeSchemaName(db.Views[i].Schema) == schema &&
			NormalizeIdentifier(db.Views[i].Name) == name {
			return &db.Views[i]
		}
	}

	return nil
}

func (db *Database) GetContinuousAggregate(schema, viewName string) *ContinuousAggregate {
	schema = NormalizeSchemaName(schema)
	viewName = NormalizeIdentifier(viewName)

	for i := range db.ContinuousAggregates {
		if NormalizeSchemaName(db.ContinuousAggregates[i].Schema) == schema &&
			NormalizeIdentifier(db.ContinuousAggregates[i].ViewName) == viewName {
			return &db.ContinuousAggregates[i]
		}
	}

	return nil
}

func (db *Database) GetMaterializedView(schema, name string) *MaterializedView {
	schema = NormalizeSchemaName(schema)
	name = NormalizeIdentifier(name)

	for i := range db.MaterializedViews {
		if NormalizeSchemaName(db.MaterializedViews[i].Schema) == schema &&
			NormalizeIdentifier(db.MaterializedViews[i].Name) == name {
			return &db.MaterializedViews[i]
		}
	}

	return nil
}

func (db *Database) GetFunction(schema, name string, argTypes []string) *Function {
	schema = NormalizeSchemaName(schema)
	name = NormalizeIdentifier(name)

	for i := range db.Functions {
		if NormalizeSchemaName(db.Functions[i].Schema) == schema &&
			NormalizeIdentifier(db.Functions[i].Name) == name &&
			equalStringSlices(db.Functions[i].ArgumentTypes, argTypes) {
			return &db.Functions[i]
		}
	}

	return nil
}

func (db *Database) GetSchemas() int              { return len(db.Schemas) }
func (db *Database) GetExtensions() int           { return len(db.Extensions) }
func (db *Database) GetCustomTypes() int          { return len(db.CustomTypes) }
func (db *Database) GetSequences() int            { return len(db.Sequences) }
func (db *Database) GetTables() int               { return len(db.Tables) }
func (db *Database) GetViews() int                { return len(db.Views) }
func (db *Database) GetMaterializedViews() int    { return len(db.MaterializedViews) }
func (db *Database) GetFunctions() int            { return len(db.Functions) }
func (db *Database) GetTriggers() int             { return len(db.Triggers) }
func (db *Database) GetHypertables() int          { return len(db.Hypertables) }
func (db *Database) GetContinuousAggregates() int { return len(db.ContinuousAggregates) }

func (db *Database) Sort() {
	sort.Slice(db.Schemas, func(i, j int) bool {
		return db.Schemas[i].Name < db.Schemas[j].Name
	})

	sort.Slice(db.Extensions, func(i, j int) bool {
		return db.Extensions[i].Name < db.Extensions[j].Name
	})

	sort.Slice(db.CustomTypes, func(i, j int) bool {
		return db.CustomTypes[i].QualifiedName() < db.CustomTypes[j].QualifiedName()
	})

	sort.Slice(db.Sequences, func(i, j int) bool {
		return db.Sequences[i].QualifiedName() < db.Sequences[j].QualifiedName()
	})

	sort.Slice(db.Tables, func(i, j int) bool {
		return db.Tables[i].QualifiedName() < db.Tables[j].QualifiedName()
	})

	for i := range db.Tables {
		db.Tables[i].Sort()
	}

	sort.Slice(db.Views, func(i, j int) bool {
		return db.Views[i].QualifiedName() < db.Views[j].QualifiedName()
	})

	sort.Slice(db.MaterializedViews, func(i, j int) bool {
		return db.MaterializedViews[i].QualifiedName() < db.MaterializedViews[j].QualifiedName()
	})

	sort.Slice(db.Functions, func(i, j int) bool {
		return db.Functions[i].Signature() < db.Functions[j].Signature()
	})

	sort.Slice(db.Triggers, func(i, j int) bool {
		if db.Triggers[i].TableName != db.Triggers[j].TableName {
			return db.Triggers[i].TableName < db.Triggers[j].TableName
		}

		return db.Triggers[i].Name < db.Triggers[j].Name
	})

	sort.Slice(db.Hypertables, func(i, j int) bool {
		return db.Hypertables[i].QualifiedTableName() < db.Hypertables[j].QualifiedTableName()
	})

	sort.Slice(db.ContinuousAggregates, func(i, j int) bool {
		return db.ContinuousAggregates[i].QualifiedViewName() < db.ContinuousAggregates[j].QualifiedViewName()
	})
}

func (ct *CustomType) QualifiedName() string {
	return QualifiedName(ct.Schema, ct.Name)
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func NormalizeIdentifier(identifier string) string {
	identifier = strings.Trim(identifier, `"`)
	return strings.ToLower(identifier)
}

func NormalizeSchemaName(schema string) string {
	if schema == "" {
		return DefaultSchema
	}

	return NormalizeIdentifier(schema)
}

func QualifiedName(schema, name string) string {
	if schema == "" {
		schema = DefaultSchema
	}

	if name == "" {
		return schema
	}

	return fmt.Sprintf("%s.%s", schema, name)
}
