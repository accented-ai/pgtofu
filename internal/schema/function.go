package schema

import (
	"fmt"
	"strings"
)

const (
	VolatilityImmutable = "IMMUTABLE"
	VolatilityStable    = "STABLE"
	VolatilityVolatile  = "VOLATILE"
)

type Function struct {
	Schema        string   `json:"schema"`
	Name          string   `json:"name"`
	ArgumentTypes []string `json:"argument_types"`
	ArgumentNames []string `json:"argument_names,omitempty"`
	ArgumentModes []string `json:"argument_modes,omitempty"`
	ReturnType    string   `json:"return_type"`
	Language      string   `json:"language"`
	Body          string   `json:"body"`
	Volatility    string   `json:"volatility"`
	Definition    string   `json:"definition"`

	IsAggregate       bool   `json:"is_aggregate,omitempty"`
	IsWindow          bool   `json:"is_window,omitempty"`
	IsStrict          bool   `json:"is_strict,omitempty"`
	IsSecurityDefiner bool   `json:"is_security_definer,omitempty"`
	Comment           string `json:"comment,omitempty"`
	Owner             string `json:"owner,omitempty"`
}

type Trigger struct {
	Schema         string   `json:"schema"`
	Name           string   `json:"name"`
	TableName      string   `json:"table_name"`
	Timing         string   `json:"timing"`
	Events         []string `json:"events"`
	ForEachRow     bool     `json:"for_each_row"`
	WhenCondition  string   `json:"when_condition,omitempty"`
	FunctionSchema string   `json:"function_schema"`
	FunctionName   string   `json:"function_name"`
	Definition     string   `json:"definition"`
	Comment        string   `json:"comment,omitempty"`
}

func (f *Function) QualifiedName() string {
	return QualifiedName(f.Schema, f.Name)
}

func (f *Function) Signature() string {
	argTypes := strings.Join(f.ArgumentTypes, ", ")
	return fmt.Sprintf("%s(%s)", f.QualifiedName(), argTypes)
}

func (f *Function) ArgumentList() string {
	if len(f.ArgumentNames) == 0 {
		return strings.Join(f.ArgumentTypes, ", ")
	}

	parts := make([]string, 0, len(f.ArgumentTypes))
	for i, argType := range f.ArgumentTypes {
		var part string
		if i < len(f.ArgumentModes) && f.ArgumentModes[i] != "IN" {
			part = f.ArgumentModes[i] + " "
		}

		if i < len(f.ArgumentNames) && f.ArgumentNames[i] != "" {
			part += f.ArgumentNames[i] + " "
		}

		part += argType
		parts = append(parts, part)
	}

	return strings.Join(parts, ", ")
}

func (t *Trigger) QualifiedTableName() string {
	return QualifiedName(t.Schema, t.TableName)
}

func (t *Trigger) QualifiedFunctionName() string {
	return QualifiedName(t.FunctionSchema, t.FunctionName)
}

func (t *Trigger) EventList() string {
	return strings.Join(t.Events, " OR ")
}
