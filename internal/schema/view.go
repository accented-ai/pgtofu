package schema

type View struct {
	Schema            string                 `json:"schema"`
	Name              string                 `json:"name"`
	Definition        string                 `json:"definition"`
	CatalogDefinition *CatalogViewDefinition `json:"catalog_definition,omitempty"`
	Comment           string                 `json:"comment,omitempty"`
	Owner             string                 `json:"owner,omitempty"`
	CheckOption       string                 `json:"check_option,omitempty"`
	IsUpdatable       bool                   `json:"is_updatable,omitempty"`
	SecurityInvoker   bool                   `json:"security_invoker,omitempty"`
}

type MaterializedView struct {
	Schema            string                 `json:"schema"`
	Name              string                 `json:"name"`
	Definition        string                 `json:"definition"`
	CatalogDefinition *CatalogViewDefinition `json:"catalog_definition,omitempty"`
	Comment           string                 `json:"comment,omitempty"`
	Owner             string                 `json:"owner,omitempty"`
	Tablespace        string                 `json:"tablespace,omitempty"`
	Indexes           []Index                `json:"indexes,omitempty"`
	WithData          bool                   `json:"with_data"`
}

type CatalogViewDefinition struct {
	Original   string `json:"original"`
	Definition string `json:"definition"`
}

func (v *View) BoundDefinition() string {
	return boundViewDefinition(v.Definition, v.CatalogDefinition)
}

func (mv *MaterializedView) BoundDefinition() string {
	return boundViewDefinition(mv.Definition, mv.CatalogDefinition)
}

func boundViewDefinition(definition string, catalog *CatalogViewDefinition) string {
	if definition != "" && catalog != nil && catalog.Original == definition {
		return catalog.Definition
	}

	return ""
}

func (v *View) QualifiedName() string {
	return QualifiedName(v.Schema, v.Name)
}

func (mv *MaterializedView) QualifiedName() string {
	return QualifiedName(mv.Schema, mv.Name)
}
