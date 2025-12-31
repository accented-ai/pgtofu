package schema

type View struct {
	Schema      string `json:"schema"`
	Name        string `json:"name"`
	Definition  string `json:"definition"`
	Comment     string `json:"comment,omitempty"`
	Owner       string `json:"owner,omitempty"`
	CheckOption string `json:"check_option,omitempty"`
	IsUpdatable bool   `json:"is_updatable,omitempty"`
}

type MaterializedView struct {
	Schema     string  `json:"schema"`
	Name       string  `json:"name"`
	Definition string  `json:"definition"`
	Comment    string  `json:"comment,omitempty"`
	Owner      string  `json:"owner,omitempty"`
	Tablespace string  `json:"tablespace,omitempty"`
	Indexes    []Index `json:"indexes,omitempty"`
	WithData   bool    `json:"with_data"`
}

func (v *View) QualifiedName() string {
	return QualifiedName(v.Schema, v.Name)
}

func (mv *MaterializedView) QualifiedName() string {
	return QualifiedName(mv.Schema, mv.Name)
}
