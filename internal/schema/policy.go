package schema

type Policy struct {
	Name            string       `json:"name"`
	Command         string       `json:"command"`
	Permissive      bool         `json:"permissive"`
	Roles           []PolicyRole `json:"roles,omitempty"`
	UsingExpression string       `json:"using_expression,omitempty"`
	CheckExpression string       `json:"check_expression,omitempty"`
	Comment         string       `json:"comment,omitempty"`
}

// Public distinguishes the PUBLIC keyword from an explicitly named role.
type PolicyRole struct {
	Name   string `json:"name,omitempty"`
	Public bool   `json:"public,omitempty"`
}
