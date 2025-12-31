package generator

const (
	SQLIndent            = "    "
	MigrationFilePattern = "%06d_%s.%s.sql"

	DefaultOutputDir     = "./migrations"
	DefaultStartVersion  = 1
	DefaultMaxOpsPerFile = 20
	DefaultFileMode      = 0o644
	DefaultDirMode       = 0o755
)

type DetailKey string

func (k DetailKey) String() string {
	return string(k)
}

const (
	DetailKeyTable         DetailKey = "table"
	DetailKeyColumn        DetailKey = "column"
	DetailKeyColumnName    DetailKey = "column_name"
	DetailKeyOldType       DetailKey = "old_type"
	DetailKeyNewType       DetailKey = "new_type"
	DetailKeyOldNullable   DetailKey = "old_nullable"
	DetailKeyNewNullable   DetailKey = "new_nullable"
	DetailKeyOldDefault    DetailKey = "old_default"
	DetailKeyNewDefault    DetailKey = "new_default"
	DetailKeyOldComment    DetailKey = "old_comment"
	DetailKeyNewComment    DetailKey = "new_comment"
	DetailKeyConstraint    DetailKey = "constraint"
	DetailKeyIndex         DetailKey = "index"
	DetailKeyView          DetailKey = "view"
	DetailKeyMaterialized  DetailKey = "materialized_view"
	DetailKeyFunction      DetailKey = "function"
	DetailKeyTrigger       DetailKey = "trigger"
	DetailKeySequence      DetailKey = "sequence"
	DetailKeyCustomType    DetailKey = "custom_type"
	DetailKeyExtension     DetailKey = "extension"
	DetailKeyHypertable    DetailKey = "hypertable"
	DetailKeyOldDefinition DetailKey = "old_definition"
	DetailKeyNewDefinition DetailKey = "new_definition"
)
