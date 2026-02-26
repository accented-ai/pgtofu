package generator

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func (b *DDLBuilder) getCompressedHypertableForTable(tableName string) *schema.Hypertable {
	currentHT := b.getHypertable(tableName, b.result.Current)
	desiredHT := b.getHypertable(tableName, b.result.Desired)

	currentCompressed := currentHT != nil &&
		currentHT.CompressionEnabled &&
		currentHT.CompressionSettings != nil
	desiredCompressed := desiredHT != nil &&
		desiredHT.CompressionEnabled &&
		desiredHT.CompressionSettings != nil

	if !currentCompressed && !desiredCompressed {
		return nil
	}

	if desiredCompressed {
		return desiredHT
	}

	return currentHT
}

func formatDisableCompression(tableName string) string {
	return fmt.Sprintf("ALTER TABLE %s SET (timescaledb.compress = false);", tableName)
}

func formatEnableCompression(ht *schema.Hypertable) (string, error) {
	if ht == nil || !ht.CompressionEnabled || ht.CompressionSettings == nil {
		return "", nil
	}

	return formatCompressionPolicy(ht)
}

const compressedHypertableWarning = `-- WARNING: This table is a compressed hypertable.
-- If chunks are already compressed, you must decompress them first:
--
--   SELECT decompress_chunk(c)
--   FROM show_chunks('%s') c
--   WHERE is_compressed;
--
-- Decompression can be slow and resource-intensive on large tables.
-- Consider running this migration during a maintenance window.
`

func (b *DDLBuilder) hasModifyCompressionPolicyForTable(tableName string) bool {
	for i := range b.result.Changes {
		change := &b.result.Changes[i]
		if change.Type == differ.ChangeTypeModifyCompressionPolicy &&
			strings.EqualFold(change.ObjectName, tableName) {
			return true
		}
	}

	return false
}

func (b *DDLBuilder) wrapWithCompressionToggle(
	stmt DDLStatement,
	tableName string,
) (DDLStatement, error) {
	ht := b.getCompressedHypertableForTable(tableName)
	if ht == nil {
		return stmt, nil
	}

	qualifiedTable := QualifiedName(ht.Schema, ht.TableName)

	disableSQL := formatDisableCompression(qualifiedTable)

	skipReEnable := b.hasModifyCompressionPolicyForTable(tableName)

	var enableSQL string

	if !skipReEnable {
		var err error

		enableSQL, err = formatEnableCompression(ht)
		if err != nil {
			return DDLStatement{}, err
		}
	}

	var sb strings.Builder

	fmt.Fprintf(&sb, compressedHypertableWarning, qualifiedTable)
	sb.WriteString(disableSQL)
	sb.WriteString("\n")

	stmtSQL := strings.TrimSpace(stmt.SQL)
	if !strings.HasSuffix(stmtSQL, ";") {
		stmtSQL += ";"
	}

	sb.WriteString(stmtSQL)

	if enableSQL != "" {
		sb.WriteString("\n")
		sb.WriteString(enableSQL)
		sb.WriteString(";")
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: stmt.Description,
		IsUnsafe:    true,
		RequiresTx:  stmt.RequiresTx,
		CannotUseTx: stmt.CannotUseTx,
	}, nil
}
