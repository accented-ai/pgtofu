package differ

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type IndexComparator struct {
	options *Options
}

func NewIndexComparator(opts *Options) *IndexComparator {
	return &IndexComparator{options: opts}
}

func (ic *IndexComparator) Compare(result *DiffResult) {
	currentIndexes := ic.buildIndexMap(result.Current)
	desiredIndexes := ic.buildIndexMap(result.Desired)

	ic.detectAddedIndexes(result, currentIndexes, desiredIndexes, result.Desired)
	ic.detectDroppedIndexes(result, currentIndexes, desiredIndexes, result.Current)
	ic.detectModifiedIndexes(result, currentIndexes, desiredIndexes, result.Current, result.Desired)
}

func (ic *IndexComparator) buildIndexMap(db *schema.Database) map[string]*schema.Index {
	m := make(map[string]*schema.Index)

	for i := range db.Tables {
		for j := range db.Tables[i].Indexes {
			idx := &db.Tables[i].Indexes[j]
			key := IndexKey(idx.Schema, idx.Name)
			m[key] = idx
		}
	}

	for i := range db.MaterializedViews {
		for j := range db.MaterializedViews[i].Indexes {
			idx := &db.MaterializedViews[i].Indexes[j]
			key := IndexKey(idx.Schema, idx.Name)
			m[key] = idx
		}
	}

	for i := range db.ContinuousAggregates {
		for j := range db.ContinuousAggregates[i].Indexes {
			idx := &db.ContinuousAggregates[i].Indexes[j]
			key := IndexKey(idx.Schema, idx.Name)
			m[key] = idx
		}
	}

	return m
}

func (ic *IndexComparator) detectAddedIndexes(
	result *DiffResult,
	currentIndexes, desiredIndexes map[string]*schema.Index,
	desiredDB *schema.Database,
) {
	for key, idx := range desiredIndexes {
		if _, exists := currentIndexes[key]; !exists {
			if ic.isConstraintBackedIndex(idx, desiredDB) {
				continue
			}

			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeAddIndex,
				Severity: SeveritySafe,
				Description: fmt.Sprintf(
					"Add %sindex: %s on %s(%s)",
					indexTypeDescription(idx),
					idx.Name,
					idx.QualifiedTableName(),
					idx.ColumnList(),
				),
				ObjectType: "index",
				ObjectName: key,
				Details:    map[string]any{"index": idx},
				DependsOn:  []string{idx.QualifiedTableName()},
			})
		}
	}
}

func (ic *IndexComparator) detectDroppedIndexes(
	result *DiffResult,
	currentIndexes, desiredIndexes map[string]*schema.Index,
	currentDB *schema.Database,
) {
	for key, idx := range currentIndexes {
		if _, exists := desiredIndexes[key]; !exists {
			if ic.isConstraintBackedIndex(idx, currentDB) {
				continue
			}

			severity := SeverityPotentiallyBreaking
			if idx.IsUnique {
				severity = SeverityBreaking
			}

			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeDropIndex,
				Severity: severity,
				Description: fmt.Sprintf(
					"Drop %sindex: %s from %s",
					indexTypeDescription(idx),
					idx.Name,
					idx.QualifiedTableName(),
				),
				ObjectType: "index",
				ObjectName: key,
				Details:    map[string]any{"index": idx},
			})
		}
	}
}

func (ic *IndexComparator) detectModifiedIndexes(
	result *DiffResult,
	currentIndexes, desiredIndexes map[string]*schema.Index,
	currentDB, desiredDB *schema.Database,
) {
	for key, desiredIdx := range desiredIndexes {
		currentIdx, exists := currentIndexes[key]
		if !exists {
			continue
		}

		if ic.isConstraintBackedIndex(currentIdx, currentDB) ||
			ic.isConstraintBackedIndex(desiredIdx, desiredDB) {
			continue
		}

		if !areIndexesEqual(currentIdx, desiredIdx) {
			severity := SeverityPotentiallyBreaking
			if desiredIdx.IsUnique {
				severity = SeverityBreaking
			}

			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeModifyIndex,
				Severity: severity,
				Description: fmt.Sprintf(
					"Modify index: %s on %s",
					desiredIdx.Name,
					desiredIdx.QualifiedTableName(),
				),
				ObjectType: "index",
				ObjectName: key,
				Details:    map[string]any{"current": currentIdx, "desired": desiredIdx},
				DependsOn:  []string{desiredIdx.QualifiedTableName()},
			})
		}
	}
}

func (ic *IndexComparator) isConstraintBackedIndex(idx *schema.Index, db *schema.Database) bool {
	if idx.IsPrimary {
		return true
	}

	table := db.GetTable(idx.Schema, idx.TableName)
	if table == nil {
		return false
	}

	for i := range table.Constraints {
		constraint := &table.Constraints[i]
		if constraint.Name == idx.Name &&
			(constraint.Type == schema.ConstraintPrimaryKey ||
				constraint.Type == schema.ConstraintUnique ||
				constraint.Type == schema.ConstraintExclude) {
			return true
		}
	}

	return false
}

func indexTypeDescription(idx *schema.Index) string {
	var desc strings.Builder

	if idx.IsUnique {
		desc.WriteString("unique ")
	}

	if idx.IsPartial() {
		desc.WriteString("partial ")
	}

	if idx.IsCoveringIndex() {
		desc.WriteString("covering ")
	}

	if idx.Type != "" && idx.Type != "btree" {
		desc.WriteString(idx.Type)
		desc.WriteString(" ")
	}

	return desc.String()
}

func areIndexesEqual(i1, i2 *schema.Index) bool {
	if i1.Type != i2.Type || i1.IsUnique != i2.IsUnique {
		return false
	}

	if i1.NullsNotDistinct != i2.NullsNotDistinct {
		return false
	}

	if i1.QualifiedTableName() != i2.QualifiedTableName() {
		return false
	}

	if !equalIndexColumns(i1.Columns, i2.Columns) {
		return false
	}

	if !equalStringSlicesSorted(i1.IncludeColumns, i2.IncludeColumns) {
		return false
	}

	if !equalStorageParams(i1.StorageParams, i2.StorageParams) {
		return false
	}

	return normalizeExpression(i1.Where) == normalizeExpression(i2.Where)
}

func equalStorageParams(a, b map[string]string) bool {
	normalizedA := normalizeStorageParams(a)
	normalizedB := normalizeStorageParams(b)

	if len(normalizedA) != len(normalizedB) {
		return false
	}

	for k, v1 := range normalizedA {
		v2, ok := normalizedB[k]
		if !ok {
			return false
		}

		if v1 != v2 {
			return false
		}
	}

	return true
}

func trimQuotes(value string) string {
	if len(value) < 2 {
		return value
	}

	first := value[0]
	last := value[len(value)-1]

	if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
		return value[1 : len(value)-1]
	}

	return value
}

func normalizeStorageParams(params map[string]string) map[string]string {
	if len(params) == 0 {
		return nil
	}

	normalized := make(map[string]string, len(params))

	for k, v := range params {
		key := strings.ToLower(strings.TrimSpace(k))
		value := strings.TrimSpace(v)

		value = trimQuotes(value)

		normalized[key] = value
	}

	return normalized
}

func equalIndexColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if normalizeIndexColumn(a[i]) != normalizeIndexColumn(b[i]) {
			return false
		}
	}

	return true
}

func normalizeIndexColumn(col string) string {
	expr := strings.TrimSpace(col)
	expr = strings.ToLower(expr)
	expr = stripIdentifierQuotes(expr)

	for strings.HasPrefix(expr, "((") && strings.HasSuffix(expr, "))") {
		expr = expr[1 : len(expr)-1]
	}

	expr = removeTypeCasts(expr)
	expr = regexp.MustCompile(`\s+`).ReplaceAllString(expr, " ")
	expr = strings.TrimSpace(expr)
	expr = canonicalizeSortOrder(expr)

	return expr
}

func canonicalizeSortOrder(expr string) string {
	expr = strings.TrimSuffix(expr, " asc nulls last")
	expr = strings.TrimSuffix(expr, " asc")
	expr = strings.TrimSuffix(expr, " nulls last")

	if strings.HasSuffix(expr, " desc nulls first") {
		expr = strings.TrimSuffix(expr, " nulls first")
	}

	return strings.TrimSpace(expr)
}

func stripIdentifierQuotes(expr string) string {
	if !strings.Contains(expr, `"`) {
		return expr
	}

	return strings.ReplaceAll(expr, `"`, "")
}

func equalStringSlicesSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	aSorted := make([]string, len(a))
	bSorted := make([]string, len(b))

	copy(aSorted, a)
	copy(bSorted, b)

	sort.Strings(aSorted)
	sort.Strings(bSorted)

	for i := range aSorted {
		if !strings.EqualFold(aSorted[i], bSorted[i]) {
			return false
		}
	}

	return true
}
