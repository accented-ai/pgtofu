package differ

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func (d *Differ) compareHypertables(result *DiffResult) {
	currentHypertables := buildHypertableMap(result.Current.Hypertables)
	desiredHypertables := buildHypertableMap(result.Desired.Hypertables)

	for key, hypertable := range desiredHypertables {
		if _, exists := currentHypertables[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeAddHypertable,
				Severity: SeveritySafe,
				Description: fmt.Sprintf(
					"Convert table to hypertable: %s (time column: %s, interval: %s)",
					hypertable.QualifiedTableName(),
					hypertable.TimeColumnName,
					hypertable.PartitionInterval,
				),
				ObjectType: "hypertable",
				ObjectName: key,
				Details:    map[string]any{"hypertable": hypertable},
			})

			if hypertable.CompressionEnabled {
				dummy := *hypertable
				dummy.CompressionEnabled = false
				d.compareCompressionSettings(result, &dummy, hypertable)
			}

			if hypertable.RetentionPolicy != nil {
				dummy := *hypertable
				dummy.RetentionPolicy = nil
				d.compareRetentionPolicies(result, &dummy, hypertable)
			}
		}
	}

	for key, hypertable := range currentHypertables {
		if _, exists := desiredHypertables[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropHypertable,
				Severity:    SeverityBreaking,
				Description: "Convert hypertable to regular table: " + hypertable.QualifiedTableName(),
				ObjectType:  "hypertable",
				ObjectName:  key,
				Details:     map[string]any{"hypertable": hypertable},
			})
		}
	}

	for key, desiredHT := range desiredHypertables {
		if currentHT, exists := currentHypertables[key]; exists {
			d.compareCompressionSettings(result, currentHT, desiredHT)
			d.compareRetentionPolicies(result, currentHT, desiredHT)

			if currentHT.PartitionInterval != desiredHT.PartitionInterval {
				result.Warnings = append(result.Warnings, fmt.Sprintf(
					"Partition interval change detected for %s: %s -> %s. This requires recreating the hypertable.",
					currentHT.QualifiedTableName(),
					currentHT.PartitionInterval,
					desiredHT.PartitionInterval,
				))
			}
		}
	}
}

func (d *Differ) compareCompressionSettings(
	result *DiffResult,
	current, desired *schema.Hypertable,
) {
	tableKey := TableKey(current.Schema, current.TableName)
	tableName := current.QualifiedTableName()

	if !current.CompressionEnabled && desired.CompressionEnabled {
		result.Changes = append(result.Changes, Change{
			Type:        ChangeTypeAddCompressionPolicy,
			Severity:    SeveritySafe,
			Description: "Enable compression on hypertable: " + tableName,
			ObjectType:  "compression_policy",
			ObjectName:  tableKey,
			Details: map[string]any{
				"hypertable": desired,
				"settings":   desired.CompressionSettings,
			},
		})

		return
	}

	if current.CompressionEnabled && !desired.CompressionEnabled {
		result.Changes = append(result.Changes, Change{
			Type:        ChangeTypeDropCompressionPolicy,
			Severity:    SeverityPotentiallyBreaking,
			Description: "Disable compression on hypertable: " + tableName,
			ObjectType:  "compression_policy",
			ObjectName:  tableKey,
			Details:     map[string]any{"hypertable": current},
		})

		return
	}

	if current.CompressionEnabled && desired.CompressionEnabled {
		if !areCompressionSettingsEqual(current.CompressionSettings, desired.CompressionSettings) {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeModifyCompressionPolicy,
				Severity:    SeverityPotentiallyBreaking,
				Description: "Modify compression settings for hypertable: " + tableName,
				ObjectType:  "compression_policy",
				ObjectName:  tableKey,
				Details: map[string]any{
					"current_settings": current.CompressionSettings,
					"desired_settings": desired.CompressionSettings,
				},
			})
		}
	}
}

func (d *Differ) compareRetentionPolicies(
	result *DiffResult,
	current, desired *schema.Hypertable,
) {
	tableKey := TableKey(current.Schema, current.TableName)
	tableName := current.QualifiedTableName()

	if current.RetentionPolicy == nil && desired.RetentionPolicy != nil {
		result.Changes = append(result.Changes, Change{
			Type:     ChangeTypeAddRetentionPolicy,
			Severity: SeverityBreaking,
			Description: fmt.Sprintf("Add retention policy to hypertable: %s (drop after: %s)",
				tableName, desired.RetentionPolicy.DropAfter),
			ObjectType: "retention_policy",
			ObjectName: tableKey,
			Details:    map[string]any{"policy": desired.RetentionPolicy},
		})

		return
	}

	if current.RetentionPolicy != nil && desired.RetentionPolicy == nil {
		result.Changes = append(result.Changes, Change{
			Type:        ChangeTypeDropRetentionPolicy,
			Severity:    SeveritySafe,
			Description: "Remove retention policy from hypertable: " + tableName,
			ObjectType:  "retention_policy",
			ObjectName:  tableKey,
			Details:     map[string]any{"policy": current.RetentionPolicy},
		})

		return
	}

	if current.RetentionPolicy != nil && desired.RetentionPolicy != nil {
		if current.RetentionPolicy.DropAfter != desired.RetentionPolicy.DropAfter {
			severity := SeverityPotentiallyBreaking
			if isRetentionShorter(
				desired.RetentionPolicy.DropAfter,
				current.RetentionPolicy.DropAfter,
			) {
				severity = SeverityBreaking
			}

			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeModifyRetentionPolicy,
				Severity: severity,
				Description: fmt.Sprintf(
					"Modify retention policy for hypertable: %s (%s -> %s)",
					tableName,
					current.RetentionPolicy.DropAfter,
					desired.RetentionPolicy.DropAfter,
				),
				ObjectType: "retention_policy",
				ObjectName: tableKey,
				Details: map[string]any{
					"current_policy": current.RetentionPolicy,
					"desired_policy": desired.RetentionPolicy,
				},
			})
		}
	}
}

func (d *Differ) compareContinuousAggregates(result *DiffResult) {
	currentAggs := buildContinuousAggregateMap(result.Current.ContinuousAggregates)
	desiredAggs := buildContinuousAggregateMap(result.Desired.ContinuousAggregates)

	for key, agg := range desiredAggs {
		if _, exists := currentAggs[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeAddContinuousAggregate,
				Severity: SeveritySafe,
				Description: fmt.Sprintf("Add continuous aggregate: %s on %s",
					agg.QualifiedViewName(), agg.QualifiedHypertableName()),
				ObjectType: "continuous_aggregate",
				ObjectName: key,
				Details:    map[string]any{"aggregate": agg},
				DependsOn:  []string{agg.QualifiedHypertableName()},
			})
		}
	}

	for key, agg := range currentAggs {
		if _, exists := desiredAggs[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropContinuousAggregate,
				Severity:    SeverityBreaking,
				Description: "Drop continuous aggregate: " + agg.QualifiedViewName(),
				ObjectType:  "continuous_aggregate",
				ObjectName:  key,
				Details:     map[string]any{"aggregate": agg},
			})
		}
	}

	for key, desiredAgg := range desiredAggs {
		if currentAgg, exists := currentAggs[key]; exists {
			if !areContinuousAggregatesEqual(currentAgg, desiredAgg) {
				result.Changes = append(result.Changes, Change{
					Type:        ChangeTypeModifyContinuousAggregate,
					Severity:    SeverityBreaking,
					Description: "Modify continuous aggregate: " + desiredAgg.QualifiedViewName(),
					ObjectType:  "continuous_aggregate",
					ObjectName:  key,
					Details:     map[string]any{"current": currentAgg, "desired": desiredAgg},
					DependsOn:   []string{desiredAgg.QualifiedHypertableName()},
				})
			}
		}
	}
}

func buildHypertableMap(hypertables []schema.Hypertable) map[string]*schema.Hypertable {
	m := make(map[string]*schema.Hypertable, len(hypertables))
	for i := range hypertables {
		key := TableKey(hypertables[i].Schema, hypertables[i].TableName)
		m[key] = &hypertables[i]
	}

	return m
}

func buildContinuousAggregateMap(
	aggs []schema.ContinuousAggregate,
) map[string]*schema.ContinuousAggregate {
	m := make(map[string]*schema.ContinuousAggregate, len(aggs))
	for i := range aggs {
		key := ViewKey(aggs[i].Schema, aggs[i].ViewName)
		m[key] = &aggs[i]
	}

	return m
}

func areCompressionSettingsEqual(s1, s2 *schema.CompressionSettings) bool {
	if s1 == nil && s2 == nil {
		return true
	}

	if s1 == nil || s2 == nil {
		return false
	}

	if !equalStringSlicesSorted(
		normalizeSegmentColumns(s1.SegmentByColumns),
		normalizeSegmentColumns(s2.SegmentByColumns),
	) {
		return false
	}

	return equalOrderByColumns(s1.OrderByColumns, s2.OrderByColumns)
}

func areContinuousAggregatesEqual(a1, a2 *schema.ContinuousAggregate) bool {
	if NormalizeViewDefinition(a1.Query) != NormalizeViewDefinition(a2.Query) {
		return false
	}

	if a1.QualifiedHypertableName() != a2.QualifiedHypertableName() {
		return false
	}

	if !areRefreshPoliciesEqual(a1.RefreshPolicy, a2.RefreshPolicy) {
		return false
	}

	return normalizeComment(a1.Comment) == normalizeComment(a2.Comment)
}

func areRefreshPoliciesEqual(p1, p2 *schema.RefreshPolicy) bool {
	if p1 == nil && p2 == nil {
		return true
	}

	if p1 == nil || p2 == nil {
		return false
	}

	return normalizeInterval(p1.StartOffset) == normalizeInterval(p2.StartOffset) &&
		normalizeInterval(p1.EndOffset) == normalizeInterval(p2.EndOffset) &&
		normalizeInterval(p1.ScheduleInterval) == normalizeInterval(p2.ScheduleInterval)
}

func normalizeInterval(interval string) string {
	s := strings.ToLower(strings.TrimSpace(interval))

	if strings.Contains(s, ":") {
		parts := strings.Split(s, ":")
		if len(parts) == 3 && parts[0] != "" && parts[1] == "00" && parts[2] == "00" {
			hours := strings.TrimLeft(parts[0], "0")
			if hours == "" {
				hours = "0"
			}

			s = hours + " hour"
		}
	}

	replacements := map[string]string{
		"hours":   "hour",
		"days":    "day",
		"months":  "month",
		"years":   "year",
		"minutes": "minute",
		"seconds": "second",
		"mons":    "mon",
	}

	for old, new := range replacements {
		s = strings.ReplaceAll(s, old, new)
	}

	return strings.Join(strings.Fields(s), " ")
}

func isRetentionShorter(interval1, interval2 string) bool {
	return len(strings.ToLower(interval1)) < len(strings.ToLower(interval2))
}

func normalizeSegmentColumns(columns []string) []string {
	unique := make(map[string]struct{}, len(columns))

	var result []string //nolint:prealloc

	for _, col := range columns {
		clean := strings.ToLower(strings.TrimSpace(col))
		if clean == "" {
			continue
		}

		if _, exists := unique[clean]; exists {
			continue
		}

		unique[clean] = struct{}{}
		result = append(result, clean)
	}

	return result
}

func equalOrderByColumns(a, b []schema.OrderByColumn) bool {
	normA := normalizeOrderByColumns(a)
	normB := normalizeOrderByColumns(b)

	if len(normA) != len(normB) {
		return false
	}

	for i := range normA {
		if !equalOrderByColumn(normA[i], normB[i]) {
			return false
		}
	}

	return true
}

func equalOrderByColumn(a, b schema.OrderByColumn) bool {
	colA := strings.ToLower(strings.TrimSpace(a.Column))

	colB := strings.ToLower(strings.TrimSpace(b.Column))
	if colA != colB {
		return false
	}

	dirA := normalizeOrderDirection(a.Direction)
	dirB := normalizeOrderDirection(b.Direction)

	return dirA == dirB
}

func normalizeOrderDirection(direction string) string {
	dir := strings.ToUpper(strings.TrimSpace(direction))
	if dir == "" {
		return "ASC"
	}

	return dir
}

func normalizeOrderByColumns(cols []schema.OrderByColumn) []schema.OrderByColumn {
	result := make([]schema.OrderByColumn, 0, len(cols))
	seen := make(map[string]struct{}, len(cols))

	for _, col := range cols {
		cleanCol := strings.ToLower(strings.TrimSpace(col.Column))
		if cleanCol == "" {
			continue
		}

		dir := normalizeOrderDirection(col.Direction)

		key := fmt.Sprintf("%s|%s", cleanCol, dir)
		if _, exists := seen[key]; exists {
			continue
		}

		seen[key] = struct{}{}

		result = append(result, schema.OrderByColumn{
			Column:    cleanCol,
			Direction: dir,
		})
	}

	return result
}
