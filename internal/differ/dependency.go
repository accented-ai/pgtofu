package differ

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func (d *Differ) resolveDependencies(result *DiffResult) error {
	graph := newDependencyGraph()

	for i := range result.Changes {
		graph.addNode(i, &result.Changes[i])
	}

	for i := range result.Changes {
		change := &result.Changes[i]

		for _, dep := range change.DependsOn {
			for j := range result.Changes {
				if providesObject(&result.Changes[j], dep) {
					graph.addEdge(i, j)
				}
			}
		}

		for j := range result.Changes {
			if i != j && d.implicitlyDependsOn(change, &result.Changes[j]) {
				graph.addEdge(i, j)
			}
		}
	}

	order, err := graph.topologicalSort()
	if err != nil {
		return util.WrapError("resolving dependencies", err)
	}

	for orderIndex, changeIndex := range order {
		result.Changes[changeIndex].Order = orderIndex
	}

	sort.Slice(result.Changes, func(i, j int) bool {
		return result.Changes[i].Order < result.Changes[j].Order
	})

	return nil
}

func providesObject(change *Change, objectName string) bool {
	switch change.Type {
	case ChangeTypeAddTable, ChangeTypeAddView, ChangeTypeAddMaterializedView,
		ChangeTypeAddFunction:
		if change.ObjectName == objectName {
			return true
		}

		if !strings.Contains(objectName, ".") { //nolint:nestif
			parts := strings.Split(change.ObjectName, ".")
			if len(parts) == 2 {
				tableName := strings.ToLower(parts[1])
				if tableName == strings.ToLower(objectName) {
					schemaPart := strings.ToLower(parts[0])
					if schemaPart == schema.DefaultSchema || schemaPart == "public" {
						return true
					}
				}
			}
		}

		if !strings.Contains(change.ObjectName, ".") && //nolint:nestif
			strings.Contains(objectName, ".") {
			parts := strings.Split(objectName, ".")
			if len(parts) == 2 {
				tableName := strings.ToLower(parts[1])
				if tableName == strings.ToLower(change.ObjectName) {
					schemaPart := strings.ToLower(parts[0])
					if schemaPart == schema.DefaultSchema || schemaPart == "public" {
						return true
					}
				}
			}
		}

		return false
	case ChangeTypeAddHypertable:
		return change.ObjectName == objectName
	}

	return false
}

func extractSchemaFromChange(change *Change) string {
	if change.Type == ChangeTypeAddSchema || change.Type == ChangeTypeDropSchema {
		return change.ObjectName
	}

	if change.ObjectName == "" {
		return schema.DefaultSchema
	}

	parts := strings.Split(change.ObjectName, ".")
	if len(parts) < 2 {
		return schema.DefaultSchema
	}

	schemaName := strings.ToLower(parts[0])
	if schemaName == "" {
		return schema.DefaultSchema
	}

	return schemaName
}

func tableMatchesDependency(tableName string, dependencies []string) bool {
	tableLower := strings.ToLower(tableName)

	for _, dep := range dependencies {
		depLower := strings.ToLower(dep)

		if tableLower == depLower {
			return true
		}

		tableParts := strings.Split(tableLower, ".")
		depParts := strings.Split(depLower, ".")

		if len(tableParts) == 2 && len(depParts) == 1 {
			if (tableParts[0] == schema.DefaultSchema || tableParts[0] == "public") &&
				tableParts[1] == depParts[0] {
				return true
			}
		}

		if len(tableParts) == 1 && len(depParts) == 2 {
			if (depParts[0] == schema.DefaultSchema || depParts[0] == "public") &&
				depParts[1] == tableParts[0] {
				return true
			}
		}
	}

	return false
}

func indexUsesColumn(indexChange *Change, tableName, columnName string) bool {
	idx, ok := indexChange.Details["index"].(*schema.Index)
	if !ok {
		return false
	}

	if !strings.EqualFold(idx.QualifiedTableName(), tableName) {
		return false
	}

	columnLower := strings.ToLower(columnName)

	for _, col := range idx.Columns {
		if strings.EqualFold(col, columnLower) {
			return true
		}
	}

	for _, col := range idx.IncludeColumns {
		if strings.EqualFold(col, columnLower) {
			return true
		}
	}

	return false
}

func constraintUsesColumn(constraintChange *Change, tableName, columnName string) bool {
	constraint, ok := constraintChange.Details["constraint"].(*schema.Constraint)
	if !ok {
		return false
	}

	changeTableName, _ := constraintChange.Details["table"].(string)
	if !strings.EqualFold(changeTableName, tableName) {
		return false
	}

	columnLower := strings.ToLower(columnName)

	for _, col := range constraint.Columns {
		if strings.EqualFold(col, columnLower) {
			return true
		}
	}

	return false
}

func getColumnFromChange(change *Change) (tableName, columnName string, ok bool) {
	tableName, _ = change.Details["table"].(string)
	if tableName == "" {
		return "", "", false
	}

	col, ok := change.Details["column"].(*schema.Column)
	if !ok || col == nil {
		return "", "", false
	}

	return tableName, col.Name, true
}

func (d *Differ) implicitlyDependsOn( //nolint:cyclop,gocognit,gocyclo,maintidx
	change *Change,
	otherChange *Change,
) bool {
	if change.Type != ChangeTypeAddSchema && otherChange.Type == ChangeTypeAddSchema {
		changeSchema := extractSchemaFromChange(change)

		otherSchema := otherChange.ObjectName
		if changeSchema != "" && changeSchema == otherSchema {
			return true
		}
	}

	if change.Type != ChangeTypeAddExtension && change.Type != ChangeTypeModifyExtension {
		if otherChange.Type == ChangeTypeAddExtension ||
			otherChange.Type == ChangeTypeModifyExtension {
			return true
		}
	}

	if change.Type == ChangeTypeAddTable && otherChange.Type == ChangeTypeAddCustomType {
		return true
	}

	if (change.Type == ChangeTypeAddIndex || change.Type == ChangeTypeAddConstraint) &&
		otherChange.Type == ChangeTypeAddTable &&
		change.ObjectName == otherChange.ObjectName {
		return true
	}

	if (change.Type == ChangeTypeAddView || change.Type == ChangeTypeAddMaterializedView) &&
		otherChange.Type == ChangeTypeAddTable &&
		slices.Contains(change.DependsOn, otherChange.ObjectName) {
		return true
	}

	if change.Type == ChangeTypeAddTrigger &&
		otherChange.Type == ChangeTypeAddFunction &&
		slices.Contains(change.DependsOn, otherChange.ObjectName) {
		return true
	}

	if change.Type == ChangeTypeAddHypertable &&
		otherChange.Type == ChangeTypeAddTable &&
		change.ObjectName == otherChange.ObjectName {
		return true
	}

	if (change.Type == ChangeTypeAddCompressionPolicy || change.Type == ChangeTypeAddRetentionPolicy) &&
		otherChange.Type == ChangeTypeAddHypertable &&
		change.ObjectName == otherChange.ObjectName {
		return true
	}

	if change.Type == ChangeTypeDropTable &&
		otherChange.Type == ChangeTypeDropView &&
		slices.Contains(otherChange.DependsOn, change.ObjectName) {
		return true
	}

	if change.Type == ChangeTypeDropFunction &&
		otherChange.Type == ChangeTypeDropTrigger &&
		slices.Contains(otherChange.DependsOn, change.ObjectName) {
		return true
	}

	if change.Type == ChangeTypeDropTable &&
		otherChange.Type == ChangeTypeDropConstraint &&
		slices.Contains(otherChange.DependsOn, change.ObjectName) {
		return true
	}

	if change.Type == ChangeTypeDropHypertable &&
		(otherChange.Type == ChangeTypeDropCompressionPolicy || otherChange.Type == ChangeTypeDropRetentionPolicy) &&
		change.ObjectName == otherChange.ObjectName {
		return true
	}

	if change.Type == ChangeTypeModifyTableComment &&
		otherChange.Type == ChangeTypeAddTable &&
		change.ObjectName == otherChange.ObjectName {
		return true
	}

	if change.Type == ChangeTypeModifyColumnComment {
		if otherChange.Type == ChangeTypeAddColumn &&
			change.ObjectName == otherChange.ObjectName {
			return true
		}

		oldComment, _ := change.Details["old_comment"].(string)
		if oldComment == "" &&
			otherChange.Type == ChangeTypeAddTable &&
			change.ObjectName == otherChange.ObjectName {
			return true
		}
	}

	if change.Type == ChangeTypeModifyMaterializedView {
		_, hasOldComment := change.Details["old_comment"]

		_, hasNewComment := change.Details["new_comment"]
		if hasOldComment && hasNewComment &&
			otherChange.Type == ChangeTypeAddMaterializedView &&
			change.ObjectName == otherChange.ObjectName {
			return true
		}
	}

	if change.Type == ChangeTypeModifyFunction {
		_, hasOldComment := change.Details["old_comment"]
		_, hasNewComment := change.Details["new_comment"]
		_, hasCurrent := change.Details["current"]
		_, hasDesired := change.Details["desired"]

		if hasOldComment && hasNewComment && !hasCurrent && !hasDesired {
			oldComment, _ := change.Details["old_comment"].(string)
			if oldComment == "" &&
				otherChange.Type == ChangeTypeAddFunction &&
				change.ObjectName == otherChange.ObjectName {
				return true
			}
		}
	}

	if change.Type == ChangeTypeModifyView {
		_, hasOldComment := change.Details["old_comment"]
		_, hasNewComment := change.Details["new_comment"]
		_, hasCurrent := change.Details["current"]
		_, hasDesired := change.Details["desired"]

		if hasOldComment && hasNewComment && !hasCurrent && !hasDesired {
			oldComment, _ := change.Details["old_comment"].(string)
			if oldComment == "" &&
				otherChange.Type == ChangeTypeAddView &&
				change.ObjectName == otherChange.ObjectName {
				return true
			}
		}
	}

	if (change.Type == ChangeTypeModifyView || change.Type == ChangeTypeModifyMaterializedView) &&
		otherChange.Type == ChangeTypeAddColumn &&
		tableMatchesDependency(otherChange.ObjectName, change.DependsOn) {
		return true
	}

	if (change.Type == ChangeTypeAddView || change.Type == ChangeTypeAddMaterializedView) &&
		otherChange.Type == ChangeTypeAddColumn &&
		tableMatchesDependency(otherChange.ObjectName, change.DependsOn) {
		return true
	}

	if change.Type == ChangeTypeDropColumn &&
		(otherChange.Type == ChangeTypeModifyView || otherChange.Type == ChangeTypeModifyMaterializedView) &&
		tableMatchesDependency(change.ObjectName, otherChange.DependsOn) {
		return true
	}

	if change.Type == ChangeTypeDropColumn &&
		(otherChange.Type == ChangeTypeDropView || otherChange.Type == ChangeTypeDropMaterializedView) &&
		tableMatchesDependency(change.ObjectName, otherChange.DependsOn) {
		return true
	}

	if (change.Type == ChangeTypeAddIndex || change.Type == ChangeTypeModifyIndex) &&
		otherChange.Type == ChangeTypeAddColumn {
		tableName, columnName, ok := getColumnFromChange(otherChange)
		if ok && indexUsesColumn(change, tableName, columnName) {
			return true
		}
	}

	if (change.Type == ChangeTypeAddConstraint || change.Type == ChangeTypeModifyConstraint) &&
		otherChange.Type == ChangeTypeAddColumn {
		tableName, columnName, ok := getColumnFromChange(otherChange)
		if ok && constraintUsesColumn(change, tableName, columnName) {
			return true
		}
	}

	if change.Type == ChangeTypeDropColumn &&
		otherChange.Type == ChangeTypeDropIndex {
		tableName, columnName, ok := getColumnFromChange(change)
		if ok && indexUsesColumn(otherChange, tableName, columnName) {
			return true
		}
	}

	if change.Type == ChangeTypeDropColumn &&
		otherChange.Type == ChangeTypeDropConstraint {
		tableName, columnName, ok := getColumnFromChange(change)
		if ok && constraintUsesColumn(otherChange, tableName, columnName) {
			return true
		}
	}

	return false
}

type dependencyGraph struct {
	nodes    map[int]*Change
	edges    map[int]map[int]bool
	inDegree map[int]int
}

func newDependencyGraph() *dependencyGraph {
	return &dependencyGraph{
		nodes:    make(map[int]*Change),
		edges:    make(map[int]map[int]bool),
		inDegree: make(map[int]int),
	}
}

func (g *dependencyGraph) addNode(index int, change *Change) {
	g.nodes[index] = change
	if g.edges[index] == nil {
		g.edges[index] = make(map[int]bool)
	}

	if _, exists := g.inDegree[index]; !exists {
		g.inDegree[index] = 0
	}
}

func (g *dependencyGraph) addEdge(from, to int) {
	if g.edges[from] == nil {
		g.edges[from] = make(map[int]bool)
	}

	if g.edges[from][to] {
		return
	}

	g.edges[from][to] = true
	g.inDegree[from]++
}

func (g *dependencyGraph) topologicalSort() ([]int, error) {
	inDegree := make(map[int]int)
	maps.Copy(inDegree, g.inDegree)

	var queue []int

	for node := range g.nodes {
		if inDegree[node] == 0 {
			queue = append(queue, node)
		}
	}

	g.sortQueue(queue)

	var result []int

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		result = append(result, node)

		for dependent := range g.edges {
			if g.edges[dependent][node] {
				inDegree[dependent]--
				if inDegree[dependent] == 0 {
					queue = append(queue, dependent)
					g.sortQueue(queue)
				}
			}
		}
	}

	if len(result) != len(g.nodes) {
		cycle := g.findCycle(inDegree)
		return nil, fmt.Errorf("circular dependency detected: %s", cycle)
	}

	return result, nil
}

func (g *dependencyGraph) sortQueue(queue []int) {
	sort.Slice(queue, func(i, j int) bool {
		changeI := g.nodes[queue[i]]
		changeJ := g.nodes[queue[j]]

		objectNameI := g.getObjectNameForSorting(changeI)
		objectNameJ := g.getObjectNameForSorting(changeJ)

		if objectNameI != objectNameJ {
			return objectNameI < objectNameJ
		}

		priorityI := getChangePriority(changeI.Type)
		priorityJ := getChangePriority(changeJ.Type)

		if priorityI != priorityJ {
			return priorityI < priorityJ
		}

		return queue[i] < queue[j]
	})
}

func (g *dependencyGraph) getObjectNameForSorting(change *Change) string {
	if change.ObjectName != "" {
		return change.ObjectName
	}

	if change.ObjectType != "" {
		return change.ObjectType
	}

	return string(change.Type)
}

func getChangePriority(changeType ChangeType) int { //nolint:cyclop
	switch changeType {
	case ChangeTypeAddSchema:
		return 1
	case ChangeTypeAddExtension, ChangeTypeModifyExtension:
		return 2
	case ChangeTypeAddCustomType:
		return 3
	case ChangeTypeAddSequence:
		return 4
	case ChangeTypeAddTable:
		return 10
	case ChangeTypeAddColumn:
		return 20
	case ChangeTypeAddConstraint:
		return 30
	case ChangeTypeAddIndex:
		return 40
	case ChangeTypeModifyTableComment:
		return 11
	case ChangeTypeModifyColumnComment:
		return 21
	case ChangeTypeModifyColumnType:
		return 22
	case ChangeTypeModifyColumnNullability:
		return 23
	case ChangeTypeModifyColumnDefault:
		return 24
	case ChangeTypeAddView:
		return 50
	case ChangeTypeModifyView:
		return 51
	case ChangeTypeAddMaterializedView:
		return 60
	case ChangeTypeModifyMaterializedView:
		return 61
	case ChangeTypeAddFunction:
		return 70
	case ChangeTypeModifyFunction:
		return 71
	case ChangeTypeAddTrigger:
		return 80
	case ChangeTypeAddHypertable:
		return 90
	case ChangeTypeAddCompressionPolicy:
		return 91
	case ChangeTypeAddRetentionPolicy:
		return 92
	case ChangeTypeAddContinuousAggregate:
		return 100
	default:
		return 1000
	}
}

func (g *dependencyGraph) findCycle(remainingInDegree map[int]int) string {
	remaining := make([]int, 0, len(remainingInDegree))
	for node, degree := range remainingInDegree {
		if degree > 0 {
			remaining = append(remaining, node)
		}
	}

	graph := CycleGraph[int]{
		GetEdges: func(node int) []int {
			if g.edges[node] == nil {
				return nil
			}

			deps := make([]int, 0, len(g.edges[node]))
			for dep := range g.edges[node] {
				deps = append(deps, dep)
			}

			return deps
		},
		FormatNode: func(node int) string {
			change := g.nodes[node]
			return fmt.Sprintf("%s (%s)", change.Description, change.ObjectName)
		},
		HasRemaining: func(node int) bool {
			return remainingInDegree[node] > 0
		},
	}

	return FindCycle(remaining, graph)
}
