package differ

import (
	"errors"
	"fmt"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

type Differ struct {
	options      *Options
	tableComp    *TableComparator
	indexComp    *IndexComparator
	viewComp     *ViewComparator
	functionComp *FunctionComparator
	triggerComp  *TriggerComparator
}

type Options struct {
	IgnoreComments        bool
	IgnoreOwners          bool
	IgnoreTablespaces     bool
	DetectRenames         bool
	IgnoreIndexNames      bool
	IgnoreConstraintNames bool
}

func DefaultOptions() *Options {
	return &Options{
		IgnoreComments:        false,
		IgnoreOwners:          true,
		IgnoreTablespaces:     true,
		DetectRenames:         true,
		IgnoreIndexNames:      false,
		IgnoreConstraintNames: false,
	}
}

func New(opts *Options) *Differ {
	if opts == nil {
		opts = DefaultOptions()
	}

	return &Differ{
		options:      opts,
		tableComp:    NewTableComparator(opts),
		indexComp:    NewIndexComparator(opts),
		viewComp:     NewViewComparator(opts),
		functionComp: NewFunctionComparator(opts),
		triggerComp:  NewTriggerComparator(),
	}
}

func (d *Differ) compareSchemas(result *DiffResult) {
	currentSchemas := make(map[string]schema.Schema)
	for _, sch := range result.Current.Schemas {
		currentSchemas[sch.Name] = sch
	}

	desiredSchemas := make(map[string]schema.Schema)
	for _, sch := range result.Desired.Schemas {
		desiredSchemas[sch.Name] = sch
	}

	for key, sch := range desiredSchemas {
		if _, exists := currentSchemas[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeAddSchema,
				Severity:    SeveritySafe,
				Description: "Add schema: " + sch.Name,
				ObjectType:  "schema",
				ObjectName:  sch.Name,
				Details:     map[string]any{"schema": sch},
			})
		}
	}

	for key, sch := range currentSchemas {
		if _, exists := desiredSchemas[key]; !exists {
			if sch.Name == schema.DefaultSchema {
				continue
			}

			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropSchema,
				Severity:    SeverityBreaking,
				Description: "Drop schema: " + sch.Name,
				ObjectType:  "schema",
				ObjectName:  sch.Name,
				Details:     map[string]any{"schema": sch},
			})
		}
	}
}

func (d *Differ) Compare(current, desired *schema.Database) (*DiffResult, error) {
	if current == nil {
		return nil, errors.New("current schema is nil")
	}

	if desired == nil {
		return nil, errors.New("desired schema is nil")
	}

	result := &DiffResult{
		Current:  current,
		Desired:  desired,
		Changes:  []Change{},
		Warnings: []string{},
	}

	d.compareSchemas(result)
	d.compareExtensions(result)
	d.compareCustomTypes(result)
	d.compareSequences(result)
	d.tableComp.Compare(result)
	d.indexComp.Compare(result)
	d.compareViews(result)
	d.compareMaterializedViews(result)
	d.functionComp.Compare(result)
	d.triggerComp.Compare(result)
	d.compareHypertables(result)
	d.compareContinuousAggregates(result)
	d.filterDuplicateCAIndexChanges(result)
	d.processViewRecreationForColumnTypeChanges(result)
	d.processContinuousAggregateRecreationForColumnChanges(result)

	if err := d.resolveDependencies(result); err != nil {
		return nil, util.WrapError("resolving dependencies", err)
	}

	d.computeStats(result)

	return result, nil
}

func (d *Differ) compareExtensions(result *DiffResult) {
	currentExts := make(map[string]schema.Extension)
	for _, ext := range result.Current.Extensions {
		currentExts[ext.Name] = ext
	}

	desiredExts := make(map[string]schema.Extension)
	for _, ext := range result.Desired.Extensions {
		desiredExts[ext.Name] = ext
	}

	for key, ext := range desiredExts {
		if current, exists := currentExts[key]; exists {
			if extensionNeedsUpdate(current, ext) {
				result.Changes = append(result.Changes, Change{
					Type:        ChangeTypeModifyExtension,
					Severity:    SeverityPotentiallyBreaking,
					Description: "Modify extension: " + ext.Name,
					ObjectType:  "extension",
					ObjectName:  ext.Name,
					Details: map[string]any{
						"current": current,
						"desired": ext,
					},
				})
			}

			continue
		}

		result.Changes = append(result.Changes, Change{
			Type:        ChangeTypeAddExtension,
			Severity:    SeveritySafe,
			Description: "Add extension: " + ext.Name,
			ObjectType:  "extension",
			ObjectName:  ext.Name,
			Details:     map[string]any{"extension": ext},
		})
	}

	for key, ext := range currentExts {
		if _, exists := desiredExts[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropExtension,
				Severity:    SeverityBreaking,
				Description: "Drop extension: " + ext.Name,
				ObjectType:  "extension",
				ObjectName:  ext.Name,
				Details:     map[string]any{"extension": ext},
			})
		}
	}
}

func extensionNeedsUpdate(current, desired schema.Extension) bool {
	if schema.NormalizeSchemaName(current.Schema) != schema.NormalizeSchemaName(desired.Schema) {
		return true
	}

	if desired.Version != "" && desired.Version != current.Version {
		return true
	}

	return false
}

func (d *Differ) compareCustomTypes(result *DiffResult) {
	currentTypes := make(map[string]schema.CustomType)
	for _, ct := range result.Current.CustomTypes {
		currentTypes[TableKey(ct.Schema, ct.Name)] = ct
	}

	desiredTypes := make(map[string]schema.CustomType)
	for _, ct := range result.Desired.CustomTypes {
		desiredTypes[TableKey(ct.Schema, ct.Name)] = ct
	}

	for key, ct := range desiredTypes {
		if _, exists := currentTypes[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeAddCustomType,
				Severity:    SeveritySafe,
				Description: fmt.Sprintf("Add %s type: %s", ct.Type, ct.Name),
				ObjectType:  "type",
				ObjectName:  key,
				Details:     map[string]any{"custom_type": ct},
			})
		}
	}

	for key, ct := range currentTypes {
		if _, exists := desiredTypes[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropCustomType,
				Severity:    SeverityBreaking,
				Description: fmt.Sprintf("Drop %s type: %s", ct.Type, ct.Name),
				ObjectType:  "type",
				ObjectName:  key,
				Details:     map[string]any{"custom_type": ct},
			})
		}
	}

	for key, desired := range desiredTypes {
		if current, exists := currentTypes[key]; exists {
			if desired.Type == "enum" && current.Type == "enum" {
				d.compareEnumValues(result, key, &current, &desired)
			}
		}
	}
}

func (d *Differ) compareEnumValues(
	result *DiffResult,
	key string,
	current, desired *schema.CustomType,
) {
	currentValues := make(map[string]bool)
	for _, v := range current.Values {
		currentValues[v] = true
	}

	for _, v := range desired.Values {
		if !currentValues[v] {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeModifyCustomType,
				Severity:    SeveritySafe,
				Description: fmt.Sprintf("Add enum value '%s' to type %s", v, desired.Name),
				ObjectType:  "type",
				ObjectName:  key,
				Details:     map[string]any{"enum_value": v, "type_name": desired.Name},
			})
		}
	}

	desiredValues := make(map[string]bool)
	for _, v := range desired.Values {
		desiredValues[v] = true
	}

	for _, v := range current.Values {
		if !desiredValues[v] {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeModifyCustomType,
				Severity:    SeverityBreaking,
				Description: fmt.Sprintf("Remove enum value '%s' from type %s", v, current.Name),
				ObjectType:  "type",
				ObjectName:  key,
				Details:     map[string]any{"enum_value": v, "type_name": current.Name},
			})
		}
	}
}

func (d *Differ) compareSequences(result *DiffResult) {
	currentSeqs := make(map[string]schema.Sequence)
	for _, seq := range result.Current.Sequences {
		currentSeqs[TableKey(seq.Schema, seq.Name)] = seq
	}

	desiredSeqs := make(map[string]schema.Sequence)
	for _, seq := range result.Desired.Sequences {
		desiredSeqs[TableKey(seq.Schema, seq.Name)] = seq
	}

	for key, seq := range desiredSeqs {
		if _, exists := currentSeqs[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeAddSequence,
				Severity:    SeveritySafe,
				Description: "Add sequence: " + seq.QualifiedName(),
				ObjectType:  "sequence",
				ObjectName:  key,
				Details:     map[string]any{"sequence": seq},
			})
		}
	}

	for key, seq := range currentSeqs {
		if _, exists := desiredSeqs[key]; !exists {
			severity := SeverityBreaking
			if seq.OwnedByTable != "" {
				severity = SeverityPotentiallyBreaking
			}

			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropSequence,
				Severity:    severity,
				Description: "Drop sequence: " + seq.QualifiedName(),
				ObjectType:  "sequence",
				ObjectName:  key,
				Details:     map[string]any{"sequence": seq},
			})
		}
	}

	for key, desired := range desiredSeqs {
		if current, exists := currentSeqs[key]; exists {
			if current.Increment != desired.Increment ||
				current.MinValue != desired.MinValue ||
				current.MaxValue != desired.MaxValue ||
				current.IsCyclic != desired.IsCyclic {
				result.Changes = append(result.Changes, Change{
					Type:        ChangeTypeModifySequence,
					Severity:    SeveritySafe,
					Description: "Modify sequence: " + desired.QualifiedName(),
					ObjectType:  "sequence",
					ObjectName:  key,
					Details:     map[string]any{"current": current, "desired": desired},
				})
			}
		}
	}
}

func (d *Differ) computeStats(result *DiffResult) {
	for _, change := range result.Changes {
		switch change.Type {
		case ChangeTypeAddTable:
			result.Stats.TablesAdded++
		case ChangeTypeDropTable:
			result.Stats.TablesDropped++
		case ChangeTypeAddColumn:
			result.Stats.ColumnsAdded++
			result.Stats.TablesModified++
		case ChangeTypeDropColumn:
			result.Stats.ColumnsDropped++
			result.Stats.TablesModified++
		case ChangeTypeModifyColumnType,
			ChangeTypeModifyColumnNullability,
			ChangeTypeModifyColumnDefault:
			result.Stats.ColumnsModified++
			result.Stats.TablesModified++
		case ChangeTypeAddIndex:
			result.Stats.IndexesAdded++
		case ChangeTypeDropIndex:
			result.Stats.IndexesDropped++
		case ChangeTypeAddConstraint:
			result.Stats.ConstraintsAdded++
		case ChangeTypeDropConstraint:
			result.Stats.ConstraintsDropped++
		case ChangeTypeAddView:
			result.Stats.ViewsAdded++
		case ChangeTypeDropView:
			result.Stats.ViewsDropped++
		case ChangeTypeModifyView:
			result.Stats.ViewsModified++
		case ChangeTypeAddFunction:
			result.Stats.FunctionsAdded++
		case ChangeTypeDropFunction:
			result.Stats.FunctionsDropped++
		case ChangeTypeModifyFunction:
			result.Stats.FunctionsModified++
		}
	}
}
