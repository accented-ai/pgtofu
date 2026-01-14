package extractor

import (
	"fmt"
	"strings"
)

type queryBuilder struct {
	excludeSchemas      []string
	excludeExtensions   []string
	includeSystemTables bool
	hasTimescaleDB      bool
}

func (qb *queryBuilder) schemaFilter() string {
	var builder strings.Builder

	builder.WriteString("table_schema NOT LIKE 'pg_temp_%'")
	builder.WriteString(" AND table_schema NOT LIKE 'pg_toast_temp_%'")

	if len(qb.excludeSchemas) > 0 {
		builder.WriteString(" AND table_schema NOT IN (")

		for i, s := range qb.excludeSchemas {
			if i > 0 {
				builder.WriteString(", ")
			}

			builder.WriteString(fmt.Sprintf("'%s'", s))
		}

		builder.WriteString(")")
	}

	return builder.String()
}

func (qb *queryBuilder) namespaceFilter(column string) string {
	var builder strings.Builder

	builder.WriteString(column)
	builder.WriteString(" NOT LIKE 'pg_temp_%' AND ")
	builder.WriteString(column)
	builder.WriteString(" NOT LIKE 'pg_toast_temp_%'")

	if len(qb.excludeSchemas) > 0 {
		builder.WriteString(" AND ")
		builder.WriteString(column)
		builder.WriteString(" NOT IN (")

		for i, s := range qb.excludeSchemas {
			if i > 0 {
				builder.WriteString(", ")
			}

			builder.WriteString(fmt.Sprintf("'%s'", s))
		}

		builder.WriteString(")")
	}

	return builder.String()
}

func (qb *queryBuilder) systemTableFilter() string {
	if qb.includeSystemTables {
		return ""
	}

	return "AND NOT (t.table_schema = 'public' AND t.table_name = 'schema_migrations')"
}

func (qb *queryBuilder) continuousAggregateFilter() string {
	if !qb.hasTimescaleDB {
		return ""
	}

	return `
		AND NOT EXISTS (
			SELECT 1 FROM timescaledb_information.continuous_aggregates ca
			WHERE ca.view_schema = v.table_schema
			AND ca.view_name = v.table_name
		)`
}

func (qb *queryBuilder) buildInClause(values []string) string {
	if len(values) == 0 {
		return "''"
	}

	var builder strings.Builder

	for i, v := range values {
		if i > 0 {
			builder.WriteString(", ")
		}

		builder.WriteString(fmt.Sprintf("'%s'", v))
	}

	return builder.String()
}

const (
	queryTables = `
		SELECT
			t.table_schema,
			t.table_name,
			obj_description(
				(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass::oid,
				'pg_class'
			) as table_comment,
			pg_catalog.pg_get_userbyid(c.relowner) as owner,
			ts.spcname as tablespace
		FROM information_schema.tables t
		JOIN pg_catalog.pg_class c ON c.relname = t.table_name
		JOIN pg_catalog.pg_namespace n ON n.nspname = t.table_schema AND c.relnamespace = n.oid
		LEFT JOIN pg_catalog.pg_tablespace ts ON c.reltablespace = ts.oid
		WHERE t.table_type = 'BASE TABLE'
		AND %s
		%s
		AND NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_inherits i
			WHERE i.inhrelid = c.oid AND i.inhparent != 0
		)
		ORDER BY t.table_schema, t.table_name`

	queryColumns = `
		SELECT
			c.column_name,
			c.data_type,
			c.is_nullable = 'YES',
			c.column_default,
			col_description(
				(quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass::oid,
				c.ordinal_position
			),
			c.ordinal_position,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.udt_name,
			c.is_identity = 'YES',
			c.identity_generation,
			c.is_generated = 'ALWAYS',
			c.generation_expression
		FROM information_schema.columns c
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position`

	queryConstraints = `
		SELECT
			con.conname,
			CASE con.contype
				WHEN 'p' THEN 'PRIMARY KEY'
				WHEN 'f' THEN 'FOREIGN KEY'
				WHEN 'u' THEN 'UNIQUE'
				WHEN 'c' THEN 'CHECK'
				WHEN 'x' THEN 'EXCLUDE'
			END,
			ARRAY(
				SELECT a.attname
				FROM unnest(con.conkey) AS u(attnum)
				JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = con.conrelid
				ORDER BY array_position(con.conkey, u.attnum)
			),
			pg_get_constraintdef(con.oid),
			fn.nspname,
			fc.relname,
			ARRAY(
				SELECT a.attname
				FROM unnest(con.confkey) AS u(attnum)
				JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = con.confrelid
				ORDER BY array_position(con.confkey, u.attnum)
			),
			CASE con.confupdtype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
			END,
			CASE con.confdeltype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
			END,
			con.condeferrable,
			con.condeferred
		FROM pg_constraint con
		JOIN pg_class c ON con.conrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_class fc ON con.confrelid = fc.oid
		LEFT JOIN pg_namespace fn ON fc.relnamespace = fn.oid
		WHERE n.nspname = $1 AND c.relname = $2
		AND con.conislocal = true
		AND NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_inherits i
			WHERE i.inhrelid = con.conrelid AND i.inhparent != 0
		)
		ORDER BY
			CASE con.contype
				WHEN 'p' THEN 1
				WHEN 'f' THEN 2
				WHEN 'u' THEN 3
				WHEN 'c' THEN 4
				ELSE 5
			END,
			con.conname`

	queryPartitionInfo = `
		SELECT
			pt.partstrat::text,
			COALESCE(
				array_agg(a.attname ORDER BY array_position(pt.partattrs, a.attnum))
				FILTER (WHERE a.attname IS NOT NULL),
				ARRAY[]::text[]
			)
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
		LEFT JOIN pg_attribute a ON a.attrelid = c.oid
			AND a.attnum = ANY(pt.partattrs)
			AND NOT a.attisdropped
		WHERE n.nspname = $1 AND c.relname = $2
		GROUP BY pt.partstrat`

	queryPartitions = `
		SELECT
			c2.relname,
			pg_get_expr(c2.relpartbound, c2.oid)
		FROM pg_class c1
		JOIN pg_namespace n1 ON c1.relnamespace = n1.oid
		JOIN pg_inherits i ON i.inhparent = c1.oid
		JOIN pg_class c2 ON i.inhrelid = c2.oid
		JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
		WHERE n1.nspname = $1 AND c1.relname = $2
		ORDER BY c2.relname`

	queryIndexes = `
		SELECT
			i.schemaname,
			i.indexname,
			i.tablename,
			ix.indisunique,
			ix.indisprimary,
			am.amname,
			pg_get_expr(ix.indpred, ix.indrelid),
			pg_get_indexdef(ix.indexrelid),
			ts.spcname
		FROM pg_indexes i
		JOIN pg_class c ON c.relname = i.indexname
		JOIN pg_index ix ON ix.indexrelid = c.oid
		JOIN pg_am am ON c.relam = am.oid
		LEFT JOIN pg_tablespace ts ON c.reltablespace = ts.oid
		WHERE i.schemaname = $1 AND i.tablename = $2
		ORDER BY i.indexname`

	queryIndexStorageParams = `
		SELECT unnest(c.reloptions)
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1 AND c.relname = $2 AND c.reloptions IS NOT NULL`

	queryHypertableDimensions = `
		SELECT column_name
		FROM timescaledb_information.dimensions
		WHERE hypertable_schema = $1 AND hypertable_name = $2
		ORDER BY dimension_number`

	queryViews = `
		SELECT
			v.table_schema,
			v.table_name,
			v.view_definition,
			obj_description(
				(quote_ident(v.table_schema) || '.' || quote_ident(v.table_name))::regclass::oid,
				'pg_class'
			),
			pg_catalog.pg_get_userbyid(c.relowner),
			v.check_option,
			v.is_updatable = 'YES'
		FROM information_schema.views v
		JOIN pg_catalog.pg_class c ON c.relname = v.table_name
		JOIN pg_catalog.pg_namespace n ON n.nspname = v.table_schema AND c.relnamespace = n.oid
		WHERE %s
		%s
		ORDER BY v.table_schema, v.table_name`

	queryMaterializedViews = `
		SELECT
			n.nspname,
			c.relname,
			pg_get_viewdef(c.oid),
			obj_description(c.oid, 'pg_class'),
			pg_catalog.pg_get_userbyid(c.relowner),
			ts.spcname,
			c.relispopulated
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_tablespace ts ON c.reltablespace = ts.oid
		WHERE c.relkind = 'm'
		AND %s
		ORDER BY n.nspname, c.relname`

	queryFunctions = `
		SELECT
			n.nspname,
			p.proname,
			pg_catalog.pg_get_function_arguments(p.oid),
			pg_catalog.pg_get_function_result(p.oid),
			l.lanname,
			CASE WHEN p.prosrc IS NOT NULL AND p.prosrc != '' THEN p.prosrc
				 ELSE pg_catalog.pg_get_functiondef(p.oid)
			END,
			p.prokind = 'a',
			p.prokind = 'w',
			p.proisstrict,
			p.prosecdef,
			CASE p.provolatile
				WHEN 'i' THEN 'IMMUTABLE'
				WHEN 's' THEN 'STABLE'
				WHEN 'v' THEN 'VOLATILE'
			END,
			obj_description(p.oid, 'pg_proc'),
			pg_catalog.pg_get_userbyid(p.proowner),
			pg_catalog.pg_get_functiondef(p.oid)
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		JOIN pg_language l ON p.prolang = l.oid
		WHERE %s
		AND p.prokind IN ('f', 'p')
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY n.nspname, p.proname, p.oid`

	queryTriggers = `
		SELECT
			n.nspname,
			t.tgname,
			c.relname,
			CASE
				WHEN t.tgtype & 1 = 1 THEN 'BEFORE'
				WHEN t.tgtype & 2 = 2 THEN 'AFTER'
				WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
			END,
			array_remove(ARRAY[
				CASE WHEN t.tgtype & 4 = 4 THEN 'INSERT' END,
				CASE WHEN t.tgtype & 8 = 8 THEN 'DELETE' END,
				CASE WHEN t.tgtype & 16 = 16 THEN 'UPDATE' END,
				CASE WHEN t.tgtype & 32 = 32 THEN 'TRUNCATE' END
			], NULL),
			(t.tgtype & 1 = 1),
			pg_get_triggerdef(t.oid),
			pn.nspname,
			p.proname,
			obj_description(t.oid, 'pg_trigger')
		FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_proc p ON t.tgfoid = p.oid
		JOIN pg_namespace pn ON p.pronamespace = pn.oid
		WHERE NOT t.tgisinternal
		AND %s
		ORDER BY n.nspname, c.relname, t.tgname`

	queryExtensions = `
		SELECT
			e.extname,
			n.nspname,
			e.extversion,
			obj_description(e.oid, 'pg_extension')
		FROM pg_extension e
		JOIN pg_namespace n ON e.extnamespace = n.oid
		WHERE e.extname NOT IN (%s)
		ORDER BY e.extname`

	queryCustomTypes = `
		SELECT
			n.nspname,
			t.typname,
			CASE t.typtype
				WHEN 'e' THEN 'enum'
				WHEN 'c' THEN 'composite'
				WHEN 'd' THEN 'domain'
				ELSE 'other'
			END,
			obj_description(t.oid, 'pg_type'),
			pg_catalog.format_type(t.oid, NULL)
		FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		WHERE t.typtype IN ('e', 'c', 'd')
		AND %s
		AND NOT EXISTS (
			SELECT 1 FROM pg_class c
			WHERE c.relnamespace = t.typnamespace
			AND c.relname = t.typname
			AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
		)
		ORDER BY n.nspname, t.typname`

	queryEnumValues = `
		SELECT e.enumlabel
		FROM pg_enum e
		JOIN pg_type t ON e.enumtypid = t.oid
		JOIN pg_namespace n ON t.typnamespace = n.oid
		WHERE n.nspname = $1 AND t.typname = $2
		ORDER BY e.enumsortorder`

	querySequences = `
		SELECT
			n.nspname,
			c.relname,
			'bigint',
			s.seqstart,
			s.seqmin,
			s.seqmax,
			s.seqincrement,
			s.seqcache,
			s.seqcycle,
			d.refobjid::regclass::text,
			a.attname
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_sequence s ON s.seqrelid = c.oid
		LEFT JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
		LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE c.relkind = 'S'
		AND %s
		ORDER BY n.nspname, c.relname`

	querySchemas = `
		SELECT nspname
		FROM pg_namespace
		WHERE %s
		ORDER BY nspname`

	queryHypertables = `
		SELECT
			h.hypertable_schema,
			h.hypertable_name,
			d.column_name,
			d.column_type,
			d.time_interval::text,
			h.num_dimensions
		FROM timescaledb_information.hypertables h
		JOIN timescaledb_information.dimensions d ON h.hypertable_schema = d.hypertable_schema
			AND h.hypertable_name = d.hypertable_name
		WHERE d.dimension_number = 1
		ORDER BY h.hypertable_schema, h.hypertable_name`

	queryCompressionEnabled = `
		SELECT EXISTS (
			SELECT 1
			FROM timescaledb_information.compression_settings
			WHERE hypertable_schema = $1 AND hypertable_name = $2
		)`

	querySpacePartitionColumns = `
		SELECT column_name
		FROM timescaledb_information.dimensions
		WHERE hypertable_schema = $1 AND hypertable_name = $2
		AND dimension_number > 1
		ORDER BY dimension_number`

	queryCompressionSettings = `
		SELECT
			string_agg(attname, ', ' ORDER BY segmentby_column_index) FILTER (WHERE segmentby_column_index IS NOT NULL),
			string_agg(
				attname || ' ' || CASE WHEN orderby_asc THEN 'ASC' ELSE 'DESC' END,
				', '
				ORDER BY orderby_column_index
			) FILTER (WHERE orderby_column_index IS NOT NULL)
		FROM timescaledb_information.compression_settings
		WHERE hypertable_schema = $1 AND hypertable_name = $2
		GROUP BY hypertable_schema, hypertable_name`

	queryRetentionPolicy = `
		SELECT
			config::json->>'drop_after',
			schedule_interval
		FROM timescaledb_information.jobs j
		JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
		WHERE j.proc_name = 'policy_retention'
		AND j.config::json->>'hypertable_schema' = $1
		AND j.config::json->>'hypertable_name' = $2
		LIMIT 1`

	queryContinuousAggregates = `
		SELECT
			ca.view_schema,
			ca.view_name,
			ca.hypertable_schema,
			ca.hypertable_name,
			ca.view_definition,
			ca.materialized_only,
			ca.finalized,
			obj_description(
				(quote_ident(ca.view_schema) || '.' || quote_ident(ca.view_name))::regclass::oid,
				'pg_class'
			),
			ca.materialization_hypertable_schema,
			ca.materialization_hypertable_name
		FROM timescaledb_information.continuous_aggregates ca
		WHERE %s
		ORDER BY ca.view_schema, ca.view_name`

	queryRefreshPolicy = `
		SELECT
			config::json->>'start_offset',
			config::json->>'end_offset',
			schedule_interval::text
		FROM timescaledb_information.jobs j
		CROSS JOIN timescaledb_information.continuous_aggregates ca
		WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
		AND ca.view_schema = $1
		AND ca.view_name = $2
		AND (config::json->>'mat_hypertable_id')::int =
			substring(ca.materialization_hypertable_name from '_materialized_hypertable_([0-9]+)')::int
		LIMIT 1`
)

func (qb *queryBuilder) tablesQuery() string {
	return fmt.Sprintf(queryTables, qb.schemaFilter(), qb.systemTableFilter())
}

func (qb *queryBuilder) viewsQuery() string {
	return fmt.Sprintf(queryViews, qb.schemaFilter(), qb.continuousAggregateFilter())
}

func (qb *queryBuilder) materializedViewsQuery() string {
	return fmt.Sprintf(queryMaterializedViews, qb.namespaceFilter("n.nspname"))
}

func (qb *queryBuilder) functionsQuery() string {
	return fmt.Sprintf(queryFunctions, qb.namespaceFilter("n.nspname"))
}

func (qb *queryBuilder) triggersQuery() string {
	return fmt.Sprintf(queryTriggers, qb.namespaceFilter("n.nspname"))
}

func (qb *queryBuilder) extensionsQuery() string {
	return fmt.Sprintf(queryExtensions, qb.buildInClause(qb.excludeExtensions))
}

func (qb *queryBuilder) customTypesQuery() string {
	return fmt.Sprintf(queryCustomTypes, qb.namespaceFilter("n.nspname"))
}

func (qb *queryBuilder) sequencesQuery() string {
	return fmt.Sprintf(querySequences, qb.namespaceFilter("n.nspname"))
}

func (qb *queryBuilder) schemasQuery() string {
	return fmt.Sprintf(querySchemas, qb.namespaceFilter("nspname"))
}

func (qb *queryBuilder) continuousAggregatesQuery() string {
	return fmt.Sprintf(queryContinuousAggregates, qb.namespaceFilter("ca.view_schema"))
}
