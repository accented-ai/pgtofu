package extractor

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func (e *Extractor) extractFunctions(ctx context.Context) ([]schema.Function, error) {
	query := e.queries.functionsQuery()

	var functions []schema.Function

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var (
			fn        schema.Function
			arguments *string
		)

		if err := rows.Scan(
			&fn.Schema,
			&fn.Name,
			&arguments,
			scanner.String("returnType"),
			&fn.Language,
			&fn.Body,
			&fn.IsAggregate,
			&fn.IsWindow,
			&fn.IsStrict,
			&fn.IsSecurityDefiner,
			&fn.Volatility,
			scanner.String("comment"),
			scanner.String("owner"),
			&fn.Definition,
		); err != nil {
			return util.WrapError("scan function", err)
		}

		fn.ReturnType = scanner.GetString("returnType")
		fn.Comment = scanner.GetString("comment")
		fn.Owner = scanner.GetString("owner")

		if arguments != nil && *arguments != "" {
			fn.ArgumentTypes, fn.ArgumentNames, fn.ArgumentModes = parseFunctionArguments(
				*arguments,
			)
		}

		functions = append(functions, fn)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch functions", err)
	}

	return functions, nil
}

func (e *Extractor) extractTriggers(ctx context.Context) ([]schema.Trigger, error) {
	query := e.queries.triggersQuery()

	var triggers []schema.Trigger

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var trig schema.Trigger

		if err := rows.Scan(
			&trig.Schema,
			&trig.Name,
			&trig.TableName,
			&trig.Timing,
			&trig.Events,
			&trig.ForEachRow,
			&trig.Definition,
			&trig.FunctionSchema,
			&trig.FunctionName,
			scanner.String("comment"),
		); err != nil {
			return util.WrapError("scan trigger", err)
		}

		trig.Comment = scanner.GetString("comment")
		triggers = append(triggers, trig)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch triggers", err)
	}

	return triggers, nil
}

func parseFunctionArguments(arguments string) ([]string, []string, []string) {
	if arguments == "" {
		return nil, nil, nil
	}

	args := splitArguments(arguments)
	argTypes := make([]string, 0, len(args))
	argNames := make([]string, 0, len(args))
	argModes := make([]string, 0, len(args))

	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue
		}

		mode := "IN"

		for _, m := range []string{"IN OUT", "INOUT", "OUT", "IN", "VARIADIC"} {
			if strings.HasPrefix(strings.ToUpper(arg), m+" ") {
				mode = m
				arg = strings.TrimSpace(arg[len(m):])

				break
			}
		}

		if idx := strings.Index(strings.ToUpper(arg), " DEFAULT "); idx != -1 {
			arg = arg[:idx]
		}

		parts := strings.Fields(arg)

		var name, argType string

		if len(parts) >= 2 {
			name = strings.Join(parts[:len(parts)-1], " ")
			argType = parts[len(parts)-1]
		} else if len(parts) == 1 {
			argType = parts[0]
		}

		argTypes = append(argTypes, argType)
		argNames = append(argNames, name)
		argModes = append(argModes, mode)
	}

	return argTypes, argNames, argModes
}

func splitArguments(arguments string) []string {
	var (
		args    []string
		current strings.Builder
	)

	depth := 0
	inString := false

	for i := range len(arguments) {
		ch := arguments[i]

		switch ch {
		case '\'':
			inString = !inString

			current.WriteByte(ch)
		case '(':
			if !inString {
				depth++
			}

			current.WriteByte(ch)
		case ')':
			if !inString {
				depth--
			}

			current.WriteByte(ch)
		case ',':
			if !inString && depth == 0 {
				args = append(args, current.String())
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}
