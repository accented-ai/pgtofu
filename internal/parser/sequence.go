package parser

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

type SequenceParser struct{}

func NewSequenceParser() *SequenceParser {
	return &SequenceParser{}
}

func (p *SequenceParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateSequence}
}

func (p *SequenceParser) Parse(_ *Parser, stmt Statement, db *schema.Database) error {
	statementParseMu.Lock()
	defer statementParseMu.Unlock()

	tree, err := wasmquery.Parse(stmt.NormalizedSQL())
	if err != nil {
		return util.WrapError("parse CREATE SEQUENCE", err)
	}

	if len(tree.GetStmts()) != 1 {
		return errors.New("expected one CREATE SEQUENCE statement")
	}

	statement := tree.GetStmts()[0].GetStmt().GetCreateSeqStmt()
	if statement == nil {
		return errors.New("expected CREATE SEQUENCE")
	}

	options, err := sequenceOptions(statement.GetOptions())
	if err != nil {
		return err
	}

	relation := statement.GetSequence()

	sequence := schema.Sequence{
		Schema: schema.TruncateIdentifier(relation.GetSchemaname()),
		Name:   schema.TruncateIdentifier(relation.GetRelname()),
	}
	if sequence.Schema == "" {
		sequence.Schema = schema.DefaultSchema
	}

	if err := populateSequence(&sequence, options); err != nil {
		return err
	}

	db.Sequences = append(db.Sequences, sequence)

	return nil
}

func sequenceOptions(nodes []*pgquery.Node) (map[string]*pgquery.Node, error) {
	options := make(map[string]*pgquery.Node, len(nodes))
	for _, node := range nodes {
		option := node.GetDefElem()

		name := option.GetDefname()
		if _, exists := options[name]; exists {
			return nil, fmt.Errorf("duplicate sequence option %s", name)
		}

		switch name {
		case "as", "increment", "start", "minvalue", "maxvalue", "cache", "cycle":
			options[name] = option.GetArg()
		default:
			return nil, fmt.Errorf("unsupported sequence option %s", name)
		}
	}

	return options, nil
}

func populateSequence(sequence *schema.Sequence, options map[string]*pgquery.Node) error {
	dataType, typeMin, typeMax, err := sequenceDataType(options["as"])
	if err != nil {
		return err
	}

	sequence.DataType = dataType

	sequence.Increment, err = sequenceInteger(options["increment"], 1)
	if err != nil {
		return err
	}

	sequence.MinValue, sequence.MaxValue = 1, typeMax
	if sequence.Increment < 0 {
		sequence.MinValue, sequence.MaxValue = typeMin, -1
	}

	for _, option := range []struct {
		name  string
		value *int64
	}{
		{"minvalue", &sequence.MinValue},
		{"maxvalue", &sequence.MaxValue},
	} {
		*option.value, err = sequenceInteger(options[option.name], *option.value)
		if err != nil {
			return err
		}
	}

	sequence.StartValue = sequence.MinValue
	if sequence.Increment < 0 {
		sequence.StartValue = sequence.MaxValue
	}

	sequence.StartValue, err = sequenceInteger(options["start"], sequence.StartValue)
	if err != nil {
		return err
	}

	sequence.CacheSize, err = sequenceInteger(options["cache"], 1)
	if err != nil {
		return err
	}

	sequence.IsCyclic = options["cycle"].GetBoolean().GetBoolval()
	if sequence.Increment == 0 || sequence.CacheSize <= 0 {
		return errors.New("sequence requires a nonzero increment and positive cache")
	}

	if sequence.MinValue < typeMin || sequence.MaxValue > typeMax ||
		sequence.MinValue >= sequence.MaxValue {
		return errors.New(
			"sequence bounds must fit its data type and minvalue must be less than maxvalue",
		)
	}

	if sequence.StartValue < sequence.MinValue || sequence.StartValue > sequence.MaxValue {
		return errors.New("sequence start must be within its bounds")
	}

	return nil
}

func sequenceDataType(node *pgquery.Node) (string, int64, int64, error) {
	if node == nil {
		return "bigint", math.MinInt64, math.MaxInt64, nil
	}

	names := node.GetTypeName().GetNames()
	if len(names) == 0 {
		return "", 0, 0, errors.New("sequence requires an integer data type")
	}

	switch name := names[len(names)-1].GetString_().GetSval(); name {
	case "int2":
		return "smallint", math.MinInt16, math.MaxInt16, nil
	case "int4":
		return "integer", math.MinInt32, math.MaxInt32, nil
	case "int8":
		return "bigint", math.MinInt64, math.MaxInt64, nil
	default:
		return "", 0, 0, fmt.Errorf("unsupported sequence data type %s", name)
	}
}

func sequenceInteger(node *pgquery.Node, fallback int64) (int64, error) {
	if node == nil {
		return fallback, nil
	}

	if integer := node.GetInteger(); integer != nil {
		return int64(integer.GetIval()), nil
	}

	if number := node.GetFloat(); number != nil {
		value, err := strconv.ParseInt(number.GetFval(), 10, 64)
		if err != nil {
			return 0, util.WrapError("parse sequence integer", err)
		}

		return value, nil
	}

	return 0, errors.New("sequence option requires an integer")
}
