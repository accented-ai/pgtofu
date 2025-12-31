package parser

import (
	"errors"
	"fmt"
)

type ParseError struct {
	File    string
	Line    int
	Column  int
	Message string
	SQL     string
	Cause   error
}

func (e ParseError) Error() string {
	location := ""

	switch {
	case e.File != "" && e.Line > 0:
		location = fmt.Sprintf("%s:%d", e.File, e.Line)
	case e.File != "":
		location = e.File
	case e.Line > 0:
		location = fmt.Sprintf("line %d", e.Line)
	}

	if location != "" {
		return fmt.Sprintf("%s: %s", location, e.Message)
	}

	return e.Message
}

func (e ParseError) Unwrap() error {
	return e.Cause
}

func NewParseError(message string) ParseError {
	return ParseError{Message: message}
}

func WrapParseError(err error, message string) ParseError {
	return ParseError{
		Message: fmt.Sprintf("%s: %v", message, err),
		Cause:   err,
	}
}

var (
	ErrInvalidSyntax     = errors.New("invalid syntax")
	ErrUnexpectedToken   = errors.New("unexpected token")
	ErrUnknownStatement  = errors.New("unknown statement type")
	ErrMissingIdentifier = errors.New("missing identifier")
)
