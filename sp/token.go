package sp

import (
	"strings"
)

// Token is a lexical token of the InfluxQL language.
type Token int

// These are a comprehensive list of InfluxQL language tokens.
const (
	// ILLEGAL Token, EOF, WS are Special InfluxQL tokens.
	ILLEGAL Token = iota
	EOF
	WS

	literalBeg
	// IDENT and the following are InfluxQL literal tokens.
	IDENT     // main
	NUMBER    // 12345.67
	INTEGER   // 12345
	STRING    // "abc"
	BADSTRING // "abc
	BADESCAPE // \q
	TRUE      // true
	FALSE     // false
	REGEX     // Regular expressions
	BADREGEX  // `.*
	literalEnd

	operatorBeg
	// ADD and the following are InfluxQL Operators
	ADD // +
	SUB // -
	MUL // *
	DIV // /
	MOD // %

	AND // AND
	OR  // OR
	NI  // not in
	IN

	EQ       // =
	NEQ      // !=
	EQREGEX  // =~
	NEQREGEX // !~
	LT       // <
	LTE      // <=
	GT       // >
	GTE      // >=
	operatorEnd

	LBRACKET // [
	LPAREN   // (
	RBRACKET // ]
	RPAREN   // )
	COMMA    // ,
	DOT      // .

	keywordBeg
	// ALL and the following are InfluxQL Keywords
	AS
	ASC
	BY
	DESC
	FROM
	GROUP
	HAVING
	LIMIT
	ORDER
	SELECT
	WHERE
	keywordEnd
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:     "IDENT",
	NUMBER:    "NUMBER",
	INTEGER:   "INTEGER",
	STRING:    "STRING",
	BADSTRING: "BADSTRING",
	BADESCAPE: "BADESCAPE",
	TRUE:      "TRUE",
	FALSE:     "FALSE",
	REGEX:     "REGEX",

	ADD: "+",
	SUB: "-",
	MUL: "*",
	DIV: "/",
	MOD: "%",

	AND: "AND",
	OR:  "OR",
	NI:  "NI",
	IN:  "IN",

	EQ:       "=",
	NEQ:      "!=",
	EQREGEX:  "=~",
	NEQREGEX: "!~",
	LT:       "<",
	LTE:      "<=",
	GT:       ">",
	GTE:      ">=",

	LBRACKET: "[",
	LPAREN:   "(",
	RBRACKET: "]",
	RPAREN:   ")",
	COMMA:    ",",
	DOT:      ".",

	AS:     "AS",
	ASC:    "ASC",
	BY:     "BY",
	DESC:   "DESC",
	FROM:   "FROM",
	GROUP:  "GROUP",
	HAVING: "HAVING",
	LIMIT:  "LIMIT",
	ORDER:  "ORDER",
	SELECT: "SELECT",
	WHERE:  "WHERE",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR, IN, NI} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	keywords["true"] = TRUE
	keywords["false"] = FALSE
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		switch tok {
		default:
			return tokens[tok]
		}
	}
	return ""
}

// Precedence returns the operator precedence of the binary operator token.
func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case IN, NI:
		return 3
	case EQ, NEQ, EQREGEX, NEQREGEX, LT, LTE, GT, GTE:
		return 4
	case ADD, SUB:
		return 5
	case MUL, DIV, MOD:
		return 6
	}
	return 0
}

// isOperator returns true for operator tokens.
func (tok Token) isOperator() bool { return tok > operatorBeg && tok < operatorEnd }

// tokstr returns a literal if provided, otherwise returns the token string.
func tokstr(tok Token, lit string) string {
	if lit != "" {
		return lit
	}
	return tok.String()
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}

// Pos specifies the line and character position of a token.
// The Char and Line are both zero-based indexes.
type Pos struct {
	Line int
	Char int
}
