package sp

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Parser represents an InfluxQL parser.
type Parser struct {
	s *bufScanner
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: newBufScanner(r)}
}

// ParseStatement parses a statement string and returns its AST representation.
func ParseStatement(s string) (Statement, error) {
	return NewParser(strings.NewReader(s)).ParseStatement()
}

// ParseStatement parses an InfluxQL string and returns a Statement AST object.
func (p *Parser) ParseStatement() (Statement, error) {
	// Inspect the first token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case SELECT:
		return p.parseSelectStatement()
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"SELECT"}, pos)
	}
}

// parseInt parses a string and returns an integer literal.
func (p *Parser) parseInt(min, max int) (int, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != INTEGER {
		return 0, newParseError(tokstr(tok, lit), []string{"integer"}, pos)
	}

	// Convert string to int.
	n, err := strconv.Atoi(lit)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	} else if min > n || n > max {
		return 0, &ParseError{
			Message: fmt.Sprintf("invalid value %d: must be %d <= n <= %d", n, min, max),
			Pos:     pos,
		}
	}

	return n, nil
}

// parseUInt32 parses a string and returns a 32-bit unsigned integer literal.
func (p *Parser) parseUInt32() (uint32, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != INTEGER {
		return 0, newParseError(tokstr(tok, lit), []string{"integer"}, pos)
	}

	// Convert string to unsigned 32-bit integer
	n, err := strconv.ParseUint(lit, 10, 32)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint32(n), nil
}

// parseUInt64 parses a string and returns a 64-bit unsigned integer literal.
func (p *Parser) parseUInt64() (uint64, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != INTEGER {
		return 0, newParseError(tokstr(tok, lit), []string{"integer"}, pos)
	}

	// Convert string to unsigned 64-bit integer
	n, err := strconv.ParseUint(lit, 10, 64)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint64(n), nil
}

// parseIdent parses an identifier.
func (p *Parser) parseIdent() (string, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT {
		return "", newParseError(tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// parseIdentList parses a comma delimited list of identifiers.
func (p *Parser) parseIdentList() ([]string, error) {
	// Parse first (required) identifier.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			return idents, nil
		}

		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}
}

// parseSegmentedIdents parses a segmented identifiers.
// e.g.,  tcp.in_bytes
func (p *Parser) parseSegmentedIdents() ([]string, error) {
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.scan(); tok != DOT {
			// No more segments so we're done.
			p.unscan()
			break
		}
		// Parse the next identifier.
		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}

	return idents, nil
}

// parserString parses a string.
func (p *Parser) parseString() (string, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != STRING {
		return "", newParseError(tokstr(tok, lit), []string{"string"}, pos)
	}
	return lit, nil
}

// parserString parses a string.
func (p *Parser) parseStringList() ([]string, error) {
	// Parse first (required) string.
	str, err := p.parseString()
	if err != nil {
		return nil, err
	}
	strs := []string{str}

	// Parse remaining (optional) strings.
	for {
		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			return strs, nil
		}

		if str, err = p.parseString(); err != nil {
			return nil, err
		}

		strs = append(strs, str)
	}
}

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*SelectStatement, error) {
	stmt := &SelectStatement{}
	var err error

	// Parse fields: "FIELD+".
	if stmt.Fields, err = p.parseFields(); err != nil {
		return nil, err
	}

	// Parse source: "FROM".
	if stmt.Sources, err = p.parseSources(); err != nil {
		return nil, err
	}

	// Parse condition: "WHERE EXPR".
	if stmt.Condition, err = p.parseCondition(); err != nil {
		return nil, err
	}

	// Parse dimensions: "GROUP BY DIMENSION+".
	if stmt.Dimensions, err = p.parseDimensions(); err != nil {
		return nil, err
	}

	// Parse having: "HAVING EXPR".
	if stmt.Having, err = p.parseHaving(); err != nil {
		return nil, err
	}

	// Parse sort: "ORDER BY FIELD+".
	if stmt.SortFields, err = p.parseOrderBy(); err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT <m>,<n>".
	if stmt.Limit, stmt.Offset, err = p.parseLimit(); err != nil {
		return nil, err
	}

	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != EOF {
		return nil, newParseError(tokstr(tok, lit), []string{"EOF"}, pos)
	}

	// Set if the query is a raw data query or one with an aggregate
	stmt.IsRawQuery = true
	WalkFunc(stmt.Fields, func(n Node) {
		if _, ok := n.(*Call); ok {
			stmt.IsRawQuery = false
		}
	})

	if err := stmt.validate(); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseFields parses a list of one or more fields.
func (p *Parser) parseFields() (Fields, error) {
	var fields Fields

	for {
		// Parse the field.
		f, err := p.parseField()
		if err != nil {
			return nil, err
		}

		// Add new field.
		fields = append(fields, f)

		// If there's not a comma next then stop parsing fields.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}
	return fields, nil
}

// parseField parses a single field.
func (p *Parser) parseField() (*Field, error) {
	f := &Field{}
	p.scanIgnoreWhitespace()
	p.unscan()
	// field must expr.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	f.Expr = expr

	// Parse the alias if the current and next tokens are "WS AS".
	alias, err := p.parseAlias()
	if err != nil {
		return nil, err
	}
	f.Alias = alias

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return f, nil
}

// validateField checks if the Expr is a valid field. We disallow all binary expression
// that return a boolean
type validateField struct {
	foundInvalid bool
	badToken     Token
}

func (c *validateField) Visit(n Node) Visitor {
	e, ok := n.(*BinaryExpr)
	if !ok {
		return c
	}

	switch e.Op {
	case EQ, NEQ, EQREGEX, NEQREGEX, LT, LTE, GT, GTE, AND, OR, IN, NI:
		c.foundInvalid = true
		c.badToken = e.Op
		return nil
	}
	return c
}

// parseAlias parses the "AS IDENT" alias for fields and dimensions.
func (p *Parser) parseAlias() (string, error) {
	// Check if the next token is "AS". If not, then unscan and exit.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != AS {
		p.unscan()
		return "", nil
	}

	// Then we should have the alias identifier.
	lit, err := p.parseIdent()
	if err != nil {
		return "", err
	}
	return lit, nil
}

// parseSources parses a comma delimited list of sources.
func (p *Parser) parseSources() (Sources, error) {
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != FROM {
		return nil, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}
	var sources Sources

	for {
		s, err := p.parseSource()
		if err != nil {
			return nil, err
		}
		sources = append(sources, s)

		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			break
		}
	}

	return sources, nil
}

// peekRune returns the next rune that would be read by the scanner.
func (p *Parser) peekRune() rune {
	r, _, _ := p.s.s.r.ReadRune()
	if r != eof {
		_ = p.s.s.r.UnreadRune()
	}

	return r
}

func (p *Parser) parseSource() (Source, error) {
	m := &Measurement{}

	// Didn't find a regex so parse segmented identifiers.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	m.Database = ident
	return m, nil
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition() (Expr, error) {
	// Check if the WHERE token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != WHERE {
		p.unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parseDimensions parses the "GROUP BY" clause of the query, if it exists.
func (p *Parser) parseDimensions() (Dimensions, error) {
	// If the next token is not GROUP then exit.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != GROUP {
		p.unscan()
		return nil, nil
	}

	// Now the next token should be "BY".
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	var dimensions Dimensions
	for {
		// Parse the dimension.
		d, err := p.parseDimension()
		if err != nil {
			return nil, err
		}

		// Add new dimension.
		dimensions = append(dimensions, d)

		// If there's not a comma next then stop parsing dimensions.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}
	return dimensions, nil
}

// parseDimension parses a single dimension.
func (p *Parser) parseDimension() (*Dimension, error) {
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		return &Dimension{Expr: re}, nil
	}

	// Parse the expression first.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// Parse the alias if the current and next tokens are "WS AS".
	alias, err := p.parseAlias()
	if err != nil {
		return nil, err
	}

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return &Dimension{Expr: expr, Alias: alias}, nil
}

// parseHaving parses the "HAVING" clause of the query, if it exists.
func (p *Parser) parseHaving() (Expr, error) {
	// Check if the WHERE token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != HAVING {
		p.unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parseLimit parses the specified token followed
// by an int, if it exists.
func (p *Parser) parseLimit() (int, int, error) {
	// Check if the token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != LIMIT {
		p.unscan()
		return 0, 0, nil
	}

	// Scan the number.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != INTEGER {
		return 0, 0, newParseError(tokstr(tok, lit), []string{"integer"}, pos)
	}

	// Parse number.
	n, _ := strconv.ParseInt(lit, 10, 64)
	if n < 0 {
		msg := fmt.Sprintf("%s must be >= 0", LIMIT.String())
		return 0, 0, &ParseError{Message: msg, Pos: pos}
	}

	// Parse offset
	if _tok, _, _ := p.scanIgnoreWhitespace(); _tok != COMMA {
		p.unscan()
		return int(n), 0, nil
	}

	// Scan the number.
	tok, pos, lit = p.scanIgnoreWhitespace()
	if tok != INTEGER {
		return 0, 0, newParseError(tokstr(tok, lit), []string{"integer"}, pos)
	}

	// Parse number.
	m, _ := strconv.ParseInt(lit, 10, 64)
	if m < 0 {
		msg := fmt.Sprintf("offset must be >= 0")
		return 0, 0, &ParseError{Message: msg, Pos: pos}
	}

	return int(n), int(m), nil
}

// parseOrderBy parses the "ORDER BY" clause of a query, if it exists.
func (p *Parser) parseOrderBy() (SortFields, error) {
	// Return nil result and nil error if no ORDER token at this position.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != ORDER {
		p.unscan()
		return nil, nil
	}

	// Parse the required BY token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	// Parse the ORDER BY fields.
	fields, err := p.parseSortFields()
	if err != nil {
		return nil, err
	}

	return fields, nil
}

// parseSortFields parses the sort fields for an ORDER BY clause.
func (p *Parser) parseSortFields() (SortFields, error) {
	var fields SortFields

	tok, pos, lit := p.scanIgnoreWhitespace()

	switch tok {
	// The first field after an order by may not have a field name (e.g. ORDER BY ASC)
	case ASC, DESC:
		fields = append(fields, &SortField{Ascending: (tok == ASC)})
	// If it's a token, parse it as a sort field.  At least one is required.
	case IDENT:
		p.unscan()
		field, err := p.parseSortField()
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)
	// Parse error...
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "ASC", "DESC"}, pos)
	}

	// Parse additional fields.
	for {
		tok, _, _ := p.scanIgnoreWhitespace()

		if tok != COMMA {
			p.unscan()
			break
		}

		field, err := p.parseSortField()
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)
	}

	return fields, nil
}

// parseSortField parses one field of an ORDER BY clause.
func (p *Parser) parseSortField() (*SortField, error) {
	field := &SortField{}

	// Parse sort field name.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	field.Name = ident

	// Check for optional ASC or DESC clause. Default is ASC.
	tok, _, _ := p.scanIgnoreWhitespace()
	if tok != ASC && tok != DESC {
		p.unscan()
		tok = ASC
	}
	field.Ascending = (tok == ASC)

	return field, nil
}

// parseVarRef parses a reference to a measurement or field.
func (p *Parser) parseVarRef() (*VarRef, error) {
	// Parse the segments of the variable ref.
	segments, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}
	vr := &VarRef{Val: strings.Join(segments, "."), Segments: segments}
	return vr, nil
}

func (p *Parser) parseList() (*ListLiteral, error) {
	list := &ListLiteral{}

	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != LBRACKET {
		p.unscan()
		return nil, newParseError(tokstr(tok, lit), []string{"["}, pos)
	}

	for {
		// Read next token.
		tok, pos, lit := p.scanIgnoreWhitespace()
		switch tok {
		case STRING:
			list.Vals = append(list.Vals, lit)
		case NUMBER:
			v, err := strconv.ParseFloat(lit, 64)
			if err != nil {
				return nil, &ParseError{Message: "unable to parse number", Pos: pos}
			}
			list.Vals = append(list.Vals, v)
		case INTEGER:
			v, err := strconv.ParseInt(lit, 10, 64)
			if err != nil {
				return nil, &ParseError{Message: "unable to parse integer", Pos: pos}
			}
			list.Vals = append(list.Vals, v)
		default:
			p.unscan()
			return nil, newParseError(tokstr(tok, lit), []string{"string", "float", "integer"}, pos)
		}

		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			break
		}
	}

	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != RBRACKET {
		p.unscan()
		return nil, newParseError(tokstr(tok, lit), []string{"]"}, pos)
	}
	return list, nil
}

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (Expr, error) {
	var err error
	// Dummy root node.
	root := &BinaryExpr{}

	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	root.RHS, err = p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		op, _, _ := p.scanIgnoreWhitespace()
		if !op.isOperator() {
			p.unscan()
			return root.RHS, nil
		}

		// Otherwise parse the next expression.
		var rhs Expr
		if IsRegexOp(op) {
			// RHS of a regex operator must be a regular expression.
			p.consumeWhitespace()
			if rhs, err = p.parseRegex(); err != nil {
				return nil, err
			}
			// parseRegex can return an empty type, but we need it to be present
			if rhs.(*RegexLiteral) == nil {
				tok, pos, lit := p.scanIgnoreWhitespace()
				return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
			}
		} else if IsListOp(op) {
			p.consumeWhitespace()
			if rhs, err = p.parseList(); err != nil {
				return nil, err
			}
		} else {
			if rhs, err = p.parseUnaryExpr(); err != nil {
				return nil, err
			}
		}

		// Find the right spot in the tree to add the new expression by
		// descending the RHS of the expression tree until we reach the last
		// BinaryExpr or a BinaryExpr whose RHS has an operator with
		// precedence >= the operator being added.
		for node := root; ; {
			r, ok := node.RHS.(*BinaryExpr)
			if !ok || r.Op.Precedence() >= op.Precedence() {
				// Add the new expression here and break.
				node.RHS = &BinaryExpr{LHS: node.RHS, RHS: rhs, Op: op}
				break
			}
			node = r
		}
	}
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (Expr, error) {
	// If the first token is a LPAREN then parse it as its own grouped expression.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == LPAREN {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		// Expect an RPAREN at the end.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok != RPAREN {
			return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
		}

		return &ParenExpr{Expr: expr}, nil
	}
	p.unscan()

	// Read next token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case IDENT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a variable reference.
		if tok0, _, _ := p.scan(); tok0 == LPAREN {
			return p.parseCall(lit)
		}

		p.unscan() // unscan the last token (wasn't an LPAREN)
		p.unscan() // unscan the IDENT token

		// Parse it as a VarRef.
		return p.parseVarRef()
	case STRING:
		return &StringLiteral{Val: lit}, nil
	case NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return &NumberLiteral{Val: v}, nil
	case INTEGER:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse integer", Pos: pos}
		}
		return &IntegerLiteral{Val: v}, nil
	case TRUE, FALSE:
		return &BooleanLiteral{Val: (tok == TRUE)}, nil
	case MUL:
		wc := &Wildcard{}
		return wc, nil
	case REGEX:
		re, err := regexp.Compile(lit)
		if err != nil {
			return nil, &ParseError{Message: err.Error(), Pos: pos}
		}
		return &RegexLiteral{Val: re}, nil
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// parseRegex parses a regular expression.
func (p *Parser) parseRegex() (*RegexLiteral, error) {
	nextRune := p.peekRune()
	if isWhitespace(nextRune) {
		p.consumeWhitespace()
	}

	// If the next character is not a '/', then return nils.
	nextRune = p.peekRune()
	if nextRune != '/' {
		return nil, nil
	}

	tok, pos, lit := p.s.ScanRegex()

	if tok == BADESCAPE {
		msg := fmt.Sprintf("bad escape: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok == BADREGEX {
		msg := fmt.Sprintf("bad regex: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok != REGEX {
		return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
	}

	re, err := regexp.Compile(lit)
	if err != nil {
		return nil, &ParseError{Message: err.Error(), Pos: pos}
	}

	return &RegexLiteral{Val: re}, nil
}

// parseCall parses a function call.
// This function assumes the function name and LPAREN have been consumed.
func (p *Parser) parseCall(name string) (*Call, error) {
	name = strings.ToLower(name)

	// Parse first function argument if one exists.
	var args []Expr
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		args = append(args, re)
	} else {
		// If there's a right paren then just return immediately.
		if tok, _, _ := p.scan(); tok == RPAREN {
			return &Call{Name: name}, nil
		}
		p.unscan()

		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	// Parse additional function arguments if there is a comma.
	for {
		// If there's not a comma, stop parsing arguments.
		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			break
		}

		re, err := p.parseRegex()
		if err != nil {
			return nil, err
		} else if re != nil {
			args = append(args, re)
			continue
		}

		// Parse an expression argument.
		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	// There should be a right parentheses at the end.
	if tok, pos, lit := p.scan(); tok != RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return &Call{Name: name, Args: args}, nil
}

// scan returns the next token from the underlying scanner.
func (p *Parser) scan() (tok Token, pos Pos, lit string) { return p.s.Scan() }

// scanIgnoreWhitespace scans the next non-whitespace token.
func (p *Parser) scanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	tok, pos, lit = p.scan()
	if tok == WS {
		tok, pos, lit = p.scan()
	}
	return
}

// consumeWhitespace scans the next token if it's whitespace.
func (p *Parser) consumeWhitespace() {
	if tok, _, _ := p.scan(); tok != WS {
		p.unscan()
	}
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() { p.s.Unscan() }

var (
	qsReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `'`, `\'`)
	qiReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
)

// QuoteString returns a quoted string.
func QuoteString(s string) string {
	return `'` + qsReplacer.Replace(s) + `'`
}

// QuoteIdent returns a quoted identifier from multiple bare identifiers.
func QuoteIdent(segments ...string) string {
	var buf bytes.Buffer
	for i, segment := range segments {
		needQuote := IdentNeedsQuotes(segment) ||
			((i < len(segments)-1) && segment != "") || // not last segment && not ""
			((i == 0 || i == len(segments)-1) && segment == "") // the first or last segment and an empty string

		if needQuote {
			_ = buf.WriteByte('"')
		}

		_, _ = buf.WriteString(qiReplacer.Replace(segment))

		if needQuote {
			_ = buf.WriteByte('"')
		}

		if i < len(segments)-1 {
			_ = buf.WriteByte('.')
		}
	}
	return buf.String()
}

// IdentNeedsQuotes returns true if the ident string given would require quotes.
func IdentNeedsQuotes(ident string) bool {
	// check if this identifier is a keyword
	tok := Lookup(ident)
	if tok != IDENT {
		return true
	}
	for i, r := range ident {
		if i == 0 && !isIdentFirstChar(r) {
			return true
		} else if i > 0 && !isIdentChar(r) {
			return true
		}
	}
	return false
}

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
