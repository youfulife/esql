package sp

import "fmt"

// GroovyWrapped ...
func (tok Token) GroovyWrapped() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		switch tok {
		case AND:
			return "&&"
		case OR:
			return "||"
		case EQ:
			return "=="
		default:
			return tokens[tok]
		}
	}
	return ""
}

// GroovyWrapped ...
func (r *VarRef) GroovyWrapped() string {
	return fmt.Sprintf("doc['%s'].value", qsReplacer.Replace(r.Val))
}

//RewriteConditions ...
func (s *SelectStatement) RewriteConditions() {

	// Rewrite all variable references in the fields with their types if one
	// hasn't been specified.
	rewrite := func(n Node) {
		switch expr := n.(type) {
		case *VarRef:
			expr.Val = expr.GroovyWrapped()
		case *BinaryExpr:
			tokens[expr.Op] = expr.Op.GroovyWrapped()
		}
		return
	}
	WalkFunc(s.Condition, rewrite)
}

//RewriteMetricArgs ...
func (c *Call) RewriteMetricArgs() {

	// Rewrite all variable references in the fields with their types if one
	// hasn't been specified.
	rewrite := func(n Node) {
		switch expr := n.(type) {
		case *VarRef:
			expr.Val = expr.GroovyWrapped()
		}
		return
	}
	for _, arg := range c.Args {
		WalkFunc(arg, rewrite)
	}
}

//RewriteDimensions ...
func (s *SelectStatement) RewriteDimensions() {

	// Rewrite all variable references
	rewrite := func(n Node) {
		switch expr := n.(type) {
		case *VarRef:
			expr.Val = expr.GroovyWrapped()
		}
		return
	}
	for _, d := range s.Dimensions {
		switch expr := d.Expr.(type) {
		case *BinaryExpr:
			WalkFunc(d, rewrite)
		case *Call:
			if _, ok := expr.Args[0].(*BinaryExpr); ok {
				WalkFunc(d, rewrite)
			}
		default:
		}
	}
}

//RewriteHaving ...
func (s *SelectStatement) RewriteHaving() {
	// Rewrite all variable references in the fields with their types if one
	// hasn't been specified.
	rewrite := func(n Node) {
		switch expr := n.(type) {
		case *BinaryExpr:
			tokens[expr.Op] = expr.Op.GroovyWrapped()
		}
		return
	}
	WalkFunc(s.Having, rewrite)
}
