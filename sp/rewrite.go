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

// Rewriter can be called by Rewrite to replace nodes in the AST hierarchy.
// The Rewrite() function is called once per node.
type Rewriter interface {
	Rewrite(Node) Node
}

// Rewrite recursively invokes the rewriter to replace each node.
// Nodes are traversed depth-first and rewritten from leaf to root.
func Rewrite(r Rewriter, node Node) Node {
	switch n := node.(type) {
	case Statements:
		for i, s := range n {
			n[i] = Rewrite(r, s).(Statement)
		}

	case *SelectStatement:
		n.Fields = Rewrite(r, n.Fields).(Fields)
		n.Dimensions = Rewrite(r, n.Dimensions).(Dimensions)
		n.Sources = Rewrite(r, n.Sources).(Sources)
		n.Condition = Rewrite(r, n.Condition).(Expr)

	case Fields:
		for i, f := range n {
			n[i] = Rewrite(r, f).(*Field)
		}

	case *Field:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case Dimensions:
		for i, d := range n {
			n[i] = Rewrite(r, d).(*Dimension)
		}

	case *Dimension:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *BinaryExpr:
		n.LHS = Rewrite(r, n.LHS).(Expr)
		n.RHS = Rewrite(r, n.RHS).(Expr)

	case *ParenExpr:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *Call:
		for i, expr := range n.Args {
			n.Args[i] = Rewrite(r, expr).(Expr)
		}
	}

	return r.Rewrite(node)
}

// RewriteFunc rewrites a node hierarchy.
func RewriteFunc(node Node, fn func(Node) Node) Node {
	return Rewrite(rewriterFunc(fn), node)
}

type rewriterFunc func(Node) Node

func (fn rewriterFunc) Rewrite(n Node) Node { return fn(n) }

// RewriteExpr recursively invokes the function to replace each expr.
// Nodes are traversed depth-first and rewritten from leaf to root.
func RewriteExpr(expr Expr, fn func(Expr) Expr) Expr {
	switch e := expr.(type) {
	case *BinaryExpr:
		e.LHS = RewriteExpr(e.LHS, fn)
		e.RHS = RewriteExpr(e.RHS, fn)
		if e.LHS != nil && e.RHS == nil {
			expr = e.LHS
		} else if e.RHS != nil && e.LHS == nil {
			expr = e.RHS
		} else if e.LHS == nil && e.RHS == nil {
			return nil
		}

	case *ParenExpr:
		e.Expr = RewriteExpr(e.Expr, fn)
		if e.Expr == nil {
			return nil
		}

	case *Call:
		for i, expr := range e.Args {
			e.Args[i] = RewriteExpr(expr, fn)
		}
	}

	return fn(expr)
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
		// only rewrite BinaryExpr, not support ParenExpr
		_, ok := d.Expr.(*BinaryExpr)
		if ok {
			WalkFunc(d, rewrite)
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
