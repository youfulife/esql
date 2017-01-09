package sp

import (
	"bytes"
	"fmt"
)

// Groovy returns the string representation of the token.
func (tok Token) Groovy() string {
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

// Groovy returns a string representation of the variable reference.
func (r *VarRef) Groovy() string {
	buf := bytes.NewBufferString(ScriptIdent(r.Val))
	return buf.String()
}

// ScriptIdent returns a quoted string.
func ScriptIdent(s string) string {
	return fmt.Sprintf("doc['%s'].value", qsReplacer.Replace(s))
}
