package sp_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/chenyoufu/esql/sp"
)

// Ensure the parser can parse strings into Statement ASTs.
func TestParser_ParseStatement(t *testing.T) {
	// For use in various tests.
	now := time.Now()

	var tests = []struct {
		skip   bool
		s      string
		params map[string]interface{}
		stmt   sp.Statement
		err    string
	}{
		// SELECT * statement
		{
			s: `SELECT * FROM myseries`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields: []*sp.Field{
					{Expr: &sp.Wildcard{}},
				},
				Sources: []sp.Source{&sp.Measurement{Database: "myseries"}},
			},
		},

		// SELECT group by having statement
		{
			s: `SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol GROUP BY ipo_year HAVING ipo_count > 200`,
			stmt: &sp.SelectStatement{
				IsRawQuery: false,
				Fields: []*sp.Field{
					{Expr: &sp.VarRef{Val: "ipo_year", Segments: []string{"ipo_year"}}},
					{Expr: &sp.Call{Name: "count", Args: []sp.Expr{&sp.Wildcard{}}}, Alias: "ipo_count"},
				},
				Sources: []sp.Source{&sp.Measurement{Database: "symbol"}},
				Dimensions: []*sp.Dimension{
					{Expr: &sp.VarRef{Val: "ipo_year", Segments: []string{"ipo_year"}}},
				},
				Having: &sp.BinaryExpr{
					Op:  sp.GT,
					LHS: &sp.VarRef{Val: "ipo_count", Segments: []string{"ipo_count"}},
					RHS: &sp.IntegerLiteral{Val: 200},
				},
			},
		},

		{
			s: `SELECT * FROM myseries GROUP BY *`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields: []*sp.Field{
					{Expr: &sp.Wildcard{}},
				},
				Sources:    []sp.Source{&sp.Measurement{Database: "myseries"}},
				Dimensions: []*sp.Dimension{{Expr: &sp.Wildcard{}}},
			},
		},
		{
			s: `SELECT field1, * FROM myseries GROUP BY *`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields: []*sp.Field{
					{Expr: &sp.VarRef{Val: "field1", Segments: []string{"field1"}}},
					{Expr: &sp.Wildcard{}},
				},
				Sources:    []sp.Source{&sp.Measurement{Database: "myseries"}},
				Dimensions: []*sp.Dimension{{Expr: &sp.Wildcard{}}},
			},
		},
		{
			s: `SELECT *, field1 FROM myseries GROUP BY *`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields: []*sp.Field{
					{Expr: &sp.Wildcard{}},
					{Expr: &sp.VarRef{Val: "field1", Segments: []string{"field1"}}},
				},
				Sources:    []sp.Source{&sp.Measurement{Database: "myseries"}},
				Dimensions: []*sp.Dimension{{Expr: &sp.Wildcard{}}},
			},
		},

		// SELECT statement
		{
			s: fmt.Sprintf(`SELECT mean(field1), sum(field2) ,count(field3) AS field_x FROM myseries WHERE host = 'hosta.influxdb.org' and time > %d GROUP BY time("10h") ORDER BY DESC LIMIT 20, 10`, now.Unix()),
			stmt: &sp.SelectStatement{
				IsRawQuery: false,
				Fields: []*sp.Field{
					{Expr: &sp.Call{Name: "mean", Args: []sp.Expr{&sp.VarRef{Val: "field1", Segments: []string{"field1"}}}}},
					{Expr: &sp.Call{Name: "sum", Args: []sp.Expr{&sp.VarRef{Val: "field2", Segments: []string{"field2"}}}}},
					{Expr: &sp.Call{Name: "count", Args: []sp.Expr{&sp.VarRef{Val: "field3", Segments: []string{"field3"}}}}, Alias: "field_x"},
				},
				Sources: []sp.Source{&sp.Measurement{Database: "myseries"}},
				Condition: &sp.BinaryExpr{
					Op: sp.AND,
					LHS: &sp.BinaryExpr{
						Op:  sp.EQ,
						LHS: &sp.VarRef{Val: "host", Segments: []string{"host"}},
						RHS: &sp.StringLiteral{Val: "hosta.influxdb.org"},
					},
					RHS: &sp.BinaryExpr{
						Op:  sp.GT,
						LHS: &sp.VarRef{Val: "time", Segments: []string{"time"}},
						RHS: &sp.IntegerLiteral{Val: now.Unix()},
					},
				},
				Dimensions: []*sp.Dimension{{Expr: &sp.Call{Name: "time", Args: []sp.Expr{&sp.StringLiteral{Val: "10h"}}}}},
				SortFields: []*sp.SortField{
					{Ascending: false},
				},
				Limit:  20,
				Offset: 10,
			},
		},
		{
			s: `SELECT foo.bar.baz AS foo FROM myseries`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields: []*sp.Field{
					{Expr: &sp.VarRef{Val: "foo.bar.baz", Segments: []string{"foo", "bar", "baz"}}, Alias: "foo"},
				},
				Sources: []sp.Source{&sp.Measurement{Database: "myseries"}},
			},
		},

		{
			s: `SELECT func1(arg1, 100, arg3, arg4) FROM myseries`,
			stmt: &sp.SelectStatement{
				IsRawQuery: false,
				Fields: []*sp.Field{
					{Expr: &sp.Call{
						Name: "func1",
						Args: []sp.Expr{
							&sp.VarRef{Val: "arg1", Segments: []string{"arg1"}},
							&sp.IntegerLiteral{Val: 100},
							&sp.VarRef{Val: "arg3", Segments: []string{"arg3"}},
							&sp.VarRef{Val: "arg4", Segments: []string{"arg4"}},
						}}},
				},
				Sources: []sp.Source{&sp.Measurement{Database: "myseries"}},
			},
		},

		{
			s: `SELECT func1(field1) / func2(field2) FROM myseries`,
			stmt: &sp.SelectStatement{
				IsRawQuery: false,
				Fields: []*sp.Field{
					{
						Expr: &sp.BinaryExpr{
							LHS: &sp.Call{
								Name: "func1",
								Args: []sp.Expr{
									&sp.VarRef{Val: "field1", Segments: []string{"field1"}},
								},
							},
							RHS: &sp.Call{
								Name: "func2",
								Args: []sp.Expr{
									&sp.VarRef{Val: "field2", Segments: []string{"field2"}},
								},
							},
							Op: sp.DIV,
						},
					},
				},
				Sources: []sp.Source{
					&sp.Measurement{Database: "myseries"},
				},
			},
		},

		{
			s: fmt.Sprintf(`SELECT func1(func2(field1)) FROM myseries GROUP BY func3(field3)`),
			stmt: &sp.SelectStatement{
				IsRawQuery: false,
				Fields: []*sp.Field{
					{
						Expr: &sp.Call{
							Name: "func1",
							Args: []sp.Expr{
								&sp.Call{
									Name: "func2",
									Args: []sp.Expr{
										&sp.VarRef{Val: "field1", Segments: []string{"field1"}},
									},
								},
							},
						},
					},
				},
				Sources: []sp.Source{&sp.Measurement{Database: "myseries"}},
				Dimensions: []*sp.Dimension{
					{
						Expr: &sp.Call{
							Name: "func3",
							Args: []sp.Expr{
								&sp.VarRef{Val: "field3", Segments: []string{"field3"}},
							},
						},
					},
				},
			},
		},

		// SELECT statement (lowercase)
		{
			s: `select my_field from myseries`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.VarRef{Val: "my_field", Segments: []string{"my_field"}}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "myseries"}},
			},
		},

		// SELECT statement with multiple ORDER BY fields
		{
			skip: true,
			s:    `SELECT field1 FROM myseries ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.VarRef{Val: "field1"}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "myseries"}},
				SortFields: []*sp.SortField{
					{Ascending: true},
					{Name: "field1"},
					{Name: "field2"},
				},
				Limit: 10,
			},
		},

		// SELECT * FROM cpu WHERE host = 'serverC' AND region =~ /.*west.*/
		{
			s: `SELECT * FROM cpu WHERE host = 'serverC' AND region =~ /.*west.*/`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.Wildcard{}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "cpu"}},
				Condition: &sp.BinaryExpr{
					Op: sp.AND,
					LHS: &sp.BinaryExpr{
						Op:  sp.EQ,
						LHS: &sp.VarRef{Val: "host", Segments: []string{"host"}},
						RHS: &sp.StringLiteral{Val: "serverC"},
					},
					RHS: &sp.BinaryExpr{
						Op:  sp.EQREGEX,
						LHS: &sp.VarRef{Val: "region", Segments: []string{"region"}},
						RHS: &sp.RegexLiteral{Val: regexp.MustCompile(".*west.*")},
					},
				},
			},
		},

		{
			s: `select count(distinct(field3)), sum(field4) from metrics`,
			stmt: &sp.SelectStatement{
				IsRawQuery: false,
				Fields: []*sp.Field{
					{Expr: &sp.Call{Name: "count", Args: []sp.Expr{&sp.Call{Name: "distinct", Args: []sp.Expr{&sp.VarRef{Val: "field3", Segments: []string{"field3"}}}}}}},
					{Expr: &sp.Call{Name: "sum", Args: []sp.Expr{&sp.VarRef{Val: "field4", Segments: []string{"field4"}}}}},
				},
				Sources: []sp.Source{&sp.Measurement{Database: "metrics"}},
			},
		},

		// SELECT * FROM WHERE field comparisons
		{
			s: `SELECT * FROM cpu WHERE load > 100`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.Wildcard{}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "cpu"}},
				Condition: &sp.BinaryExpr{
					Op:  sp.GT,
					LHS: &sp.VarRef{Val: "load", Segments: []string{"load"}},
					RHS: &sp.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load >= 100`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.Wildcard{}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "cpu"}},
				Condition: &sp.BinaryExpr{
					Op:  sp.GTE,
					LHS: &sp.VarRef{Val: "load", Segments: []string{"load"}},
					RHS: &sp.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load = 100`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.Wildcard{}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "cpu"}},
				Condition: &sp.BinaryExpr{
					Op:  sp.EQ,
					LHS: &sp.VarRef{Val: "load", Segments: []string{"load"}},
					RHS: &sp.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load <= 100`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.Wildcard{}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "cpu"}},
				Condition: &sp.BinaryExpr{
					Op:  sp.LTE,
					LHS: &sp.VarRef{Val: "load", Segments: []string{"load"}},
					RHS: &sp.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load < 100`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.Wildcard{}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "cpu"}},
				Condition: &sp.BinaryExpr{
					Op:  sp.LT,
					LHS: &sp.VarRef{Val: "load", Segments: []string{"load"}},
					RHS: &sp.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load != 100`,
			stmt: &sp.SelectStatement{
				IsRawQuery: true,
				Fields:     []*sp.Field{{Expr: &sp.Wildcard{}}},
				Sources:    []sp.Source{&sp.Measurement{Database: "cpu"}},
				Condition: &sp.BinaryExpr{
					Op:  sp.NEQ,
					LHS: &sp.VarRef{Val: "load", Segments: []string{"load"}},
					RHS: &sp.IntegerLiteral{Val: 100},
				},
			},
		},
		// Errors
		{s: ``, err: `found EOF, expected SELECT at line 1, char 1`},
		{s: `SELECT`, err: `found EOF, expected identifier, string, number, bool at line 1, char 8`},
		{s: `blah blah`, err: `found blah, expected SELECT at line 1, char 1`},
		{s: `SELECT field1 X`, err: `found X, expected FROM at line 1, char 15`},
		{s: `SELECT field1 FROM "series" WHERE X`, err: `found series, expected identifier at line 1, char 19`},
		{s: `SELECT field1 FROM myseries GROUP`, err: `found EOF, expected BY at line 1, char 35`},
		{s: `SELECT field1 FROM myseries LIMIT`, err: `found EOF, expected integer at line 1, char 35`},
		{s: `SELECT field1 FROM myseries LIMIT 10.5`, err: `found 10.5, expected integer at line 1, char 35`},
		{s: `SELECT top() FROM myseries`, err: `invalid number of arguments for top, expected at least 1, got 0`},
		{s: `SELECT field1 FROM myseries ORDER`, err: `found EOF, expected BY at line 1, char 35`},
		{s: `SELECT field1 FROM myseries ORDER BY`, err: `found EOF, expected identifier, ASC, DESC at line 1, char 38`},
		{s: `SELECT field1 FROM myseries ORDER BY /`, err: `found /, expected identifier, ASC, DESC at line 1, char 38`},
		{s: `SELECT field1 FROM myseries ORDER BY 1`, err: `found 1, expected identifier, ASC, DESC at line 1, char 38`},
		{s: `SELECT field1 FROM myseries ORDER BY time ASC,`, err: `found EOF, expected identifier at line 1, char 47`},
		{s: `SELECT field1 AS`, err: `found EOF, expected identifier at line 1, char 18`},
		{s: `SELECT field1 FROM 12`, err: `found 12, expected identifier at line 1, char 20`},
		{s: `SELECT 1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 FROM myseries`, err: `unable to parse integer at line 1, char 8`},
		{s: `SELECT 10.5h FROM myseries`, err: `found h, expected FROM at line 1, char 12`},
		{s: `SELECT value > 2 FROM cpu`, err: `invalid operator > in SELECT field, only support +-*/`},
		{s: `SELECT value = 2 FROM cpu`, err: `invalid operator = in SELECT field, only support +-*/`},
	}

	for i, tt := range tests {
		if tt.skip {
			continue
		}
		p := sp.NewParser(strings.NewReader(tt.s))

		stmt, err := p.ParseStatement()

		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.stmt, stmt) {
				t.Logf("\n# %s\nexp=%s\ngot=%s\n", tt.s, mustMarshalJSON(tt.stmt), mustMarshalJSON(stmt))
				t.Logf("\nSQL exp=%s\nSQL got=%s\n", tt.stmt.String(), stmt.String())
				t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
			} else {

				stmt2, err := sp.ParseStatement(stmt.String())
				if err != nil {
					t.Errorf("%d. %q: unable to parse statement string: %s", i, stmt.String(), err)
				} else if !reflect.DeepEqual(tt.stmt, stmt2) {
					t.Logf("\n# %s\nexp=%s\ngot=%s\n", tt.s, mustMarshalJSON(tt.stmt), mustMarshalJSON(stmt2))
					t.Logf("\nSQL exp=%s\nSQL got=%s\n", tt.stmt.String(), stmt2.String())
					t.Errorf("%d. %q\n\nstmt reparse mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt2)
				}
			}
		}
	}
}

func TestParseGroupBy(t *testing.T) {
	// For use in various tests.
	var tests = []struct {
		s   string
		d   string
		err string
	}{
		{s: `SELECT sum(x) FROM Packetbeat where uid="xxx" group by tcp.src_ip`, d: `tcp.src_ip`, err: ``},
		{s: `SELECT sum(x) FROM Packetbeat group by tcp.src_ip, tcp.dst_ip`, d: `tcp.src_ip, tcp.dst_ip`, err: ``},
	}
	for i, tt := range tests {
		p := sp.NewParser(strings.NewReader(tt.s))
		stmt, err := p.ParseStatement()

		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		}

		d := stmt.(*sp.SelectStatement).Dimensions.String()

		if d != tt.d {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.d, d)
		}
	}
}

// Ensure the parser can only parse select statement
func TestParseStatement(t *testing.T) {
	// For use in various tests.
	var tests = []struct {
		s    string
		stmt sp.Statement
		err  string
	}{
		// Errors
		{s: ``, err: `found EOF, expected SELECT at line 1, char 1`},
		{s: `CREATE`, err: `found CREATE, expected SELECT at line 1, char 1`},
		{s: `SELECT sum(x) FROM Packetbeat`, err: ``},
	}
	for i, tt := range tests {
		p := sp.NewParser(strings.NewReader(tt.s))
		_, err := p.ParseStatement()

		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		}
	}
}

// Ensure the parser can parse expressions into an AST.
func TestParser_ParseExpr(t *testing.T) {
	var tests = []struct {
		s    string
		expr sp.Expr
		err  string
	}{
		// Primitives
		{s: `100.0`, expr: &sp.NumberLiteral{Val: 100}},
		{s: `100`, expr: &sp.IntegerLiteral{Val: 100}},
		{s: `'foo bar'`, expr: &sp.StringLiteral{Val: "foo bar"}},
		{s: `true`, expr: &sp.BooleanLiteral{Val: true}},
		{s: `false`, expr: &sp.BooleanLiteral{Val: false}},
		{s: `my_ident`, expr: &sp.VarRef{Val: "my_ident", Segments: []string{"my_ident"}}},
		// Simple binary expression
		{
			s: `1 + 2`,
			expr: &sp.BinaryExpr{
				Op:  sp.ADD,
				LHS: &sp.IntegerLiteral{Val: 1},
				RHS: &sp.IntegerLiteral{Val: 2},
			},
		},

		// Binary expression with LHS precedence
		{
			s: `1 * 2 + 3`,
			expr: &sp.BinaryExpr{
				Op: sp.ADD,
				LHS: &sp.BinaryExpr{
					Op:  sp.MUL,
					LHS: &sp.IntegerLiteral{Val: 1},
					RHS: &sp.IntegerLiteral{Val: 2},
				},
				RHS: &sp.IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with RHS precedence
		{
			s: `1 + 2 * 3`,
			expr: &sp.BinaryExpr{
				Op:  sp.ADD,
				LHS: &sp.IntegerLiteral{Val: 1},
				RHS: &sp.BinaryExpr{
					Op:  sp.MUL,
					LHS: &sp.IntegerLiteral{Val: 2},
					RHS: &sp.IntegerLiteral{Val: 3},
				},
			},
		},

		// Binary expression with LHS paren group.
		{
			s: `(1 + 2) * 3`,
			expr: &sp.BinaryExpr{
				Op: sp.MUL,
				LHS: &sp.ParenExpr{
					Expr: &sp.BinaryExpr{
						Op:  sp.ADD,
						LHS: &sp.IntegerLiteral{Val: 1},
						RHS: &sp.IntegerLiteral{Val: 2},
					},
				},
				RHS: &sp.IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with no precedence, tests left associativity.
		{
			s: `1 * 2 * 3`,
			expr: &sp.BinaryExpr{
				Op: sp.MUL,
				LHS: &sp.BinaryExpr{
					Op:  sp.MUL,
					LHS: &sp.IntegerLiteral{Val: 1},
					RHS: &sp.IntegerLiteral{Val: 2},
				},
				RHS: &sp.IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with regex.
		{
			s: `region =~ /us.*/`,
			expr: &sp.BinaryExpr{
				Op:  sp.EQREGEX,
				LHS: &sp.VarRef{Val: "region", Segments: []string{"region"}},
				RHS: &sp.RegexLiteral{Val: regexp.MustCompile(`us.*`)},
			},
		},

		// Binary expression with quoted '/' regex.
		{
			s: `url =~ /http\:\/\/www\.example\.com/`,
			expr: &sp.BinaryExpr{
				Op:  sp.EQREGEX,
				LHS: &sp.VarRef{Val: "url", Segments: []string{"url"}},
				RHS: &sp.RegexLiteral{Val: regexp.MustCompile(`http\://www\.example\.com`)},
			},
		},

		// Complex binary expression.
		{
			s: `value + 3 < 30 AND 1 + 2 OR true`,
			expr: &sp.BinaryExpr{
				Op: sp.OR,
				LHS: &sp.BinaryExpr{
					Op: sp.AND,
					LHS: &sp.BinaryExpr{
						Op: sp.LT,
						LHS: &sp.BinaryExpr{
							Op:  sp.ADD,
							LHS: &sp.VarRef{Val: "value", Segments: []string{"value"}},
							RHS: &sp.IntegerLiteral{Val: 3},
						},
						RHS: &sp.IntegerLiteral{Val: 30},
					},
					RHS: &sp.BinaryExpr{
						Op:  sp.ADD,
						LHS: &sp.IntegerLiteral{Val: 1},
						RHS: &sp.IntegerLiteral{Val: 2},
					},
				},
				RHS: &sp.BooleanLiteral{Val: true},
			},
		},

		// Function call (empty)
		{
			s: `my_func()`,
			expr: &sp.Call{
				Name: "my_func",
			},
		},

		// Function call (multi-arg)
		{
			s: `my_func(1, 2 + 3)`,
			expr: &sp.Call{
				Name: "my_func",
				Args: []sp.Expr{
					&sp.IntegerLiteral{Val: 1},
					&sp.BinaryExpr{
						Op:  sp.ADD,
						LHS: &sp.IntegerLiteral{Val: 2},
						RHS: &sp.IntegerLiteral{Val: 3},
					},
				},
			},
		},
	}

	for i, tt := range tests {
		expr, err := sp.NewParser(strings.NewReader(tt.s)).ParseExpr()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.expr, expr) {
			t.Errorf("%d. %q\n\nexpr mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.expr, expr)
		}
	}
}

// Ensure a string can be quoted.
func TestQuote(t *testing.T) {
	for i, tt := range []struct {
		in  string
		out string
	}{
		{``, `''`},
		{`foo`, `'foo'`},
		{"foo\nbar", `'foo\nbar'`},
		{`foo bar\\`, `'foo bar\\\\'`},
		{`'foo'`, `'\'foo\''`},
	} {
		if out := sp.QuoteString(tt.in); tt.out != out {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.in, tt.out, out)
		}
	}
}

// Ensure an identifier's segments can be quoted.
func TestQuoteIdent(t *testing.T) {
	for i, tt := range []struct {
		ident []string
		s     string
	}{
		{[]string{``}, `""`},
		{[]string{`select`}, `"select"`},
		{[]string{`in-bytes`}, `"in-bytes"`},
		{[]string{`foo`, `bar`}, `"foo".bar`},
		{[]string{`foo`, ``, `bar`}, `"foo"..bar`},
		{[]string{`foo bar`, `baz`}, `"foo bar".baz`},
		{[]string{`foo.bar`, `baz`}, `"foo.bar".baz`},
		{[]string{`foo.bar`, `rp`, `baz`}, `"foo.bar"."rp".baz`},
		{[]string{`foo.bar`, `rp`, `1baz`}, `"foo.bar"."rp"."1baz"`},
	} {
		if s := sp.QuoteIdent(tt.ident...); tt.s != s {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.ident, tt.s, s)
		}
	}
}

// MustParseSelectStatement parses a select statement. Panic on error.
func MustParseSelectStatement(s string) *sp.SelectStatement {
	stmt, err := sp.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err)
	}
	return stmt.(*sp.SelectStatement)
}

// MustParseExpr parses an expression. Panic on error.
func MustParseExpr(s string) sp.Expr {
	expr, err := sp.NewParser(strings.NewReader(s)).ParseExpr()
	if err != nil {
		fmt.Println(s)
		panic(err)
	}
	return expr
}

// mustMarshalJSON encodes a value to JSON.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return b
}

func intptr(v int) *int {
	return &v
}

func BenchmarkParseStatement1(b *testing.B) {
	b.ReportAllocs()
	s := `SELECT count(field) FROM series WHERE value > 10`
	for i := 0; i < b.N; i++ {

		if stmt, err := sp.NewParser(strings.NewReader(s)).ParseStatement(); err != nil {
			b.Fatalf("unexpected error: %s", err)
		} else if stmt == nil {
			b.Fatalf("expected statement: %s", stmt)
		} else {
			_ = stmt.String()
		}
	}
	//	b.SetBytes(int64(len(s)))
}

func BenchmarkParseStatement2(b *testing.B) {
	b.ReportAllocs()
	s := "select max(tcp.in_pkts) from packetbeat where guid = 'for a test you know'"
	for i := 0; i < b.N; i++ {
		if stmt, err := sp.NewParser(strings.NewReader(s)).ParseStatement(); err != nil {
			b.Fatalf("unexpected error: %s", err)
		} else if stmt == nil {
			b.Fatalf("expected statement: %s", stmt)
		}
	}
	b.SetBytes(int64(len(s)))
}
