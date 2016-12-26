package sp

// rewriteSources rewrites sources with previous database and retention policy
func rewriteSources(sources Sources, measurementName, defaultDatabase string) Sources {
	newSources := Sources{}
	for _, src := range sources {
		if src == nil {
			continue
		}
		mm := src.(*Measurement)
		database := mm.Database
		if database == "" {
			database = defaultDatabase
		}
		newSources = append(newSources,
			&Measurement{
				Database:        database,
				RetentionPolicy: mm.RetentionPolicy,
				Name:            measurementName,
			})
	}
	if len(newSources) <= 0 {
		return append(newSources, &Measurement{
			Database: defaultDatabase,
			Name:     measurementName,
		})
	}
	return newSources
}

// rewriteSourcesCondition rewrites sources into `name` expressions.
// Merges with cond and returns a new condition.
func rewriteSourcesCondition(sources Sources, cond Expr) Expr {
	if len(sources) == 0 {
		return cond
	}

	// Generate an OR'd set of filters on source name.
	var scond Expr
	for _, source := range sources {
		mm := source.(*Measurement)

		// Generate a filtering expression on the measurement name.
		var expr Expr
		if mm.Regex != nil {
			expr = &BinaryExpr{
				Op:  EQREGEX,
				LHS: &VarRef{Val: "_name"},
				RHS: &RegexLiteral{Val: mm.Regex.Val},
			}
		} else if mm.Name != "" {
			expr = &BinaryExpr{
				Op:  EQ,
				LHS: &VarRef{Val: "_name"},
				RHS: &StringLiteral{Val: mm.Name},
			}
		}

		if scond == nil {
			scond = expr
		} else {
			scond = &BinaryExpr{
				Op:  OR,
				LHS: scond,
				RHS: expr,
			}
		}
	}

	if cond != nil {
		return &BinaryExpr{
			Op:  AND,
			LHS: &ParenExpr{Expr: scond},
			RHS: &ParenExpr{Expr: cond},
		}
	}
	return scond
}
