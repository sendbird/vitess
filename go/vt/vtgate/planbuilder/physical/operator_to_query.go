/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package physical

import (
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
)

func ToSQL(ctx *plancontext.PlanningContext, op abstract.PhysicalOperator) sqlparser.SelectStatement {
	q := &queryBuilder{ctx: ctx}
	buildQuery(op, q)
	q.produce()
	return q.sel
}

func buildQuery(op abstract.PhysicalOperator, qb *queryBuilder) {
	switch op := op.(type) {
	case *Table:
		dbName := ""

		if op.QTable.IsInfSchema {
			dbName = op.QTable.Table.Qualifier.String()
		}
		qb.addTable(dbName, op.QTable.Table.Name.String(), op.QTable.Alias.As.String(), op.TableID(), op.QTable.Alias.Hints)
		for _, pred := range op.QTable.Predicates {
			qb.addPredicate(pred)
		}
		for _, name := range op.Columns {
			qb.addProjection(&sqlparser.AliasedExpr{Expr: name})
		}
	case *ApplyJoin:
		buildQuery(op.LHS, qb)
		// If we are going to add the predicate used in join here
		// We should not add the predicate's copy of when it was split into
		// two parts. To avoid this, we use the SkipPredicates map.
		for _, expr := range qb.ctx.JoinPredicates[op.Predicate] {
			qb.ctx.SkipPredicates[expr] = nil
		}
		qbR := &queryBuilder{ctx: qb.ctx}
		buildQuery(op.RHS, qbR)
		if op.LeftJoin {
			qb.joinOuterWith(qbR, op.Predicate)
		} else {
			qb.joinInnerWith(qbR, op.Predicate)
		}
	case *Filter:
		buildQuery(op.Source, qb)
		for _, pred := range op.Predicates {
			qb.addPredicate(pred)
		}
	case *Derived:
		buildQuery(op.Source, qb)
		sel := qb.sel.(*sqlparser.Select) // we can only handle SELECT in derived tables at the moment
		qb.sel = nil
		sel.SelectExprs = sqlparser.GetFirstSelect(op.Query).SelectExprs
		qb.addTableExpr(op.Alias, op.Alias, op.TableID(), &sqlparser.DerivedTable{
			Select: sel,
		}, nil)
	default:
		panic(fmt.Sprintf("%T", op))
	}
}

func (qb *queryBuilder) produce() {
	sort.Sort(qb)
}

func (qb *queryBuilder) addTable(db, tableName, alias string, tableID semantics.TableSet, hints *sqlparser.IndexHints) {
	tableExpr := sqlparser.TableName{
		Name:      sqlparser.NewTableIdent(tableName),
		Qualifier: sqlparser.NewTableIdent(db),
	}
	qb.addTableExpr(tableName, alias, tableID, tableExpr, hints)
}

func (qb *queryBuilder) addTableExpr(tableName, alias string, tableID semantics.TableSet, tblExpr sqlparser.SimpleTableExpr, hint *sqlparser.IndexHints) {
	if qb.sel == nil {
		qb.sel = &sqlparser.Select{}
	}
	sel := qb.sel.(*sqlparser.Select)
	sel.From = append(sel.From, &sqlparser.AliasedTableExpr{
		Expr:       tblExpr,
		Partitions: nil,
		As:         sqlparser.NewTableIdent(alias),
		Hints:      hint,
		Columns:    nil,
	})
	qb.sel = sel
	qb.tableNames = append(qb.tableNames, tableName)
	qb.tableIDsInFrom = append(qb.tableIDsInFrom, tableID)
}

func (qb *queryBuilder) addPredicate(expr sqlparser.Expr) {
	if _, toBeSkipped := qb.ctx.SkipPredicates[expr]; toBeSkipped {
		// This is a predicate that was added to the RHS of an ApplyJoin.
		// The original predicate will be added, so we don't have to add this here
		return
	}

	sel := qb.sel.(*sqlparser.Select)
	if sel.Where == nil {
		sel.AddWhere(expr)
		return
	}
	for _, exp := range sqlparser.SplitAndExpression(nil, expr) {
		sel.AddWhere(exp)
	}
}

func (qb *queryBuilder) addProjection(projection *sqlparser.AliasedExpr) {
	sel := qb.sel.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, projection)
}

func (qb *queryBuilder) joinInnerWith(other *queryBuilder, onCondition sqlparser.Expr) {
	sel := qb.sel.(*sqlparser.Select)
	otherSel := other.sel.(*sqlparser.Select)
	sel.From = append(sel.From, otherSel.From...)
	qb.tableIDsInFrom = append(qb.tableIDsInFrom, other.tableIDsInFrom...)
	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)

	var predicate sqlparser.Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predicate = sqlparser.AndExpressions(sqlparser.SplitAndExpression(sqlparser.SplitAndExpression(nil, predicate), otherSel.Where.Expr)...)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
	}

	qb.addPredicate(onCondition)
}

func (qb *queryBuilder) joinOuterWith(other *queryBuilder, onCondition sqlparser.Expr) {
	sel := qb.sel.(*sqlparser.Select)
	otherSel := other.sel.(*sqlparser.Select)
	var lhs sqlparser.TableExpr
	if len(sel.From) == 1 {
		lhs = sel.From[0]
	} else {
		lhs = &sqlparser.ParenTableExpr{Exprs: sel.From}
	}
	var rhs sqlparser.TableExpr
	if len(otherSel.From) == 1 {
		rhs = otherSel.From[0]
	} else {
		rhs = &sqlparser.ParenTableExpr{Exprs: otherSel.From}
	}
	sel.From = []sqlparser.TableExpr{&sqlparser.JoinTableExpr{
		LeftExpr:  lhs,
		RightExpr: rhs,
		Join:      sqlparser.LeftJoinType,
		Condition: &sqlparser.JoinCondition{
			On: onCondition,
		},
	}}
	tableSet := semantics.EmptyTableSet()
	for _, set := range qb.tableIDsInFrom {
		tableSet.MergeInPlace(set)
	}
	for _, set := range other.tableIDsInFrom {
		tableSet.MergeInPlace(set)
	}

	qb.tableIDsInFrom = []semantics.TableSet{tableSet}
	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)
	var predicate sqlparser.Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predicate = sqlparser.AndExpressions(predicate, otherSel.Where.Expr)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
	}
}

func (qb *queryBuilder) rewriteExprForDerivedTable(expr sqlparser.Expr, dtName string) {
	sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			hasTable := qb.hasTable(node.Qualifier.Name.String())
			if hasTable {
				node.Qualifier = sqlparser.TableName{
					Name: sqlparser.NewTableIdent(dtName),
				}
			}
		}
		return true
	}, nil)
}

func (qb *queryBuilder) hasTable(tableName string) bool {
	for _, name := range qb.tableNames {
		if strings.EqualFold(tableName, name) {
			return true
		}
	}
	return false
}

type queryBuilder struct {
	ctx            *plancontext.PlanningContext
	sel            sqlparser.SelectStatement
	tableIDsInFrom []semantics.TableSet
	tableNames     []string
}

// Len implements the Sort interface
func (qb *queryBuilder) Len() int {
	return len(qb.tableIDsInFrom)
}

// Less implements the Sort interface
func (qb *queryBuilder) Less(i, j int) bool {
	return qb.tableIDsInFrom[i].TableOffset() < qb.tableIDsInFrom[j].TableOffset()
}

// Swap implements the Sort interface
func (qb *queryBuilder) Swap(i, j int) {
	sel, isSel := qb.sel.(*sqlparser.Select)
	if isSel {
		sel.From[i], sel.From[j] = sel.From[j], sel.From[i]
	}
	qb.tableIDsInFrom[i], qb.tableIDsInFrom[j] = qb.tableIDsInFrom[j], qb.tableIDsInFrom[i]
}