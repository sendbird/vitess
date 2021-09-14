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

package semantics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

const T0 TableSet = 0

const (
	// Just here to make outputs more readable
	T1 TableSet = 1 << iota
	T2
	T3
	T4
	T5
)

func extract(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}

func TestBindingSingleTablePositive(t *testing.T) {
	queries := []string{
		"select col from tabl",
		"select tabl.col from tabl",
		"select d.tabl.col from tabl",
		"select col from d.tabl",
		"select tabl.col from d.tabl",
		"select d.tabl.col from d.tabl",
		"select col+col from tabl",
		"select max(col1+col2) from d.tabl",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query, "d")
			sel, _ := stmt.(*sqlparser.Select)
			t1 := sel.From[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.EqualValues(t, 1, ts)

			recursiveDeps := semTable.RecursiveDeps(extract(sel, 0))
			assert.Equal(t, T1, recursiveDeps, query)
			assert.Equal(t, T1, semTable.DirectDeps(extract(sel, 0)), query)
			assert.Equal(t, 1, recursiveDeps.NumberOfTables(), "number of tables is wrong")
		})
	}
}

func TestBindingSingleAliasedTablePositive(t *testing.T) {
	queries := []string{
		"select col from tabl as X",
		"select tabl.col from X as tabl",
		"select col from d.X as tabl",
		"select tabl.col from d.X as tabl",
		"select col+col from tabl as X",
		"select max(tabl.col1 + tabl.col2) from d.X as tabl",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query, "")
			sel, _ := stmt.(*sqlparser.Select)
			t1 := sel.From[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.EqualValues(t, 1, ts)

			recursiveDeps := semTable.RecursiveDeps(extract(sel, 0))
			require.Equal(t, T1, recursiveDeps, query)
			assert.Equal(t, 1, recursiveDeps.NumberOfTables(), "number of tables is wrong")
		})
	}
}

func TestBindingSingleTableNegative(t *testing.T) {
	queries := []string{
		"select foo.col from tabl",
		"select ks.tabl.col from tabl",
		"select ks.tabl.col from d.tabl",
		"select d.tabl.col from ks.tabl",
		"select foo.col from d.tabl",
		"select tabl.col from d.tabl as t",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)
			_, err = Analyze(parse.(sqlparser.SelectStatement), "d", &FakeSI{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "symbol")
			require.Contains(t, err.Error(), "not found")
		})
	}
}

func TestBindingSingleAliasedTableNegative(t *testing.T) {
	queries := []string{
		"select tabl.col from tabl as X",
		"select d.X.col from d.X as tabl",
		"select d.tabl.col from X as tabl",
		"select d.tabl.col from ks.X as tabl",
		"select d.tabl.col from d.X as tabl",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)
			_, err = Analyze(parse.(sqlparser.SelectStatement), "", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewTableIdent("t")},
				},
			})
			require.Error(t, err)
		})
	}
}

func TestBindingMultiTablePositive(t *testing.T) {
	type testCase struct {
		query          string
		deps           TableSet
		numberOfTables int
	}
	queries := []testCase{{
		query:          "select t.col from t, s",
		deps:           T1,
		numberOfTables: 1,
	}, {
		query:          "select s.col from t join s",
		deps:           T2,
		numberOfTables: 1,
	}, {
		query:          "select max(t.col+s.col) from t, s",
		deps:           T1 | T2,
		numberOfTables: 2,
	}, {
		query:          "select max(t.col+s.col) from t join s",
		deps:           T1 | T2,
		numberOfTables: 2,
	}, {
		query:          "select case t.col when s.col then r.col else u.col end from t, s, r, w, u",
		deps:           T1 | T2 | T3 | T5,
		numberOfTables: 4,
		// }, {
		// TODO: move to subquery
		// make sure that we don't let sub-query Dependencies leak out by mistake
		// query: "select t.col + (select 42 from s) from t",
		// deps:  T1,
		// }, {
		// 	query: "select (select 42 from s where r.id = s.id) from r",
		// 	deps:  T1 | T2,
	}, {
		query:          "select u1.a + u2.a from u1, u2",
		deps:           T1 | T2,
		numberOfTables: 2,
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query.query, "user")
			sel, _ := stmt.(*sqlparser.Select)
			recursiveDeps := semTable.RecursiveDeps(extract(sel, 0))
			assert.Equal(t, query.deps, recursiveDeps, query.query)
			assert.Equal(t, query.numberOfTables, recursiveDeps.NumberOfTables(), "number of tables is wrong")
		})
	}
}

func TestBindingMultiAliasedTablePositive(t *testing.T) {
	type testCase struct {
		query          string
		deps           TableSet
		numberOfTables int
	}
	queries := []testCase{{
		query:          "select X.col from t as X, s as S",
		deps:           T1,
		numberOfTables: 1,
	}, {
		query:          "select X.col+S.col from t as X, s as S",
		deps:           T1 | T2,
		numberOfTables: 2,
	}, {
		query:          "select max(X.col+S.col) from t as X, s as S",
		deps:           T1 | T2,
		numberOfTables: 2,
	}, {
		query:          "select max(X.col+s.col) from t as X, s",
		deps:           T1 | T2,
		numberOfTables: 2,
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query.query, "user")
			sel, _ := stmt.(*sqlparser.Select)
			recursiveDeps := semTable.RecursiveDeps(extract(sel, 0))
			assert.Equal(t, query.deps, recursiveDeps, query.query)
			assert.Equal(t, query.numberOfTables, recursiveDeps.NumberOfTables(), "number of tables is wrong")
		})
	}
}

func TestBindingMultiTableNegative(t *testing.T) {
	queries := []string{
		"select 1 from d.tabl, d.tabl",
		"select 1 from d.tabl, tabl",
		"select t.col from k.t, t",
		"select b.t.col from b.t, t",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)
			_, err = Analyze(parse.(sqlparser.SelectStatement), "d", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"tabl": {Name: sqlparser.NewTableIdent("tabl")},
					"foo":  {Name: sqlparser.NewTableIdent("foo")},
				},
			})
			require.Error(t, err)
		})
	}
}

func TestBindingMultiAliasedTableNegative(t *testing.T) {
	queries := []string{
		"select 1 from d.tabl as tabl, d.tabl",
		"select 1 from d.tabl as tabl, tabl",
		"select 1 from d.tabl as a, tabl as a",
		"select 1 from user join user_extra user",
		"select t.col from k.t as t, t",
		"select b.t.col from b.t as t, t",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)
			_, err = Analyze(parse.(sqlparser.SelectStatement), "d", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"tabl": {Name: sqlparser.NewTableIdent("tabl")},
					"foo":  {Name: sqlparser.NewTableIdent("foo")},
				},
			})
			require.Error(t, err)
		})
	}
}

func TestNotUniqueTableName(t *testing.T) {
	queries := []string{
		"select * from t, t",
		"select * from t, (select 1 from x) as t",
		"select * from t join t",
		"select * from t join (select 1 from x) as t",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, _ := sqlparser.Parse(query)
			_, err := Analyze(parse.(sqlparser.SelectStatement), "test", &FakeSI{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "Not unique table/alias")
		})
	}
}

func TestMissingTable(t *testing.T) {
	queries := []string{
		"select t.col from a",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, _ := sqlparser.Parse(query)
			_, err := Analyze(parse.(sqlparser.SelectStatement), "", &FakeSI{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "symbol t.col not found")
		})
	}
}

func TestUnknownColumnMap2(t *testing.T) {
	varchar := querypb.Type_VARCHAR
	int := querypb.Type_INT32

	authoritativeTblA := vindexes.Table{
		Name: sqlparser.NewTableIdent("a"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewColIdent("col2"),
			Type: varchar,
		}},
		ColumnListAuthoritative: true,
	}
	authoritativeTblB := vindexes.Table{
		Name: sqlparser.NewTableIdent("b"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewColIdent("col"),
			Type: varchar,
		}},
		ColumnListAuthoritative: true,
	}
	nonAuthoritativeTblA := authoritativeTblA
	nonAuthoritativeTblA.ColumnListAuthoritative = false
	nonAuthoritativeTblB := authoritativeTblB
	nonAuthoritativeTblB.ColumnListAuthoritative = false
	authoritativeTblAWithConflict := vindexes.Table{
		Name: sqlparser.NewTableIdent("a"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewColIdent("col"),
			Type: int,
		}},
		ColumnListAuthoritative: true,
	}
	authoritativeTblBWithInt := vindexes.Table{
		Name: sqlparser.NewTableIdent("b"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewColIdent("col"),
			Type: int,
		}},
		ColumnListAuthoritative: true,
	}

	tests := []struct {
		name   string
		schema map[string]*vindexes.Table
		err    bool
		typ    *querypb.Type
	}{{
		name:   "no info about tables",
		schema: map[string]*vindexes.Table{"a": {}, "b": {}},
		err:    true,
	}, {
		name:   "non authoritative columns",
		schema: map[string]*vindexes.Table{"a": &nonAuthoritativeTblA, "b": &nonAuthoritativeTblA},
		err:    true,
	}, {
		name:   "non authoritative columns - one authoritative and one not",
		schema: map[string]*vindexes.Table{"a": &nonAuthoritativeTblA, "b": &authoritativeTblB},
		err:    false,
		typ:    &varchar,
	}, {
		name:   "non authoritative columns - one authoritative and one not",
		schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &nonAuthoritativeTblB},
		err:    false,
		typ:    &varchar,
	}, {
		name:   "authoritative columns",
		schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &authoritativeTblB},
		err:    false,
		typ:    &varchar,
	}, {
		name:   "authoritative columns",
		schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &authoritativeTblBWithInt},
		err:    false,
		typ:    &int,
	}, {
		name:   "authoritative columns with overlap",
		schema: map[string]*vindexes.Table{"a": &authoritativeTblAWithConflict, "b": &authoritativeTblB},
		err:    true,
	}}
	query := "select col from a, b"
	parse, _ := sqlparser.Parse(query)
	expr := extract(parse.(*sqlparser.Select), 0)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			si := &FakeSI{Tables: test.schema}
			tbl, err := Analyze(parse.(sqlparser.SelectStatement), "", si)
			if test.err {
				require.True(t, err != nil || tbl.ProjectionErr != nil)
			} else {
				require.NoError(t, err)
				require.NoError(t, tbl.ProjectionErr)
				typ := tbl.TypeFor(expr)
				assert.Equal(t, test.typ, typ)
			}
		})
	}
}

func TestUnknownPredicate(t *testing.T) {
	query := "select 1 from a, b where col = 1"
	authoritativeTblA := &vindexes.Table{
		Name: sqlparser.NewTableIdent("a"),
	}
	authoritativeTblB := &vindexes.Table{
		Name: sqlparser.NewTableIdent("b"),
	}

	parse, _ := sqlparser.Parse(query)

	tests := []struct {
		name   string
		schema map[string]*vindexes.Table
		err    bool
	}{
		{
			name:   "no info about tables",
			schema: map[string]*vindexes.Table{"a": authoritativeTblA, "b": authoritativeTblB},
			err:    false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			si := &FakeSI{Tables: test.schema}
			_, err := Analyze(parse.(sqlparser.SelectStatement), "", si)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestScoping(t *testing.T) {
	queries := []struct {
		query        string
		errorMessage string
	}{
		{
			query:        "select 1 from u1, u2 left join u3 on u1.a = u2.a",
			errorMessage: "symbol u1.a not found",
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			_, err = Analyze(parse.(sqlparser.SelectStatement), "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewTableIdent("t")},
				},
			})
			if query.errorMessage == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, query.errorMessage)
			}
		})
	}
}

func TestScopeForSubqueries(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{
		{
			sql: `
select t.col1, (
	select t.col2 from z as t) 
from x as t`,
			deps: T2,
		}, {
			sql: `
select t.col1, (
	select t.col2 from z) 
from x as t`,
			deps: T1,
		}, {
			sql: `
select t.col1, (
	select (select z.col2 from y) from z) 
from x as t`,
			deps: T2,
		}, {
			sql: `
select t.col1, (
	select (select y.col2 from y) from z) 
from x as t`,
			deps: T3,
		},
	}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel, _ := stmt.(*sqlparser.Select)

			// extract the first expression from the subquery (which should be the second expression in the outer query)
			sel2 := sel.SelectExprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.Subquery).Select.(*sqlparser.Select)
			exp := extract(sel2, 0)
			s1 := semTable.RecursiveDeps(exp)
			require.NoError(t, semTable.ProjectionErr)
			// if scoping works as expected, we should be able to see the inner table being used by the inner expression
			assert.Equal(t, tc.deps, s1)
		})
	}
}

func TestSubqueryBinding(t *testing.T) {
	queries := []struct {
		query    string
		expected TableSet
	}{{
		query:    "select * from user u where exists (select * from user order by col)",
		expected: T2,
	}, {
		query:    "select * from user u where exists (select * from user order by user.col)",
		expected: T2,
	}, {
		query:    "select * from user u where exists (select * from user order by u.col)",
		expected: T1,
	}}

	for _, tc := range queries {
		t.Run(tc.query, func(t *testing.T) {
			ast, err := sqlparser.Parse(tc.query)
			require.NoError(t, err)

			sel := ast.(*sqlparser.Select)
			st, err := Analyze(sel, "dbName", fakeSchemaInfo())
			require.NoError(t, err)
			exists := sel.Where.Expr.(*sqlparser.ExistsExpr)
			expr := exists.Subquery.Select.(*sqlparser.Select).OrderBy[0].Expr
			require.Equal(t, tc.expected, st.DirectDeps(expr))
			require.Equal(t, tc.expected, st.RecursiveDeps(expr))
		})
	}
}

func TestOrderByBindingTable(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		"select col from tabl order by col",
		T1,
	}, {
		"select col from tabl order by tabl.col",
		T1,
	}, {
		"select col from tabl order by d.tabl.col",
		T1,
	}, {
		"select col from tabl order by 1",
		T1,
	}, {
		"select col as c from tabl order by c",
		T1,
	}, {
		"select 1 as c from tabl order by c",
		T0,
	}, {
		"select name, name from t1, t2 order by name",
		T2,
	}, {
		"(select id from t1) union (select uid from t2) order by id",
		T1 | T2,
	}, {
		"select id from t1 union (select uid from t2) order by 1",
		T1 | T2,
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")

			var order sqlparser.Expr
			switch stmt := stmt.(type) {
			case *sqlparser.Select:
				order = stmt.OrderBy[0].Expr
			case *sqlparser.Union:
				order = stmt.OrderBy[0].Expr
			default:
				t.Fail()
			}
			d := semTable.RecursiveDeps(order)
			require.Equal(t, tc.deps, d, tc.sql)
		})
	}
}

func TestGroupByBinding(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		"select col from tabl group by col",
		T1,
	}, {
		"select col from tabl group by tabl.col",
		T1,
	}, {
		"select col from tabl group by d.tabl.col",
		T1,
	}, {
		"select tabl.col as x from tabl group by x",
		T1,
	}, {
		"select tabl.col as x from tabl group by col",
		T1,
	}, {
		"select col from tabl group by 1",
		T1,
	}, {
		"select col as c from tabl group by c",
		T1,
	}, {
		"select 1 as c from tabl group by c",
		T0,
	}, {
		"select t1.id from t1, t2 group by id",
		T1,
	}, {
		"select id from t, t1 group by id",
		T2,
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel, _ := stmt.(*sqlparser.Select)
			grp := sel.GroupBy[0]
			d := semTable.RecursiveDeps(grp)
			require.Equal(t, tc.deps, d, tc.sql)
		})
	}
}

func TestHavingBinding(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		"select col from tabl having col = 1",
		T1,
	}, {
		"select col from tabl having tabl.col = 1",
		T1,
	}, {
		"select col from tabl having d.tabl.col = 1",
		T1,
	}, {
		"select tabl.col as x from tabl having x = 1",
		T1,
	}, {
		"select tabl.col as x from tabl having col",
		T1,
	}, {
		"select col from tabl having 1 = 1",
		T0,
	}, {
		"select col as c from tabl having c = 1",
		T1,
	}, {
		"select 1 as c from tabl having c = 1",
		T0,
	}, {
		"select t1.id from t1, t2 having id = 1",
		T1,
	}, {
		"select t.id from t, t1 having id = 1",
		T1,
	}, {
		"select t.id, count(*) as a from t, t1 group by t.id having a = 1",
		T1 | T2,
	}, {
		sql:  "select u2.a, u1.a from u1, u2 having u2.a = 2",
		deps: T2,
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel, _ := stmt.(*sqlparser.Select)
			hvng := sel.Having.Expr
			d := semTable.RecursiveDeps(hvng)
			require.Equal(t, tc.deps, d, tc.sql)
		})
	}
}

func TestUnion(t *testing.T) {
	query := "select col1 from tabl1 union select col2 from tabl2"

	stmt, semTable := parseAndAnalyze(t, query, "")
	union, _ := stmt.(*sqlparser.Union)
	sel1 := union.FirstStatement.(*sqlparser.Select)
	sel2 := union.UnionSelects[0].Statement.(*sqlparser.Select)

	t1 := sel1.From[0].(*sqlparser.AliasedTableExpr)
	t2 := sel2.From[0].(*sqlparser.AliasedTableExpr)
	ts1 := semTable.TableSetFor(t1)
	ts2 := semTable.TableSetFor(t2)
	assert.EqualValues(t, 1, ts1)
	assert.EqualValues(t, 2, ts2)

	d1 := semTable.RecursiveDeps(extract(sel1, 0))
	d2 := semTable.RecursiveDeps(extract(sel2, 0))
	assert.Equal(t, T1, d1)
	assert.Equal(t, T2, d2)
}

func TestUnionColumns(t *testing.T) {
	queries := []string{
		"select 1,2 union select 1",
		"(select 1,2 union select 3,4) union (select 5,6 union select 7)",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)

			_, err = Analyze(parse.(sqlparser.SelectStatement), "dbName", fakeSchemaInfo())

			require.Error(t, err)
		})
	}
}

func TestUnionWithOrderBy(t *testing.T) {
	query := "select col1 from tabl1 union (select col2 from tabl2) order by 1"

	stmt, semTable := parseAndAnalyze(t, query, "")
	union, _ := stmt.(*sqlparser.Union)
	sel1 := union.FirstStatement.(*sqlparser.Select)
	sel2 := union.UnionSelects[0].Statement.(*sqlparser.ParenSelect).Select.(*sqlparser.Select)

	t1 := sel1.From[0].(*sqlparser.AliasedTableExpr)
	t2 := sel2.From[0].(*sqlparser.AliasedTableExpr)
	ts1 := semTable.TableSetFor(t1)
	ts2 := semTable.TableSetFor(t2)
	assert.EqualValues(t, 1, ts1)
	assert.EqualValues(t, 2, ts2)

	d1 := semTable.RecursiveDeps(extract(sel1, 0))
	d2 := semTable.RecursiveDeps(extract(sel2, 0))
	assert.Equal(t, T1, d1)
	assert.Equal(t, T2, d2)
}

func TestScopingWDerivedTables(t *testing.T) {
	queries := []struct {
		query                string
		errorMessage         string
		recursiveExpectation TableSet
		expectation          TableSet
	}{
		{
			query:                "select id from (select x as id from user) as t",
			recursiveExpectation: T1,
			expectation:          T2,
		}, {
			query:                "select id from (select foo as id from user) as t",
			recursiveExpectation: T1,
			expectation:          T2,
		}, {
			query:                "select id from (select foo as id from (select x as foo from user) as c) as t",
			recursiveExpectation: T1,
			expectation:          T3,
		}, {
			query:                "select t.id from (select foo as id from user) as t",
			recursiveExpectation: T1,
			expectation:          T2,
		}, {
			query:        "select t.id2 from (select foo as id from user) as t",
			errorMessage: "symbol t.id2 not found",
		}, {
			query:                "select id from (select 42 as id) as t",
			recursiveExpectation: T0,
			expectation:          T2,
		}, {
			query:                "select t.id from (select 42 as id) as t",
			recursiveExpectation: T0,
			expectation:          T2,
		}, {
			query:        "select ks.t.id from (select 42 as id) as t",
			errorMessage: "symbol ks.t.id not found",
		}, {
			query:        "select * from (select id, id from user) as t",
			errorMessage: "Duplicate column name 'id'",
		}, {
			query:                "select t.baz = 1 from (select id as baz from user) as t",
			expectation:          T2,
			recursiveExpectation: T1,
		}, {
			query:                "select t.id from (select * from user, music) as t",
			expectation:          T3,
			recursiveExpectation: T1 | T2,
		}, {
			query:                "select t.id from (select * from user, music) as t order by t.id",
			expectation:          T3,
			recursiveExpectation: T1 | T2,
		}, {
			query:                "select t.id from (select * from user) as t join user as u on t.id = u.id",
			expectation:          T2,
			recursiveExpectation: T1,
		}, {
			query:                "select t.col1 from t3 ua join (select t1.id, t1.col1 from t1 join t2) as t",
			expectation:          T4,
			recursiveExpectation: T2,
		}, {
			query:        "select uu.test from (select id from t1) uu",
			errorMessage: "symbol uu.test not found",
		}, {
			query:        "select uu.id from (select id as col from t1) uu",
			errorMessage: "symbol uu.id not found",
		}, {
			query:        "select uu.id from (select id as col from t1) uu",
			errorMessage: "symbol uu.id not found",
		}, {
			query:                "select uu.id from (select id from t1) as uu where exists (select * from t2 as uu where uu.id = uu.uid)",
			expectation:          T2,
			recursiveExpectation: T1,
		}, {
			query:                "select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
			expectation:          T0,
			recursiveExpectation: T0,
		}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			st, err := Analyze(parse.(sqlparser.SelectStatement), "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewTableIdent("t")},
				},
			})
			if query.errorMessage != "" {
				require.EqualError(t, err, query.errorMessage)
			} else {
				require.NoError(t, err)
				sel := parse.(*sqlparser.Select)
				assert.Equal(t, query.recursiveExpectation, st.RecursiveDeps(extract(sel, 0)), "RecursiveDeps")
				assert.Equal(t, query.expectation, st.DirectDeps(extract(sel, 0)), "DirectDeps")
			}
		})
	}
}

func TestScopingWComplexDerivedTables(t *testing.T) {
	queries := []struct {
		query            string
		errorMessage     string
		rightExpectation TableSet
		leftExpectation  TableSet
	}{
		{
			query:            "select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
			rightExpectation: T1,
			leftExpectation:  T1,
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			st, err := Analyze(parse.(sqlparser.SelectStatement), "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewTableIdent("t")},
				},
			})
			if query.errorMessage != "" {
				require.EqualError(t, err, query.errorMessage)
			} else {
				require.NoError(t, err)
				sel := parse.(*sqlparser.Select)
				comparisonExpr := sel.Where.Expr.(*sqlparser.ExistsExpr).Subquery.Select.(*sqlparser.Select).Where.Expr.(*sqlparser.ExistsExpr).Subquery.Select.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr)
				left := comparisonExpr.Left
				right := comparisonExpr.Right
				assert.Equal(t, query.leftExpectation, st.RecursiveDeps(left), "Left RecursiveDeps")
				assert.Equal(t, query.rightExpectation, st.RecursiveDeps(right), "Right RecursiveDeps")
			}
		})
	}
}

func TestScopingWVindexTables(t *testing.T) {
	queries := []struct {
		query                string
		errorMessage         string
		recursiveExpectation TableSet
		expectation          TableSet
	}{
		{
			query:                "select id from user_index where id = 1",
			recursiveExpectation: T1,
			expectation:          T1,
		}, {
			query:                "select u.id + t.id from t as t join user_index as u where u.id = 1 and u.id = t.id",
			recursiveExpectation: T1 | T2,
			expectation:          T1 | T2,
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			hash, _ := vindexes.NewHash("user_index", nil)
			st, err := Analyze(parse.(sqlparser.SelectStatement), "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewTableIdent("t")},
				},
				VindexTables: map[string]vindexes.Vindex{
					"user_index": hash,
				},
			})
			if query.errorMessage != "" {
				require.EqualError(t, err, query.errorMessage)
			} else {
				require.NoError(t, err)
				sel := parse.(*sqlparser.Select)
				assert.Equal(t, query.recursiveExpectation, st.RecursiveDeps(extract(sel, 0)))
				assert.Equal(t, query.expectation, st.DirectDeps(extract(sel, 0)))
			}
		})
	}
}

func parseAndAnalyze(t *testing.T, query, dbName string) (sqlparser.Statement, *SemTable) {
	t.Helper()
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)

	semTable, err := Analyze(parse.(sqlparser.SelectStatement), dbName, fakeSchemaInfo())
	require.NoError(t, err)
	return parse, semTable
}

func fakeSchemaInfo() *FakeSI {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewColIdent("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewColIdent("uid"),
		Type: querypb.Type_INT64,
	}, {
		Name: sqlparser.NewColIdent("name"),
		Type: querypb.Type_VARCHAR,
	}}

	si := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t":  {Name: sqlparser.NewTableIdent("t")},
			"t1": {Name: sqlparser.NewTableIdent("t1"), Columns: cols1, ColumnListAuthoritative: true},
			"t2": {Name: sqlparser.NewTableIdent("t2"), Columns: cols2, ColumnListAuthoritative: true},
		},
	}
	return si
}
