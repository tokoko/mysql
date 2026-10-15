// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb/extensions"
	"google.golang.org/protobuf/proto"
)

// Function anchors declared by testPlan.
const (
	fnEqual uint32 = iota + 1
	fnGt
	fnAnd
	fnIsNull
	fnDivideInt
	fnLike
	fnUnknown
)

func testPlan(root *substraitpb.Rel, names ...string) *substraitpb.Plan {
	decl := func(anchor uint32, name string) *extensions.SimpleExtensionDeclaration {
		return &extensions.SimpleExtensionDeclaration{
			MappingType: &extensions.SimpleExtensionDeclaration_ExtensionFunction_{
				ExtensionFunction: &extensions.SimpleExtensionDeclaration_ExtensionFunction{
					ExtensionUrnReference: 1,
					FunctionAnchor:        anchor,
					Name:                  name,
				},
			},
		}
	}
	return &substraitpb.Plan{
		ExtensionUrns: []*extensions.SimpleExtensionURN{
			{ExtensionUrnAnchor: 1, Urn: "extension:io.substrait:functions_comparison"},
		},
		Extensions: []*extensions.SimpleExtensionDeclaration{
			decl(fnEqual, "equal:any_any"),
			decl(fnGt, "gt:any_any"),
			decl(fnAnd, "and:bool"),
			decl(fnIsNull, "is_null:any"),
			decl(fnDivideInt, "divide:i32_i32"),
			decl(fnLike, "like:str_str"),
			decl(fnUnknown, "frobnicate:any"),
		},
		Relations: []*substraitpb.PlanRel{{
			RelType: &substraitpb.PlanRel_Root{Root: &substraitpb.RelRoot{Input: root, Names: names}},
		}},
	}
}

func readRel(table []string, columns ...string) *substraitpb.ReadRel {
	types := make([]*substraitpb.Type, len(columns))
	for i := range types {
		types[i] = &substraitpb.Type{Kind: &substraitpb.Type_I32_{I32: &substraitpb.Type_I32{}}}
	}
	return &substraitpb.ReadRel{
		BaseSchema: &substraitpb.NamedStruct{
			Names:  columns,
			Struct: &substraitpb.Type_Struct{Types: types},
		},
		ReadType: &substraitpb.ReadRel_NamedTable_{NamedTable: &substraitpb.ReadRel_NamedTable{Names: table}},
	}
}

func read(r *substraitpb.ReadRel) *substraitpb.Rel {
	return &substraitpb.Rel{RelType: &substraitpb.Rel_Read{Read: r}}
}

func filter(input *substraitpb.Rel, cond *substraitpb.Expression) *substraitpb.Rel {
	return &substraitpb.Rel{RelType: &substraitpb.Rel_Filter{Filter: &substraitpb.FilterRel{Input: input, Condition: cond}}}
}

// fetch builds a FetchRel; a negative count means all rows.
func fetch(input *substraitpb.Rel, offset, count int64) *substraitpb.Rel {
	countExpr := i64(count)
	if count < 0 {
		countExpr = lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Null{Null: &substraitpb.Type{}}})
	}
	return &substraitpb.Rel{RelType: &substraitpb.Rel_Fetch{Fetch: &substraitpb.FetchRel{
		Input:      input,
		OffsetMode: &substraitpb.FetchRel_OffsetExpr{OffsetExpr: i64(offset)},
		CountMode:  &substraitpb.FetchRel_CountExpr{CountExpr: countExpr},
	}}}
}

func field(i int32) *substraitpb.Expression {
	return &substraitpb.Expression{RexType: &substraitpb.Expression_Selection{Selection: &substraitpb.Expression_FieldReference{
		ReferenceType: &substraitpb.Expression_FieldReference_DirectReference{
			DirectReference: &substraitpb.Expression_ReferenceSegment{
				ReferenceType: &substraitpb.Expression_ReferenceSegment_StructField_{
					StructField: &substraitpb.Expression_ReferenceSegment_StructField{Field: i},
				},
			},
		},
		RootType: &substraitpb.Expression_FieldReference_RootReference_{RootReference: &substraitpb.Expression_FieldReference_RootReference{}},
	}}}
}

func lit(l *substraitpb.Expression_Literal) *substraitpb.Expression {
	return &substraitpb.Expression{RexType: &substraitpb.Expression_Literal_{Literal: l}}
}

func i64(v int64) *substraitpb.Expression {
	return lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_I64{I64: v}})
}

func str(v string) *substraitpb.Expression {
	return lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_String_{String_: v}})
}

func call(anchor uint32, args ...*substraitpb.Expression) *substraitpb.Expression {
	fargs := make([]*substraitpb.FunctionArgument, len(args))
	for i, a := range args {
		fargs[i] = &substraitpb.FunctionArgument{ArgType: &substraitpb.FunctionArgument_Value{Value: a}}
	}
	return &substraitpb.Expression{RexType: &substraitpb.Expression_ScalarFunction_{ScalarFunction: &substraitpb.Expression_ScalarFunction{
		FunctionReference: anchor,
		Arguments:         fargs,
	}}}
}

func translateForTest(t *testing.T, plan *substraitpb.Plan) (string, error) {
	t.Helper()
	serialized, err := proto.Marshal(plan)
	require.NoError(t, err)
	return translateSubstraitPlan(serialized)
}

func requireStatus(t *testing.T, err error, code adbc.Status) {
	t.Helper()
	var se *substraitError
	require.True(t, errors.As(err, &se), "expected substraitError, got %v", err)
	assert.Equal(t, code, se.code, se.msg)
}

func TestSubstraitRelations(t *testing.T) {
	tbl := []string{"db", "people"}
	cols := []string{"id", "name", "age"}

	projected := readRel(tbl, cols...)
	projected.Projection = &substraitpb.Expression_MaskExpression{
		Select: &substraitpb.Expression_MaskExpression_StructSelect{
			StructItems: []*substraitpb.Expression_MaskExpression_StructItem{{Field: 2}, {Field: 0}},
		},
	}

	readFiltered := readRel(tbl, cols...)
	readFiltered.Filter = call(fnGt, field(2), i64(18))

	emitted := readRel(tbl, cols...)
	emitted.Common = &substraitpb.RelCommon{EmitKind: &substraitpb.RelCommon_Emit_{
		Emit: &substraitpb.RelCommon_Emit{OutputMapping: []int32{1}},
	}}

	for _, tc := range []struct {
		name string
		plan *substraitpb.Plan
		want string
	}{
		{
			name: "read",
			plan: testPlan(read(readRel(tbl, cols...)), "id", "name", "age"),
			want: "SELECT `id` AS `id`, `name` AS `name`, `age` AS `age` FROM `db`.`people`",
		},
		{
			name: "read without root names",
			plan: testPlan(read(readRel([]string{"people"}, cols...))),
			want: "SELECT `id`, `name`, `age` FROM `people`",
		},
		{
			name: "read with projection and renamed output",
			plan: testPlan(read(projected), "years", "key"),
			want: "SELECT `age` AS `years`, `id` AS `key` FROM `db`.`people`",
		},
		{
			name: "read with filter",
			plan: testPlan(read(readFiltered)),
			want: "SELECT `id`, `name`, `age` FROM `db`.`people` WHERE (`age` > 18)",
		},
		{
			name: "read with emit",
			plan: testPlan(read(emitted)),
			want: "SELECT `name` FROM `db`.`people`",
		},
		{
			name: "filter",
			plan: testPlan(filter(read(readRel(tbl, cols...)),
				call(fnAnd, call(fnEqual, field(1), str("O'Brien")), call(fnIsNull, field(2))))),
			want: "SELECT `id`, `name`, `age` FROM `db`.`people` WHERE ((`name` = 'O''Brien') AND (`age` IS NULL))",
		},
		{
			name: "stacked filters",
			plan: testPlan(filter(filter(read(readFiltered), call(fnGt, field(0), i64(1))), call(fnLike, field(1), str("A%")))),
			want: "SELECT `id`, `name`, `age` FROM `db`.`people` WHERE (`age` > 18) AND (`id` > 1) AND (`name` LIKE 'A%')",
		},
		{
			name: "limit",
			plan: testPlan(fetch(read(readRel(tbl, cols...)), 0, 10)),
			want: "SELECT `id`, `name`, `age` FROM `db`.`people` LIMIT 10",
		},
		{
			name: "limit and offset",
			plan: testPlan(fetch(read(readRel(tbl, cols...)), 5, 10)),
			want: "SELECT `id`, `name`, `age` FROM `db`.`people` LIMIT 10 OFFSET 5",
		},
		{
			name: "offset only",
			plan: testPlan(fetch(read(readRel(tbl, cols...)), 5, -1)),
			want: "SELECT `id`, `name`, `age` FROM `db`.`people` LIMIT 18446744073709551615 OFFSET 5",
		},
		{
			name: "no-op fetch",
			plan: testPlan(fetch(read(readRel(tbl, cols...)), 0, -1)),
			want: "SELECT `id`, `name`, `age` FROM `db`.`people`",
		},
		{
			name: "limit over filter",
			plan: testPlan(fetch(filter(read(readRel(tbl, cols...)), call(fnGt, field(2), i64(18))), 0, 3), "a", "b", "c"),
			want: "SELECT `id` AS `a`, `name` AS `b`, `age` AS `c` FROM `db`.`people` WHERE (`age` > 18) LIMIT 3",
		},
		{
			name: "filter over limit",
			plan: testPlan(filter(fetch(read(readRel(tbl, cols...)), 0, 3), call(fnGt, field(2), i64(18)))),
			want: "SELECT `_s1`.`_c0`, `_s1`.`_c1`, `_s1`.`_c2` FROM " +
				"(SELECT `id` AS `_c0`, `name` AS `_c1`, `age` AS `_c2` FROM `db`.`people` LIMIT 3) AS `_s1` " +
				"WHERE (`_s1`.`_c2` > 18)",
		},
		{
			name: "limit over limit",
			plan: testPlan(fetch(fetch(read(readRel(tbl, "id")), 0, 10), 2, 3)),
			want: "SELECT `_s1`.`_c0` FROM (SELECT `id` AS `_c0` FROM `db`.`people` LIMIT 10) AS `_s1` LIMIT 3 OFFSET 2",
		},
		{
			name: "quoted identifiers",
			plan: testPlan(read(readRel([]string{"we`ird"}, "co`l"))),
			want: "SELECT `co``l` FROM `we``ird`",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := translateForTest(t, tc.plan)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSubstraitExpressions(t *testing.T) {
	decimal := func(unscaled []byte, precision, scale int32) *substraitpb.Expression {
		value := make([]byte, 16)
		copy(value, unscaled)
		if len(unscaled) > 0 && unscaled[len(unscaled)-1]&0x80 != 0 {
			for i := len(unscaled); i < 16; i++ {
				value[i] = 0xff
			}
		}
		return lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Decimal_{
			Decimal: &substraitpb.Expression_Literal_Decimal{Value: value, Precision: precision, Scale: scale},
		}})
	}

	for _, tc := range []struct {
		name string
		expr *substraitpb.Expression
		want string
	}{
		{"string with backslash", str(`a\b`), "_utf8mb4 X'615c62'"},
		{"bool", lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Boolean{Boolean: true}}), "TRUE"},
		{"double", lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Fp64{Fp64: 1.5}}), "1.5e+00"},
		{"binary", lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Binary{Binary: []byte{0xde, 0xad}}}), "X'dead'"},
		{"null", lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Null{Null: &substraitpb.Type{}}}), "NULL"},
		{"date", lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Date{Date: 19723}}), "DATE '2024-01-01'"},
		{"timestamp", lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_PrecisionTimestamp_{
			PrecisionTimestamp: &substraitpb.Expression_Literal_PrecisionTimestamp{Precision: 3, Value: 1704067200123},
		}}), "TIMESTAMP '2024-01-01 00:00:00.123000'"},
		{"time", lit(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_PrecisionTime_{
			PrecisionTime: &substraitpb.Expression_Literal_PrecisionTime{Precision: 0, Value: 3723},
		}}), "TIME '01:02:03.000000'"},
		{"decimal", decimal([]byte{0x39, 0x30}, 5, 2), "123.45"},           // 12345
		{"small decimal", decimal([]byte{0x05}, 5, 3), "0.005"},            // 5
		{"negative decimal", decimal([]byte{0xc7, 0xcf}, 5, 2), "-123.45"}, // -12345
		{"integer division", call(fnDivideInt, field(0), i64(2)), "(`id` DIV 2)"},
		{"in list", &substraitpb.Expression{RexType: &substraitpb.Expression_SingularOrList_{SingularOrList: &substraitpb.Expression_SingularOrList{
			Value:   field(0),
			Options: []*substraitpb.Expression{i64(1), i64(2)},
		}}}, "(`id` IN (1, 2))"},
		{"case", &substraitpb.Expression{RexType: &substraitpb.Expression_IfThen_{IfThen: &substraitpb.Expression_IfThen{
			Ifs:  []*substraitpb.Expression_IfThen_IfClause{{If: call(fnIsNull, field(0)), Then: i64(0)}},
			Else: field(0),
		}}}, "(CASE WHEN (`id` IS NULL) THEN 0 ELSE `id` END)"},
		{"cast", &substraitpb.Expression{RexType: &substraitpb.Expression_Cast_{Cast: &substraitpb.Expression_Cast{
			Type:  &substraitpb.Type{Kind: &substraitpb.Type_Decimal_{Decimal: &substraitpb.Type_Decimal{Precision: 10, Scale: 2}}},
			Input: field(0),
		}}}, "CAST(`id` AS DECIMAL(10, 2))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rel := readRel([]string{"t"}, "id")
			rel.Filter = tc.expr
			got, err := translateForTest(t, testPlan(read(rel)))
			require.NoError(t, err)
			assert.Equal(t, "SELECT `id` FROM `t` WHERE "+tc.want, got)
		})
	}
}

func TestSubstraitErrors(t *testing.T) {
	cols := []string{"id"}
	tbl := []string{"t"}

	virtual := &substraitpb.ReadRel{
		BaseSchema: readRel(tbl, cols...).BaseSchema,
		ReadType:   &substraitpb.ReadRel_VirtualTable_{VirtualTable: &substraitpb.ReadRel_VirtualTable{}},
	}
	project := &substraitpb.Rel{RelType: &substraitpb.Rel_Project{Project: &substraitpb.ProjectRel{Input: read(readRel(tbl, cols...))}}}
	limitExpr := &substraitpb.Rel{RelType: &substraitpb.Rel_Fetch{Fetch: &substraitpb.FetchRel{
		Input:     read(readRel(tbl, cols...)),
		CountMode: &substraitpb.FetchRel_CountExpr{CountExpr: field(0)},
	}}}

	for _, tc := range []struct {
		name string
		plan *substraitpb.Plan
		code adbc.Status
	}{
		{"unsupported relation", testPlan(project), adbc.StatusNotImplemented},
		{"virtual table", testPlan(read(virtual)), adbc.StatusNotImplemented},
		{"non-literal limit", testPlan(limitExpr), adbc.StatusNotImplemented},
		{"unsupported function", testPlan(filter(read(readRel(tbl, cols...)), call(fnUnknown, field(0)))), adbc.StatusNotImplemented},
		{"undeclared function", testPlan(filter(read(readRel(tbl, cols...)), call(99, field(0)))), adbc.StatusInvalidArgument},
		{"field out of range", testPlan(filter(read(readRel(tbl, cols...)), call(fnIsNull, field(1)))), adbc.StatusInvalidArgument},
		{"too many name parts", testPlan(read(readRel([]string{"a", "b", "c"}, cols...))), adbc.StatusInvalidArgument},
		{"root names mismatch", testPlan(read(readRel(tbl, cols...)), "a", "b"), adbc.StatusInvalidArgument},
		{"empty plan", &substraitpb.Plan{}, adbc.StatusInvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := translateForTest(t, tc.plan)
			requireStatus(t, err, tc.code)
		})
	}

	t.Run("garbage bytes", func(t *testing.T) {
		_, err := translateSubstraitPlan([]byte{0xff, 0xff, 0xff})
		requireStatus(t, err, adbc.StatusInvalidArgument)
	})
}
