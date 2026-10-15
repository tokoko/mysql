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
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

// Substrait support translates a (small) subset of Substrait plans into MySQL
// SQL text, which is then executed like any other query. Supported relations:
//
//   - ReadRel over a NamedTable (with optional filter, best-effort filter and
//     projection mask)
//   - FilterRel
//   - FetchRel (Substrait's LIMIT/OFFSET relation)
//
// Expressions support field references, literals, a set of common scalar
// functions (comparison, boolean, arithmetic, string), IN lists, CASE and CAST.
// Anything else is rejected with ADBC_STATUS_NOT_IMPLEMENTED.

// substraitError carries the ADBC status code that should be reported for a
// failed translation.
type substraitError struct {
	code adbc.Status
	msg  string
}

func (e *substraitError) Error() string { return e.msg }

func substraitUnsupported(format string, args ...any) error {
	return &substraitError{code: adbc.StatusNotImplemented, msg: "substrait: " + fmt.Sprintf(format, args...)}
}

func substraitInvalid(format string, args ...any) error {
	return &substraitError{code: adbc.StatusInvalidArgument, msg: "substrait: " + fmt.Sprintf(format, args...)}
}

// mysqlMaxLimit is the documented way to express "no limit" together with an
// OFFSET in MySQL.
const mysqlMaxLimit = "18446744073709551615"

// translateSubstraitPlan converts a serialized Substrait Plan into a MySQL
// SELECT statement.
func translateSubstraitPlan(serialized []byte) (string, error) {
	var plan substraitpb.Plan
	if err := proto.Unmarshal(serialized, &plan); err != nil {
		return "", substraitInvalid("failed to parse plan: %v", err)
	}
	return translatePlan(&plan)
}

func translatePlan(plan *substraitpb.Plan) (string, error) {
	t := &substraitTranslator{functions: make(map[uint32]string)}
	for _, ext := range plan.GetExtensions() {
		if fn := ext.GetExtensionFunction(); fn != nil {
			t.functions[fn.GetFunctionAnchor()] = fn.GetName()
		}
	}

	var root *substraitpb.RelRoot
	for _, rel := range plan.GetRelations() {
		switch r := rel.GetRelType().(type) {
		case *substraitpb.PlanRel_Root:
			if root != nil {
				return "", substraitUnsupported("plans with multiple root relations are not supported")
			}
			root = r.Root
		case *substraitpb.PlanRel_Rel:
			return "", substraitUnsupported("plans with non-root relations are not supported")
		default:
			return "", substraitInvalid("plan relation has no type")
		}
	}
	if root == nil || root.GetInput() == nil {
		return "", substraitInvalid("plan has no root relation")
	}

	q, err := t.translateRel(root.GetInput())
	if err != nil {
		return "", err
	}

	names := root.GetNames()
	if len(names) > 0 && len(names) != len(q.columns) {
		return "", substraitInvalid("root has %d output names but the relation produces %d columns", len(names), len(q.columns))
	}
	return q.sql(names), nil
}

type substraitTranslator struct {
	// functions maps function anchors to their (compound) names, e.g. "equal:any_any".
	functions map[uint32]string
	// subqueries counts derived tables, to generate unique aliases.
	subqueries int
}

// relQuery is a single SELECT under construction. Columns hold the SQL for each
// output field of the relation, in order, so expressions referencing field i
// of the relation can simply substitute columns[i].
type relQuery struct {
	from     string
	columns  []string
	where    []string
	hasLimit bool
	limit    string
	offset   string
}

func (q *relQuery) sql(names []string) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	if len(q.columns) == 0 {
		// A relation with no output columns still produces rows.
		b.WriteString("1")
	}
	for i, col := range q.columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
		if names != nil {
			b.WriteString(" AS ")
			b.WriteString(quoteIdentifier(names[i]))
		}
	}
	b.WriteString(" FROM ")
	b.WriteString(q.from)
	if len(q.where) > 0 {
		b.WriteString(" WHERE ")
		b.WriteString(strings.Join(q.where, " AND "))
	}
	if q.hasLimit {
		b.WriteString(" LIMIT ")
		b.WriteString(q.limit)
		if q.offset != "" {
			b.WriteString(" OFFSET ")
			b.WriteString(q.offset)
		}
	}
	return b.String()
}

// wrap turns q into a derived table, so further clauses can be applied on top
// of it without changing its semantics (e.g. a filter above a limit).
func (t *substraitTranslator) wrap(q *relQuery) *relQuery {
	t.subqueries++
	alias := quoteIdentifier(fmt.Sprintf("_s%d", t.subqueries))
	names := make([]string, len(q.columns))
	columns := make([]string, len(q.columns))
	for i := range q.columns {
		names[i] = fmt.Sprintf("_c%d", i)
		columns[i] = alias + "." + quoteIdentifier(names[i])
	}
	return &relQuery{
		from:    "(" + q.sql(names) + ") AS " + alias,
		columns: columns,
	}
}

func (t *substraitTranslator) translateRel(rel *substraitpb.Rel) (*relQuery, error) {
	var (
		q      *relQuery
		common *substraitpb.RelCommon
		err    error
	)
	switch r := rel.GetRelType().(type) {
	case *substraitpb.Rel_Read:
		common = r.Read.GetCommon()
		q, err = t.translateRead(r.Read)
	case *substraitpb.Rel_Filter:
		common = r.Filter.GetCommon()
		q, err = t.translateFilter(r.Filter)
	case *substraitpb.Rel_Fetch:
		common = r.Fetch.GetCommon()
		q, err = t.translateFetch(r.Fetch)
	case nil:
		return nil, substraitInvalid("relation has no type")
	default:
		return nil, substraitUnsupported("relation type %T is not supported", r)
	}
	if err != nil {
		return nil, err
	}
	return applyEmit(q, common)
}

func applyEmit(q *relQuery, common *substraitpb.RelCommon) (*relQuery, error) {
	emit := common.GetEmit()
	if emit == nil {
		return q, nil
	}
	columns := make([]string, len(emit.GetOutputMapping()))
	for i, idx := range emit.GetOutputMapping() {
		if idx < 0 || int(idx) >= len(q.columns) {
			return nil, substraitInvalid("emit references field %d but the relation has %d fields", idx, len(q.columns))
		}
		columns[i] = q.columns[idx]
	}
	q.columns = columns
	return q, nil
}

func (t *substraitTranslator) translateRead(read *substraitpb.ReadRel) (*relQuery, error) {
	named := read.GetNamedTable()
	if named == nil {
		return nil, substraitUnsupported("only ReadRel with a named table is supported, got %T", read.GetReadType())
	}

	from, err := tableReference(named.GetNames())
	if err != nil {
		return nil, err
	}

	schema := read.GetBaseSchema()
	if schema == nil {
		return nil, substraitInvalid("ReadRel is missing base_schema")
	}
	// MySQL has no nested types, so every name is a top-level column.
	if n := len(schema.GetStruct().GetTypes()); schema.GetStruct() != nil && n != len(schema.GetNames()) {
		return nil, substraitUnsupported("ReadRel base_schema with nested types is not supported")
	}
	columns := make([]string, len(schema.GetNames()))
	for i, name := range schema.GetNames() {
		columns[i] = quoteIdentifier(name)
	}

	q := &relQuery{from: from, columns: columns}
	// Filters are evaluated against the base schema, before projection. The
	// best-effort filter may be applied exactly, so treat it like the filter.
	for _, cond := range []*substraitpb.Expression{read.GetFilter(), read.GetBestEffortFilter()} {
		if cond == nil {
			continue
		}
		sql, err := t.translateExpr(cond, columns)
		if err != nil {
			return nil, err
		}
		q.where = append(q.where, sql)
	}

	if proj := read.GetProjection(); proj != nil {
		items := proj.GetSelect().GetStructItems()
		projected := make([]string, len(items))
		for i, item := range items {
			if item.GetChild() != nil {
				return nil, substraitUnsupported("nested projection in ReadRel is not supported")
			}
			if item.GetField() < 0 || int(item.GetField()) >= len(columns) {
				return nil, substraitInvalid("projection references field %d but the table has %d fields", item.GetField(), len(columns))
			}
			projected[i] = columns[item.GetField()]
		}
		q.columns = projected
	}
	return q, nil
}

// tableReference renders a NamedTable's names. MySQL only has databases
// (catalogs) and tables, so accept [table] or [database, table].
func tableReference(names []string) (string, error) {
	switch len(names) {
	case 1:
		return quoteIdentifier(names[0]), nil
	case 2:
		return quoteIdentifier(names[0]) + "." + quoteIdentifier(names[1]), nil
	default:
		return "", substraitInvalid("named table must have 1 or 2 name parts (table or database.table), got %d", len(names))
	}
}

func (t *substraitTranslator) translateFilter(filter *substraitpb.FilterRel) (*relQuery, error) {
	if filter.GetInput() == nil {
		return nil, substraitInvalid("FilterRel is missing input")
	}
	if filter.GetCondition() == nil {
		return nil, substraitInvalid("FilterRel is missing condition")
	}
	q, err := t.translateRel(filter.GetInput())
	if err != nil {
		return nil, err
	}
	if q.hasLimit {
		// Filtering must happen after the limit, not before.
		q = t.wrap(q)
	}
	cond, err := t.translateExpr(filter.GetCondition(), q.columns)
	if err != nil {
		return nil, err
	}
	q.where = append(q.where, cond)
	return q, nil
}

func (t *substraitTranslator) translateFetch(fetch *substraitpb.FetchRel) (*relQuery, error) {
	if fetch.GetInput() == nil {
		return nil, substraitInvalid("FetchRel is missing input")
	}

	var (
		offset int64
		err    error
	)
	switch o := fetch.GetOffsetMode().(type) {
	case nil:
	case *substraitpb.FetchRel_Offset:
		offset = o.Offset
	case *substraitpb.FetchRel_OffsetExpr:
		var isNull bool
		offset, isNull, err = integerLiteral(o.OffsetExpr, "offset")
		if err != nil {
			return nil, err
		}
		if isNull {
			offset = 0
		}
	}
	if offset < 0 {
		return nil, substraitInvalid("FetchRel offset must be non-negative, got %d", offset)
	}

	count := int64(-1) // -1 means all rows
	switch c := fetch.GetCountMode().(type) {
	case nil:
	case *substraitpb.FetchRel_Count:
		count = c.Count
		if count < -1 {
			return nil, substraitInvalid("FetchRel count must be non-negative or -1, got %d", count)
		}
	case *substraitpb.FetchRel_CountExpr:
		var isNull bool
		count, isNull, err = integerLiteral(c.CountExpr, "count")
		if err != nil {
			return nil, err
		}
		if isNull {
			count = -1
		} else if count < 0 {
			return nil, substraitInvalid("FetchRel count must be non-negative, got %d", count)
		}
	}

	q, err := t.translateRel(fetch.GetInput())
	if err != nil {
		return nil, err
	}
	if offset == 0 && count == -1 {
		return q, nil
	}
	if q.hasLimit {
		q = t.wrap(q)
	}
	q.hasLimit = true
	if count == -1 {
		q.limit = mysqlMaxLimit
	} else {
		q.limit = strconv.FormatInt(count, 10)
	}
	if offset > 0 {
		q.offset = strconv.FormatInt(offset, 10)
	}
	return q, nil
}

// integerLiteral evaluates a FetchRel offset/count expression. MySQL's LIMIT
// only accepts constants, so the expression must be an integer literal.
func integerLiteral(expr *substraitpb.Expression, what string) (value int64, isNull bool, err error) {
	lit := expr.GetLiteral()
	if lit == nil {
		return 0, false, substraitUnsupported("FetchRel %s must be an integer literal", what)
	}
	switch v := lit.GetLiteralType().(type) {
	case *substraitpb.Expression_Literal_I8:
		return int64(v.I8), false, nil
	case *substraitpb.Expression_Literal_I16:
		return int64(v.I16), false, nil
	case *substraitpb.Expression_Literal_I32:
		return int64(v.I32), false, nil
	case *substraitpb.Expression_Literal_I64:
		return v.I64, false, nil
	case *substraitpb.Expression_Literal_Null:
		return 0, true, nil
	default:
		return 0, false, substraitUnsupported("FetchRel %s must be an integer literal, got %T", what, v)
	}
}

// translateExpr renders an expression. input holds the SQL for each field of
// the relation the expression is evaluated against.
func (t *substraitTranslator) translateExpr(expr *substraitpb.Expression, input []string) (string, error) {
	switch e := expr.GetRexType().(type) {
	case *substraitpb.Expression_Literal_:
		return translateLiteral(e.Literal)
	case *substraitpb.Expression_Selection:
		return translateFieldReference(e.Selection, input)
	case *substraitpb.Expression_ScalarFunction_:
		return t.translateScalarFunction(e.ScalarFunction, input)
	case *substraitpb.Expression_SingularOrList_:
		return t.translateInList(e.SingularOrList, input)
	case *substraitpb.Expression_IfThen_:
		return t.translateIfThen(e.IfThen, input)
	case *substraitpb.Expression_SwitchExpression_:
		return t.translateSwitch(e.SwitchExpression, input)
	case *substraitpb.Expression_Cast_:
		return t.translateCast(e.Cast, input)
	case nil:
		return "", substraitInvalid("expression has no type")
	default:
		return "", substraitUnsupported("expression type %T is not supported", e)
	}
}

func (t *substraitTranslator) translateExprs(exprs []*substraitpb.Expression, input []string) ([]string, error) {
	out := make([]string, len(exprs))
	for i, expr := range exprs {
		sql, err := t.translateExpr(expr, input)
		if err != nil {
			return nil, err
		}
		out[i] = sql
	}
	return out, nil
}

func translateFieldReference(ref *substraitpb.Expression_FieldReference, input []string) (string, error) {
	switch ref.GetRootType().(type) {
	case *substraitpb.Expression_FieldReference_RootReference_, nil:
	default:
		return "", substraitUnsupported("field reference root %T is not supported", ref.GetRootType())
	}
	field := ref.GetDirectReference().GetStructField()
	if field == nil {
		return "", substraitUnsupported("only direct struct field references are supported")
	}
	if field.GetChild() != nil {
		return "", substraitUnsupported("nested field references are not supported")
	}
	idx := field.GetField()
	if idx < 0 || int(idx) >= len(input) {
		return "", substraitInvalid("field reference %d out of range for %d input fields", idx, len(input))
	}
	return input[idx], nil
}

func translateLiteral(lit *substraitpb.Expression_Literal) (string, error) {
	switch v := lit.GetLiteralType().(type) {
	case *substraitpb.Expression_Literal_Null:
		return "NULL", nil
	case *substraitpb.Expression_Literal_Boolean:
		if v.Boolean {
			return "TRUE", nil
		}
		return "FALSE", nil
	case *substraitpb.Expression_Literal_I8:
		return strconv.FormatInt(int64(v.I8), 10), nil
	case *substraitpb.Expression_Literal_I16:
		return strconv.FormatInt(int64(v.I16), 10), nil
	case *substraitpb.Expression_Literal_I32:
		return strconv.FormatInt(int64(v.I32), 10), nil
	case *substraitpb.Expression_Literal_I64:
		return strconv.FormatInt(v.I64, 10), nil
	case *substraitpb.Expression_Literal_Fp32:
		return floatLiteral(float64(v.Fp32), 32)
	case *substraitpb.Expression_Literal_Fp64:
		return floatLiteral(v.Fp64, 64)
	case *substraitpb.Expression_Literal_String_:
		return stringLiteral(v.String_), nil
	case *substraitpb.Expression_Literal_VarChar_:
		return stringLiteral(v.VarChar.GetValue()), nil
	case *substraitpb.Expression_Literal_FixedChar:
		return stringLiteral(v.FixedChar), nil
	case *substraitpb.Expression_Literal_Binary:
		return "X'" + hex.EncodeToString(v.Binary) + "'", nil
	case *substraitpb.Expression_Literal_FixedBinary:
		return "X'" + hex.EncodeToString(v.FixedBinary) + "'", nil
	case *substraitpb.Expression_Literal_Decimal_:
		return decimalLiteral(v.Decimal)
	case *substraitpb.Expression_Literal_Date:
		d := time.Unix(int64(v.Date)*86400, 0).UTC()
		return "DATE '" + d.Format("2006-01-02") + "'", nil
	case *substraitpb.Expression_Literal_Timestamp:
		return timestampLiteral(v.Timestamp, 6)
	case *substraitpb.Expression_Literal_TimestampTz:
		return timestampLiteral(v.TimestampTz, 6)
	case *substraitpb.Expression_Literal_PrecisionTimestamp_:
		return timestampLiteral(v.PrecisionTimestamp.GetValue(), v.PrecisionTimestamp.GetPrecision())
	case *substraitpb.Expression_Literal_PrecisionTimestampTz:
		// The driver pins the session time zone to UTC, so this is the same instant.
		return timestampLiteral(v.PrecisionTimestampTz.GetValue(), v.PrecisionTimestampTz.GetPrecision())
	case *substraitpb.Expression_Literal_Time:
		return timeLiteral(v.Time, 6)
	case *substraitpb.Expression_Literal_PrecisionTime_:
		return timeLiteral(v.PrecisionTime.GetValue(), v.PrecisionTime.GetPrecision())
	case nil:
		return "", substraitInvalid("literal has no type")
	default:
		return "", substraitUnsupported("literal type %T is not supported", v)
	}
}

func floatLiteral(f float64, bits int) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", substraitUnsupported("MySQL cannot represent the floating point literal %v", f)
	}
	// Exponent notation makes MySQL treat the literal as a DOUBLE rather than a DECIMAL.
	return strconv.FormatFloat(f, 'e', -1, bits), nil
}

// stringLiteral quotes s as a MySQL string literal. Backslashes are only
// escapes when NO_BACKSLASH_ESCAPES is off, so strings that contain them (or
// other characters needing escapes) are sent as hex with a charset introducer,
// which is interpreted the same way under every sql_mode.
func stringLiteral(s string) string {
	plain := utf8.ValidString(s)
	for i := 0; plain && i < len(s); i++ {
		if c := s[i]; c == '\\' || c < 0x20 || c == 0x7f {
			plain = false
		}
	}
	if !plain {
		return "_utf8mb4 X'" + hex.EncodeToString([]byte(s)) + "'"
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// decimalLiteral renders a Substrait decimal (16-byte little-endian two's
// complement unscaled value) as an exact numeric literal.
func decimalLiteral(d *substraitpb.Expression_Literal_Decimal) (string, error) {
	raw := d.GetValue()
	if len(raw) != 16 {
		return "", substraitInvalid("decimal literal must be 16 bytes, got %d", len(raw))
	}
	be := make([]byte, 16)
	for i := range raw {
		be[15-i] = raw[i]
	}
	unscaled := new(big.Int).SetBytes(be)
	if be[0]&0x80 != 0 {
		unscaled.Sub(unscaled, new(big.Int).Lsh(big.NewInt(1), 128))
	}

	scale := int(d.GetScale())
	if scale < 0 {
		return "", substraitInvalid("decimal literal scale must be non-negative, got %d", scale)
	}
	digits := new(big.Int).Abs(unscaled).String()
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if unscaled.Sign() < 0 {
		digits = "-" + digits
	}
	return digits, nil
}

// splitSubseconds converts a value in units of 10^-precision seconds into
// whole seconds and microseconds, as MySQL supports at most microsecond
// precision.
func splitSubseconds(value int64, precision int32) (secs, micros int64, err error) {
	if precision < 0 || precision > 12 {
		return 0, 0, substraitInvalid("unsupported temporal precision %d", precision)
	}
	unit := int64(math.Pow10(int(precision)))
	secs, frac := value/unit, value%unit
	if frac < 0 {
		secs--
		frac += unit
	}
	if precision <= 6 {
		micros = frac * int64(math.Pow10(6-int(precision)))
	} else {
		div := int64(math.Pow10(int(precision) - 6))
		if frac%div != 0 {
			return 0, 0, substraitUnsupported("MySQL supports at most microsecond precision")
		}
		micros = frac / div
	}
	return secs, micros, nil
}

func timestampLiteral(value int64, precision int32) (string, error) {
	secs, micros, err := splitSubseconds(value, precision)
	if err != nil {
		return "", err
	}
	ts := time.Unix(secs, micros*1000).UTC()
	return "TIMESTAMP '" + ts.Format("2006-01-02 15:04:05.000000") + "'", nil
}

func timeLiteral(value int64, precision int32) (string, error) {
	secs, micros, err := splitSubseconds(value, precision)
	if err != nil {
		return "", err
	}
	if secs < 0 || secs >= 86400 {
		return "", substraitInvalid("time literal out of range")
	}
	return fmt.Sprintf("TIME '%02d:%02d:%02d.%06d'", secs/3600, secs/60%60, secs%60, micros), nil
}

// Binary operators by Substrait function name.
var substraitBinaryOperators = map[string]string{
	"equal":                "=",
	"not_equal":            "<>",
	"lt":                   "<",
	"lte":                  "<=",
	"gt":                   ">",
	"gte":                  ">=",
	"is_not_distinct_from": "<=>",
	"add":                  "+",
	"subtract":             "-",
	"multiply":             "*",
	"like":                 "LIKE",
	"xor":                  "XOR",
}

// Postfix predicates by Substrait function name.
var substraitPostfixOperators = map[string]string{
	"is_null":      "IS NULL",
	"is_not_null":  "IS NOT NULL",
	"is_true":      "IS TRUE",
	"is_not_true":  "IS NOT TRUE",
	"is_false":     "IS FALSE",
	"is_not_false": "IS NOT FALSE",
}

// Functions that map onto a MySQL function of the same arity.
var substraitPlainFunctions = map[string]string{
	"abs":         "ABS",
	"coalesce":    "COALESCE",
	"concat":      "CONCAT",
	"lower":       "LOWER",
	"upper":       "UPPER",
	"char_length": "CHAR_LENGTH",
	"substring":   "SUBSTRING",
	"ltrim":       "LTRIM",
	"rtrim":       "RTRIM",
	"trim":        "TRIM",
	"ceil":        "CEIL",
	"floor":       "FLOOR",
	"round":       "ROUND",
	"sqrt":        "SQRT",
	"power":       "POWER",
	"exp":         "EXP",
	"ln":          "LN",
	"sign":        "SIGN",
}

func (t *substraitTranslator) translateScalarFunction(fn *substraitpb.Expression_ScalarFunction, input []string) (string, error) {
	signature, ok := t.functions[fn.GetFunctionReference()]
	if !ok {
		return "", substraitInvalid("function reference %d is not declared in the plan", fn.GetFunctionReference())
	}
	name, _, _ := strings.Cut(signature, ":")

	var args []string
	if len(fn.GetArguments()) > 0 {
		for _, arg := range fn.GetArguments() {
			value := arg.GetValue()
			if value == nil {
				return "", substraitUnsupported("function %s: only value arguments are supported", name)
			}
			sql, err := t.translateExpr(value, input)
			if err != nil {
				return "", err
			}
			args = append(args, sql)
		}
	} else {
		var err error
		if args, err = t.translateExprs(fn.GetArgs(), input); err != nil {
			return "", err
		}
	}

	arity := func(n int) error {
		if len(args) != n {
			return substraitInvalid("function %s expects %d arguments, got %d", name, n, len(args))
		}
		return nil
	}

	if op, ok := substraitBinaryOperators[name]; ok {
		if err := arity(2); err != nil {
			return "", err
		}
		return "(" + args[0] + " " + op + " " + args[1] + ")", nil
	}
	if op, ok := substraitPostfixOperators[name]; ok {
		if err := arity(1); err != nil {
			return "", err
		}
		return "(" + args[0] + " " + op + ")", nil
	}
	if f, ok := substraitPlainFunctions[name]; ok {
		return f + "(" + strings.Join(args, ", ") + ")", nil
	}

	switch name {
	case "and", "or":
		if len(args) == 0 {
			// Identity elements for the variadic boolean operators.
			if name == "and" {
				return "TRUE", nil
			}
			return "FALSE", nil
		}
		return "(" + strings.Join(args, " "+strings.ToUpper(name)+" ") + ")", nil
	case "not":
		if err := arity(1); err != nil {
			return "", err
		}
		return "(NOT " + args[0] + ")", nil
	case "negate":
		if err := arity(1); err != nil {
			return "", err
		}
		return "(-" + args[0] + ")", nil
	case "is_distinct_from":
		if err := arity(2); err != nil {
			return "", err
		}
		return "(NOT (" + args[0] + " <=> " + args[1] + "))", nil
	case "between":
		if err := arity(3); err != nil {
			return "", err
		}
		return "(" + args[0] + " BETWEEN " + args[1] + " AND " + args[2] + ")", nil
	case "divide":
		if err := arity(2); err != nil {
			return "", err
		}
		// Substrait integer division truncates; MySQL's / always produces a
		// non-integer result, so use DIV for integers.
		if isIntegerType(fn.GetOutputType()) || (fn.GetOutputType() == nil && hasIntegerArgs(signature)) {
			return "(" + args[0] + " DIV " + args[1] + ")", nil
		}
		return "(" + args[0] + " / " + args[1] + ")", nil
	case "modulus":
		if err := arity(2); err != nil {
			return "", err
		}
		return "MOD(" + args[0] + ", " + args[1] + ")", nil
	}
	return "", substraitUnsupported("function %q is not supported", signature)
}

func isIntegerType(typ *substraitpb.Type) bool {
	switch typ.GetKind().(type) {
	case *substraitpb.Type_I8_, *substraitpb.Type_I16_, *substraitpb.Type_I32_, *substraitpb.Type_I64_:
		return true
	}
	return false
}

// hasIntegerArgs inspects a compound function name such as "divide:i32_i32".
func hasIntegerArgs(signature string) bool {
	_, params, ok := strings.Cut(signature, ":")
	if !ok {
		return false
	}
	for p := range strings.SplitSeq(params, "_") {
		switch p {
		case "i8", "i16", "i32", "i64":
		default:
			return false
		}
	}
	return true
}

func (t *substraitTranslator) translateInList(in *substraitpb.Expression_SingularOrList, input []string) (string, error) {
	if in.GetValue() == nil {
		return "", substraitInvalid("SingularOrList is missing value")
	}
	if len(in.GetOptions()) == 0 {
		return "FALSE", nil
	}
	value, err := t.translateExpr(in.GetValue(), input)
	if err != nil {
		return "", err
	}
	options, err := t.translateExprs(in.GetOptions(), input)
	if err != nil {
		return "", err
	}
	return "(" + value + " IN (" + strings.Join(options, ", ") + "))", nil
}

func (t *substraitTranslator) translateIfThen(ifThen *substraitpb.Expression_IfThen, input []string) (string, error) {
	if len(ifThen.GetIfs()) == 0 {
		return "", substraitInvalid("IfThen has no clauses")
	}
	var b strings.Builder
	b.WriteString("(CASE")
	for _, clause := range ifThen.GetIfs() {
		cond, err := t.translateExpr(clause.GetIf(), input)
		if err != nil {
			return "", err
		}
		then, err := t.translateExpr(clause.GetThen(), input)
		if err != nil {
			return "", err
		}
		b.WriteString(" WHEN " + cond + " THEN " + then)
	}
	if ifThen.GetElse() != nil {
		els, err := t.translateExpr(ifThen.GetElse(), input)
		if err != nil {
			return "", err
		}
		b.WriteString(" ELSE " + els)
	}
	b.WriteString(" END)")
	return b.String(), nil
}

func (t *substraitTranslator) translateSwitch(sw *substraitpb.Expression_SwitchExpression, input []string) (string, error) {
	if len(sw.GetIfs()) == 0 {
		return "", substraitInvalid("SwitchExpression has no clauses")
	}
	match, err := t.translateExpr(sw.GetMatch(), input)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("(CASE " + match)
	for _, clause := range sw.GetIfs() {
		when, err := translateLiteral(clause.GetIf())
		if err != nil {
			return "", err
		}
		then, err := t.translateExpr(clause.GetThen(), input)
		if err != nil {
			return "", err
		}
		b.WriteString(" WHEN " + when + " THEN " + then)
	}
	if sw.GetElse() != nil {
		els, err := t.translateExpr(sw.GetElse(), input)
		if err != nil {
			return "", err
		}
		b.WriteString(" ELSE " + els)
	}
	b.WriteString(" END)")
	return b.String(), nil
}

func (t *substraitTranslator) translateCast(cast *substraitpb.Expression_Cast, input []string) (string, error) {
	// MySQL's CAST does not honor the failure behavior: invalid input typically
	// produces a warning and a best-effort value rather than an error or NULL.
	target, err := castTarget(cast.GetType())
	if err != nil {
		return "", err
	}
	value, err := t.translateExpr(cast.GetInput(), input)
	if err != nil {
		return "", err
	}
	return "CAST(" + value + " AS " + target + ")", nil
}

func castTarget(typ *substraitpb.Type) (string, error) {
	switch k := typ.GetKind().(type) {
	case *substraitpb.Type_I8_, *substraitpb.Type_I16_, *substraitpb.Type_I32_, *substraitpb.Type_I64_:
		return "SIGNED", nil
	case *substraitpb.Type_Fp32:
		return "FLOAT", nil
	case *substraitpb.Type_Fp64:
		return "DOUBLE", nil
	case *substraitpb.Type_String_:
		return "CHAR", nil
	case *substraitpb.Type_Varchar:
		return fmt.Sprintf("CHAR(%d)", k.Varchar.GetLength()), nil
	case *substraitpb.Type_FixedChar_:
		return fmt.Sprintf("CHAR(%d)", k.FixedChar.GetLength()), nil
	case *substraitpb.Type_Binary_:
		return "BINARY", nil
	case *substraitpb.Type_FixedBinary_:
		return fmt.Sprintf("BINARY(%d)", k.FixedBinary.GetLength()), nil
	case *substraitpb.Type_Decimal_:
		return fmt.Sprintf("DECIMAL(%d, %d)", k.Decimal.GetPrecision(), k.Decimal.GetScale()), nil
	case *substraitpb.Type_Date_:
		return "DATE", nil
	case *substraitpb.Type_PrecisionTimestamp_:
		return fmt.Sprintf("DATETIME(%d)", min(k.PrecisionTimestamp.GetPrecision(), 6)), nil
	case *substraitpb.Type_PrecisionTime_:
		return fmt.Sprintf("TIME(%d)", min(k.PrecisionTime.GetPrecision(), 6)), nil
	case nil:
		return "", substraitInvalid("cast is missing a target type")
	default:
		return "", substraitUnsupported("CAST to %T is not supported", k)
	}
}
