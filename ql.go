// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//LATER profile mem
//LATER profile cpu
//LATER coverage

//MAYBE CROSSJOIN (explicit form), LEFT JOIN, INNER JOIN, OUTER JOIN equivalents.

package ql

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cznic/b"
	"github.com/cznic/strutil"
)

const (
	leftJoin = iota
	rightJoin
	fullJoin
)

// NOTE: all rset implementations must be safe for concurrent use by multiple
// goroutines.  If the do method requires any execution domain local data, they
// must be held out of the implementing instance.
var (
	_ rset = (*crossJoinRset)(nil)
	_ rset = (*distinctRset)(nil)
	_ rset = (*groupByRset)(nil)
	_ rset = (*limitRset)(nil)
	_ rset = (*offsetRset)(nil)
	_ rset = (*orderByRset)(nil)
	_ rset = (*outerJoinRset)(nil)
	_ rset = (*selectRset)(nil)
	_ rset = (*selectStmt)(nil)
	_ rset = (*tableRset)(nil)
	_ rset = (*whereRset)(nil)

	//_ rset2 = (*selectStmt2)(nil) // There's no selectStmt2.
	_ rset2 = (*crossJoinRset2)(nil)
	_ rset2 = (*distinctRset2)(nil)
	_ rset2 = (*groupByRset2)(nil)
	_ rset2 = (*leftJoinRset2)(nil)
	_ rset2 = (*limitRset2)(nil)
	_ rset2 = (*offsetRset2)(nil)
	_ rset2 = (*orderByRset2)(nil)
	_ rset2 = (*selectRset2)(nil)
	_ rset2 = (*tableRset2)(nil)
	_ rset2 = (*whereRset2)(nil)

	isTesting bool // enables test hook: select from an index
)

type rset interface {
	//do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) error
	plan(ctx *execCtx) (rset2, error)
}

type rset2 interface {
	do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error
	fieldNames() []string
}

type recordset struct {
	ctx *execCtx
	rset2
	tx *TCtx
}

func (r recordset) fieldNames() []interface{} {
	f := r.rset2.fieldNames()
	a := make([]interface{}, len(f))
	for i, v := range f {
		a[i] = v
	}
	return a
}

// Do implements Recordset.
func (r recordset) Do(names bool, f func(data []interface{}) (bool, error)) error {
	if names {
		if more, err := f(r.fieldNames()); err != nil || !more {
			return err
		}
	}
	return r.ctx.db.do(r, f)
}

// Fields implements Recordset.
func (r recordset) Fields() (names []string, err error) {
	return r.rset2.fieldNames(), nil
}

// FirstRow implements Recordset.
func (r recordset) FirstRow() (row []interface{}, err error) {
	rows, err := r.Rows(1, 0)
	if err != nil {
		return nil, err
	}

	if len(rows) != 0 {
		return rows[0], nil
	}

	return nil, nil
}

// Rows implements Recordset.
func (r recordset) Rows(limit, offset int) ([][]interface{}, error) {
	var rows [][]interface{}
	if err := r.Do(false, func(row []interface{}) (bool, error) {
		if offset > 0 {
			offset--
			return true, nil
		}

		switch {
		case limit < 0:
			rows = append(rows, row)
			return true, nil
		case limit == 0:
			return false, nil
		default: // limit > 0
			rows = append(rows, row)
			limit--
			return limit > 0, nil
		}
	}); err != nil {
		return nil, err
	}

	return rows, nil
}

// List represents a group of compiled statements.
type List struct {
	l      []stmt
	params int
}

// String implements fmt.Stringer
func (l List) String() string {
	var b bytes.Buffer
	f := strutil.IndentFormatter(&b, "\t")
	for _, s := range l.l {
		switch s.(type) {
		case beginTransactionStmt:
			f.Format("%s\n%i", s)
		case commitStmt, rollbackStmt:
			f.Format("%u%s\n", s)
		default:
			f.Format("%s\n", s)
		}
	}
	return b.String()
}

type groupByRset struct {
	colNames []string
	src      rset2
}

func (r *groupByRset) plan(ctx *execCtx) (rset2, error) {
	return &groupByRset2{colNames: r.colNames, src: r.src, fields: r.src.fieldNames()}, nil
}

type groupByRset2 struct {
	colNames []string
	src      rset2
	fields   []string
}

func (r *groupByRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t, err := ctx.db.store.CreateTemp(true)
	if err != nil {
		return
	}

	defer func() {
		if derr := t.Drop(); derr != nil && err == nil {
			err = derr
		}
	}()

	var gcols []*col
	var cols []*col
	m := map[string]int{}
	for i, v := range r.src.fieldNames() {
		m[v] = i
	}
	for _, c := range r.colNames {
		i, ok := m[c]
		if !ok {
			return fmt.Errorf("unknown column %s", c)
		}

		gcols = append(gcols, &col{name: c, index: i})
	}
	k := make([]interface{}, len(r.colNames)) //LATER optimize when len(r.cols) == 0
	if err = r.src.do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
		infer(in, &cols)
		for i, c := range gcols {
			k[i] = in[c.index]
		}
		h0, err := t.Get(k)
		if err != nil {
			return false, err
		}

		var h int64
		if len(h0) != 0 {
			h, _ = h0[0].(int64)
		}
		nh, err := t.Create(append([]interface{}{h, nil}, in...)...)
		if err != nil {
			return false, err
		}

		for i, c := range gcols {
			k[i] = in[c.index]
		}
		err = t.Set(k, []interface{}{nh})
		if err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
		return
	}

	for i, v := range r.src.fieldNames()[:len(cols)] {
		cols[i].name = v
		cols[i].index = i
	}
	if more, err := f(nil, []interface{}{t, cols}); !more || err != nil {
		return err
	}

	it, err := t.SeekFirst()
	more := true
	var data []interface{}
	for more && err == nil {
		if _, data, err = it.Next(); err != nil {
			break
		}

		more, err = f(nil, data)
	}
	return noEOF(err)
}

func (r *groupByRset2) fieldNames() []string { return r.fields }

// TCtx represents transaction context. It enables to execute multiple
// statement lists in the same context. The same context guarantees the state
// of the DB cannot change in between the separated executions.
//
// LastInsertID
//
// LastInsertID is updated by INSERT INTO statements. The value considers
// performed ROLLBACK statements, if any, even though roll backed IDs are not
// reused. QL clients should treat the field as read only.
//
// RowsAffected
//
// RowsAffected is updated by INSERT INTO, DELETE FROM and UPDATE statements.
// The value does not (yet) consider any ROLLBACK statements involved.  QL
// clients should treat the field as read only.
type TCtx struct {
	LastInsertID int64
	RowsAffected int64
}

// NewRWCtx returns a new read/write transaction context.  NewRWCtx is safe for
// concurrent use by multiple goroutines, every one of them will get a new,
// unique conext.
func NewRWCtx() *TCtx { return &TCtx{} }

// Recordset is a result of a select statment. It can call a user function for
// every row (record) in the set using the Do method.
//
// Recordsets can be safely reused. Evaluation of the rows is performed lazily.
// Every invocation of Do will see the current, potentially actualized data.
//
// Do
//
// Do will call f for every row (record) in the Recordset.
//
// If f returns more == false or err != nil then f will not be called for any
// remaining rows in the set and the err value is returned from Do.
//
// If names == true then f is firstly called with a virtual row
// consisting of field (column) names of the RecordSet.
//
// Do is executed in a read only context and performs a RLock of the
// database.
//
// Do is safe for concurrent use by multiple goroutines.
//
// Fields
//
// Fields return a slice of field names of the recordset. The result is computed
// without actually computing the recordset rows.
//
// FirstRow
//
// FirstRow will return the first row of the RecordSet or an error, if any. If
// the Recordset has no rows the result is (nil, nil).
//
// Rows
//
// Rows will return rows in Recordset or an error, if any. The semantics of
// limit and offset are the same as of the LIMIT and OFFSET clauses of the
// SELECT statement. To get all rows pass limit < 0. If there are no rows to
// return the result is (nil, nil).
type Recordset interface {
	Do(names bool, f func(data []interface{}) (more bool, err error)) error
	Fields() (names []string, err error)
	FirstRow() (row []interface{}, err error)
	Rows(limit, offset int) (rows [][]interface{}, err error)
}

type assignment struct {
	colName string
	expr    expression
}

func (a *assignment) String() string {
	return fmt.Sprintf("%s=%s", a.colName, a.expr)
}

type distinctRset struct {
	src rset2
}

func (r *distinctRset) plan(ctx *execCtx) (rset2, error) {
	return &distinctRset2{src: r.src, fields: r.src.fieldNames()}, nil
}

type distinctRset2 struct {
	src    rset2
	fields []string
}

func (r *distinctRset2) fieldNames() []string { return r.fields }

func (r *distinctRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t, err := ctx.db.store.CreateTemp(true)
	if err != nil {
		return
	}

	defer func() {
		if derr := t.Drop(); derr != nil && err == nil {
			err = derr
		}
	}()

	if err = r.src.do(ctx, func(id interface{}, in []interface{}) (bool, error) {
		if err = t.Set(in, nil); err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
		return
	}

	var data []interface{}
	more := true
	it, err := t.SeekFirst()
	for more && err == nil {
		data, _, err = it.Next()
		if err != nil {
			break
		}

		more, err = f(nil, data)
	}
	return noEOF(err)
}

type orderByRset struct {
	asc bool
	by  []expression
	src rset2
}

func (r *orderByRset) String() string {
	a := make([]string, len(r.by))
	for i, v := range r.by {
		a[i] = v.String()
	}
	s := strings.Join(a, ", ")
	if !r.asc {
		s += " DESC"
	}
	return s
}

func (r *orderByRset) plan(ctx *execCtx) (rset2, error) {
	r2 := &orderByRset2{asc: r.asc, by: r.by, src: r.src, fields: r.src.fieldNames()}
	//TODO optimize here
	return r2, nil
}

type orderByRset2 struct {
	asc    bool
	by     []expression
	src    rset2
	fields []string
}

func (r *orderByRset2) fieldNames() []string { return r.fields }

func (r *orderByRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t, err := ctx.db.store.CreateTemp(r.asc)
	if err != nil {
		return
	}

	defer func() {
		if derr := t.Drop(); derr != nil && err == nil {
			err = derr
		}
	}()

	m := map[interface{}]interface{}{}
	flds := r.fields
	k := make([]interface{}, len(r.by)+1)
	id := int64(-1)
	if err = r.src.do(ctx, func(rid interface{}, in []interface{}) (bool, error) {
		id++
		for i, fld := range flds {
			if fld != "" {
				m[fld] = in[i]
			}
		}
		m["$id"] = rid
		for i, expr := range r.by {
			val, err := expr.eval(ctx, m, ctx.arg)
			if err != nil {
				return false, err
			}

			if val != nil {
				val, ordered, err := isOrderedType(val)
				if err != nil {
					return false, err
				}

				if !ordered {
					return false, fmt.Errorf("cannot order by %v (type %T)", val, val)

				}
			}

			k[i] = val
		}
		k[len(r.by)] = id
		if err = t.Set(k, in); err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
		return
	}

	it, err := t.SeekFirst()
	if err != nil {
		return noEOF(err)
	}

	var data []interface{}
	more := true
	for more && err == nil {
		if _, data, err = it.Next(); err != nil {
			break
		}

		more, err = f(nil, data)
	}
	return noEOF(err)
}

type whereRset struct {
	expr expression
	src  rset2
}

func (r *whereRset) plan(ctx *execCtx) (rset2, error) {
	r2 := &whereRset2{expr: r.expr, src: r.src, fields: r.src.fieldNames()}
	//TODO optimize here
	return r2, nil
}

type whereRset2 struct {
	expr   expression
	src    rset2
	fields []string
}

func (r *whereRset2) fieldNames() []string { return r.fields }

func (r *whereRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	m := map[interface{}]interface{}{}
	return r.src.do(ctx, func(rid interface{}, in []interface{}) (bool, error) {
		for i, fld := range r.fields {
			if fld != "" {
				m[fld] = in[i]
			}
		}
		m["$id"] = rid
		val, err := r.expr.eval(ctx, m, ctx.arg)
		if err != nil {
			return false, err
		}

		if val == nil {
			return true, nil
		}

		x, ok := val.(bool)
		if !ok {
			return false, fmt.Errorf("invalid WHERE expression %s (value of type %T)", val, val)
		}

		if !x {
			return true, nil
		}

		return f(rid, in)
	})
}

type offsetRset struct {
	expr expression
	src  rset2
}

func (r *offsetRset) plan(ctx *execCtx) (rset2, error) {
	rs2 := &offsetRset2{expr: r.expr, src: r.src, fields: r.src.fieldNames()}
	//TODO optimize here
	return rs2, nil
}

type offsetRset2 struct {
	expr   expression
	src    rset2
	fields []string
}

func (r *offsetRset2) fieldNames() []string { return r.fields }

func (r *offsetRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
	m := map[interface{}]interface{}{}
	var eval bool
	var off uint64
	return r.src.do(ctx, func(rid interface{}, in []interface{}) (bool, error) {
		if !eval {
			for i, fld := range r.fields {
				if fld != "" {
					m[fld] = in[i]
				}
			}
			m["$id"] = rid
			val, err := r.expr.eval(ctx, m, ctx.arg)
			if err != nil {
				return false, err
			}

			if val == nil {
				return true, nil
			}

			if off, err = limOffExpr(val); err != nil {
				return false, err
			}

			eval = true
		}
		if off > 0 {
			off--
			return true, nil
		}

		return f(rid, in)
	})
}

type limitRset struct {
	expr expression
	src  rset2
}

func (r *limitRset) plan(ctx *execCtx) (rset2, error) {
	rs2 := &limitRset2{expr: r.expr, src: r.src, fields: r.src.fieldNames()}
	//TODO optimize here
	return rs2, nil
}

type limitRset2 struct {
	expr   expression
	src    rset2
	fields []string
}

func (r *limitRset2) fieldNames() []string { return r.fields }

func (r *limitRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
	m := map[interface{}]interface{}{}
	var eval bool
	var lim uint64
	return r.src.do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
		if !eval {
			for i, fld := range r.fields {
				if fld != "" {
					m[fld] = in[i]
				}
			}
			m["$id"] = rid
			val, err := r.expr.eval(ctx, m, ctx.arg)
			if err != nil {
				return false, err
			}

			if val == nil {
				return true, nil
			}

			if lim, err = limOffExpr(val); err != nil {
				return false, err
			}

			eval = true
		}
		switch lim {
		case 0:
			return false, nil
		default:
			lim--
			return f(rid, in)
		}
	})
}

type selectRset struct {
	flds []*fld
	src  rset2
}

func (r *selectRset) plan(ctx *execCtx) (rset2, error) {
	rs2 := &selectRset2{flds: append([]*fld(nil), r.flds...), src: r.src}
	for _, v := range r.flds {
		rs2.fields = append(rs2.fields, v.name)
	}
	if len(r.flds) == 0 {
		rs2.fields = r.src.fieldNames()
	}
	return rs2, nil
}

type selectRset2 struct {
	flds   []*fld
	src    rset2
	fields []string
}

func (r *selectRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	if _, ok := r.src.(*groupByRset2); ok { //TODO different plan, also possible conflict with optimizer
		return r.doGroup(ctx, f)
	}

	if len(r.flds) == 0 {
		return r.src.do(ctx, f)
	}

	fields := r.src.fieldNames()
	m := map[interface{}]interface{}{}
	return r.src.do(ctx, func(rid interface{}, in []interface{}) (bool, error) {
		for i, nm := range fields {
			if nm != "" {
				m[nm] = in[i]
			}
		}
		m["$id"] = rid
		out := make([]interface{}, len(r.flds))
		for i, fld := range r.flds {
			if out[i], err = fld.expr.eval(ctx, m, ctx.arg); err != nil {
				return false, err
			}
		}
		return f(rid, out)
	})
}

func (r *selectRset2) doGroup(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
	var t temp
	var cols []*col
	var err error
	out := make([]interface{}, len(r.flds))
	ok := false
	rows := false
	if err = r.src.do(ctx, func(rid interface{}, in []interface{}) (bool, error) {
		if ok {
			h := in[0].(int64)
			m := map[interface{}]interface{}{}
			for h != 0 {
				in, err = t.Read(nil, h, cols...)
				if err != nil {
					return false, err
				}

				rec := in[2:]
				for i, c := range cols {
					if nm := c.name; nm != "" {
						m[nm] = rec[i]
					}
				}
				m["$id"] = rid
				for _, fld := range r.flds {
					if _, err = fld.expr.eval(ctx, m, ctx.arg); err != nil {
						return false, err
					}
				}

				h = in[0].(int64)
			}
			m["$agg"] = true
			for i, fld := range r.flds {
				if out[i], err = fld.expr.eval(ctx, m, ctx.arg); err != nil {
					return false, err
				}
			}
			rows = true
			return f(nil, out)
		}

		ok = true
		t = in[0].(temp)
		cols = in[1].([]*col)
		if len(r.flds) == 0 { // SELECT *
			r.flds = make([]*fld, len(cols))
			for i, v := range cols {
				r.flds[i] = &fld{expr: &ident{v.name}, name: v.name}
			}
			out = make([]interface{}, len(r.flds))
		}
		return true, nil
	}); err != nil {
		return err
	}

	if rows {
		return nil
	}

	m := map[interface{}]interface{}{"$agg0": true} // aggregate empty record set
	for i, fld := range r.flds {
		if out[i], err = fld.expr.eval(ctx, m, ctx.arg); err != nil {
			return err
		}
	}
	_, err = f(nil, out)

	return err
}

func (r *selectRset2) fieldNames() []string { return r.fields }

type tableRset string

func (r tableRset) plan(ctx *execCtx) (rset2, error) {
	switch r {
	case "__Table":
		return sysTableRset2{}, nil
	case "__Column":
		return sysColumnRset2{}, nil
	case "__Index":
		return sysIndexRset2{}, nil
	}

	t, ok := ctx.db.root.tables[string(r)]
	if !ok && isTesting {
		if _, x0 := ctx.db.root.findIndexByName(string(r)); x0 != nil {
			return &indexRset2{nm: string(r), x: x0}, nil
		}
	}

	if !ok {
		return nil, fmt.Errorf("table %s does not exist", r)
	}

	rs := &tableRset2{t: t}
	for _, col := range t.cols {
		rs.fields = append(rs.fields, col.name)
	}
	return rs, nil
}

type sysIndexRset2 struct{}

func (r sysIndexRset2) fieldNames() []string {
	return []string{"TableName", "ColumnName", "Name", "IsUnique"}
}

func (r sysIndexRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
	rec := make([]interface{}, 4)
	di, err := ctx.db.info()
	if err != nil {
		return err
	}

	var id int64
	for _, xi := range di.Indices {
		rec[0] = xi.Table
		rec[1] = xi.Column
		rec[2] = xi.Name
		rec[3] = xi.Unique
		id++
		if more, err := f(id, rec); !more || err != nil {
			return err
		}
	}
	return nil
}

type sysColumnRset2 struct{}

func (r sysColumnRset2) fieldNames() []string { return []string{"TableName", "Ordinal", "Name", "Type"} }

func (r sysColumnRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
	rec := make([]interface{}, 4)
	di, err := ctx.db.info()
	if err != nil {
		return err
	}

	var id int64
	for _, ti := range di.Tables {
		rec[0] = ti.Name
		var ix int64
		for _, ci := range ti.Columns {
			ix++
			rec[1] = ix
			rec[2] = ci.Name
			rec[3] = ci.Type.String()
			id++
			if more, err := f(id, rec); !more || err != nil {
				return err
			}
		}
	}
	return nil
}

type sysTableRset2 struct{}

func (r sysTableRset2) fieldNames() []string { return []string{"Name", "Schema"} }

func (r sysTableRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
	rec := make([]interface{}, 2)
	di, err := ctx.db.info()
	if err != nil {
		return err
	}

	var id int64
	for _, ti := range di.Tables {
		rec[0] = ti.Name
		a := []string{}
		for _, ci := range ti.Columns {
			s := ""
			if ci.NotNull {
				s += " NOT NULL"
			}
			if c := ci.Constraint; c != "" {
				s += " " + c
			}
			if d := ci.Default; d != "" {
				s += " DEFAULT " + d
			}
			a = append(a, fmt.Sprintf("%s %s%s", ci.Name, ci.Type, s))
		}
		rec[1] = fmt.Sprintf("CREATE TABLE %s (%s);", ti.Name, strings.Join(a, ", "))
		id++
		if more, err := f(id, rec); !more || err != nil {
			return err
		}
	}
	return nil
}

type indexRset2 struct {
	nm string
	x  interface{}
}

func (r *indexRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	var x btreeIndex
	switch ix := r.x.(type) {
	case *indexedCol:
		x = ix.x
	case *index2:
		x = ix.x
	default:
		panic("internal error 007")
	}

	en, err := x.SeekFirst()
	if err != nil {
		return noEOF(err)
	}

	var id int64
	for {
		k, _, err := en.Next()
		if err != nil {
			return noEOF(err)
		}

		id++
		if more, err := f(id, k); !more || err != nil {
			return err
		}
	}
}

func (r *indexRset2) fieldNames() []string {
	return []string{r.nm}
}

type tableRset2 struct {
	t      *table
	fields []string
}

func (r *tableRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t := r.t
	h := t.head
	cols := t.cols
	for h > 0 {
		rec, err := t.store.Read(nil, h, cols...) // 0:next, 1:id, 2...: data
		if err != nil {
			return err
		}

		if n := len(cols) + 2 - len(rec); n > 0 {
			rec = append(rec, make([]interface{}, n)...)
		}
		for i, c := range cols {
			if x := c.index; 2+x < len(rec) {
				rec[2+i] = rec[2+x]
				continue
			}

			rec[2+i] = nil
		}
		if m, err := f(rec[1], rec[2:2+len(cols)]); !m || err != nil {
			return err
		}

		h = rec[0].(int64) // next
	}
	return nil
}

func (r *tableRset2) fieldNames() []string { return r.fields }

type crossJoinRset struct {
	sources []interface{}
}

func (r *crossJoinRset) String() string {
	a := make([]string, len(r.sources))
	for i, pair0 := range r.sources {
		pair := pair0.([]interface{})
		altName := pair[1].(string)
		switch x := pair[0].(type) {
		case string: // table name
			switch {
			case altName == "":
				a[i] = x
			default:
				a[i] = fmt.Sprintf("%s AS %s", x, altName)
			}
		case *selectStmt:
			switch {
			case altName == "":
				a[i] = fmt.Sprintf("(%s)", x)
			default:
				a[i] = fmt.Sprintf("(%s) AS %s", x, altName)
			}
		default:
			log.Panic("internal error 054")
		}
	}
	return strings.Join(a, ", ")
}

func (r *crossJoinRset) plan(ctx *execCtx) (rset2, error) {
	r2 := crossJoinRset2{}
	r2.rsets = make([]rset2, len(r.sources))
	r2.names = make([]string, len(r.sources))
	var err error
	m := map[string]bool{}
	for i, v := range r.sources {
		pair := v.([]interface{})
		src := pair[0]
		nm := pair[1].(string)
		if s, ok := src.(string); ok {
			src = tableRset(s)
			if nm == "" {
				nm = s
			}
		}
		if m[nm] {
			return nil, fmt.Errorf("%s: duplicate name %s", r.String(), nm)
		}

		if nm != "" {
			m[nm] = true
		}
		r2.names[i] = nm
		var rs2 rset2
		switch x := src.(type) {
		case rset:
			if rs2, err = x.plan(ctx); err != nil {
				return nil, err
			}
		case rset2:
			rs2 = x
		default:
			panic("internal error 008")
		}

		switch {
		case len(r.sources) == 1:
			r2.fields = rs2.fieldNames()
		default:
			for _, f := range rs2.fieldNames() {
				if strings.Contains(f, ".") {
					return nil, fmt.Errorf("cannot join on recordset with already qualified field names (use the AS clause): %s", f)
				}

				if f != "" && nm != "" {
					f = fmt.Sprintf("%s.%s", nm, f)
				}
				if nm == "" {
					f = ""
				}
				r2.fields = append(r2.fields, f)
			}
		}
		r2.rsets[i] = rs2
	}
	return &r2, nil
}

type crossJoinRset2 struct {
	rsets  []rset2
	names  []string
	fields []string
}

func (r *crossJoinRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
	if len(r.rsets) == 1 {
		return r.rsets[0].do(ctx, f)
	}

	ids := map[string]interface{}{}
	var g func([]interface{}, []rset2, int) error
	g = func(prefix []interface{}, rsets []rset2, x int) (err error) {
		return rsets[0].do(ctx, func(id interface{}, in []interface{}) (bool, error) {
			ids[r.names[x]] = id
			if len(rsets) > 1 {
				return true, g(append(prefix, in...), rsets[1:], x+1)
			}

			return f(ids, append(prefix, in...))
		})
	}
	return g(nil, r.rsets, 0)
}

func (r *crossJoinRset2) fieldNames() []string { return r.fields }

type fld struct {
	expr expression
	name string
}

func findFldIndex(fields []*fld, name string) int {
	for i, f := range fields {
		if f.name == name {
			return i
		}
	}

	return -1
}

func findFld(fields []*fld, name string) (f *fld) {
	for _, f = range fields {
		if f.name == name {
			return
		}
	}

	return nil
}

type col struct {
	index      int
	name       string
	typ        int
	constraint *constraint
	dflt       expression
}

func findCol(cols []*col, name string) (c *col) {
	for _, c = range cols {
		if c.name == name {
			return
		}
	}

	return nil
}

func (f *col) typeCheck(x interface{}) (ok bool) { //NTYPE
	switch x.(type) {
	case nil:
		return true
	case bool:
		return f.typ == qBool
	case complex64:
		return f.typ == qComplex64
	case complex128:
		return f.typ == qComplex128
	case float32:
		return f.typ == qFloat32
	case float64:
		return f.typ == qFloat64
	case int8:
		return f.typ == qInt8
	case int16:
		return f.typ == qInt16
	case int32:
		return f.typ == qInt32
	case int64:
		return f.typ == qInt64
	case string:
		return f.typ == qString
	case uint8:
		return f.typ == qUint8
	case uint16:
		return f.typ == qUint16
	case uint32:
		return f.typ == qUint32
	case uint64:
		return f.typ == qUint64
	case []byte:
		return f.typ == qBlob
	case *big.Int:
		return f.typ == qBigInt
	case *big.Rat:
		return f.typ == qBigRat
	case time.Time:
		return f.typ == qTime
	case time.Duration:
		return f.typ == qDuration
	case chunk:
		return true // was checked earlier
	}
	return
}

func cols2meta(f []*col) (s string) {
	a := []string{}
	for _, f := range f {
		a = append(a, string(f.typ)+f.name)
	}
	return strings.Join(a, "|")
}

// DB represent the database capable of executing QL statements.
type DB struct {
	cc          *TCtx // Current transaction context
	isMem       bool
	mu          sync.Mutex
	root        *root
	rw          bool // DB FSM
	rwmu        sync.RWMutex
	store       storage
	tnl         int // Transaction nesting level
	exprCache   map[string]expression
	exprCacheMu sync.Mutex
	hasIndex2   int // 0: nope, 1: in progress, 2: yes.
}

var selIndex2Expr = MustCompile("select Expr from __Index2_Expr where Index2_ID == $1")

func newDB(store storage) (db *DB, err error) {
	db0 := &DB{
		exprCache: map[string]expression{},
		store:     store,
	}
	if db0.root, err = newRoot(store); err != nil {
		return
	}

	if !db0.hasAllIndex2() {
		return db0, nil
	}

	db0.hasIndex2 = 2
	rss, _, err := db0.Run(nil, "select id(), TableName, IndexName, IsUnique, Root from __Index2 where !IsSimple")
	if err != nil {
		return nil, err
	}

	rows, err := rss[0].Rows(-1, 0)
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Errorf("error loading DB indices: %v", e)
			}
		}()

		id := row[0].(int64)
		tn := row[1].(string)
		xn := row[2].(string)
		unique := row[3].(bool)
		xroot := row[4].(int64)

		//dbg("newDB: tn %v, xn %v, xroot %v", tn, xn, xroot)
		t := db0.root.tables[tn]
		if t == nil {
			return nil, fmt.Errorf("DB index refers to nonexistent table: %s", tn)
		}

		x, err := store.OpenIndex(unique, xroot)
		if err != nil {
			return nil, err
		}

		if v := t.indices2[xn]; v != nil {
			return nil, fmt.Errorf("duplicate DB index: %s", xn)
		}

		ix := &index2{
			unique: unique,
			x:      x,
			xroot:  xroot,
		}

		rss, _, err := db0.Execute(nil, selIndex2Expr, id)
		if err != nil {
			return nil, err
		}

		rows, err := rss[0].Rows(-1, 0)
		if err != nil {
			return nil, err
		}

		if len(rows) == 0 {
			return nil, fmt.Errorf("index has no expression: %s", xn)
		}

		var list []expression
		for _, row := range rows {
			src, ok := row[0].(string)
			if !ok {
				return nil, fmt.Errorf("index %s: expression of type %T", xn, row[0])
			}

			expr, err := db0.str2expr(src)
			if err != nil {
				return nil, fmt.Errorf("index %s: expression error: %v", xn, err)
			}

			list = append(list, expr)
		}

		ix.exprList = list
		if t.indices2 == nil {
			t.indices2 = map[string]*index2{}
		}
		t.indices2[xn] = ix
	}
	return db0, nil
}

func (db *DB) deleteIndex2ByIndexName(nm string) error {
	for _, s := range deleteIndex2ByIndexName.l {
		if _, err := s.exec(&execCtx{db: db, arg: []interface{}{nm}}); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) deleteIndex2ByTableName(nm string) error {
	for _, s := range deleteIndex2ByTableName.l {
		if _, err := s.exec(&execCtx{db: db, arg: []interface{}{nm}}); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) createIndex2() error {
	if db.hasIndex2 != 0 {
		return nil
	}

	db.hasIndex2 = 1
	ctx := execCtx{db: db}
	for _, s := range createIndex2.l {
		if _, err := s.exec(&ctx); err != nil {
			db.hasIndex2 = 0
			return err
		}
	}

	for t := db.root.thead; t != nil; t = t.tnext {
		for i, index := range t.indices {
			if index == nil {
				continue
			}

			expr := "id()"
			if i != 0 {
				expr = t.cols[i-1].name
			}

			if err := db.insertIndex2(t.name, index.name, []string{expr}, index.unique, true, index.xroot); err != nil {
				db.hasIndex2 = 0
				return err
			}
		}
	}

	db.hasIndex2 = 2
	return nil
}

func (db *DB) insertIndex2(tableName, indexName string, expr []string, unique, isSimple bool, h int64) error {
	ctx := execCtx{db: db}
	ctx.arg = []interface{}{
		tableName,
		indexName,
		unique,
		isSimple,
		h,
	}
	if _, err := insertIndex2.l[0].exec(&ctx); err != nil {
		return err
	}

	id := db.root.lastInsertID
	for _, e := range expr {
		ctx.arg = []interface{}{id, e}
		if _, err := insertIndex2Expr.l[0].exec(&ctx); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) hasAllIndex2() bool {
	t := db.root.tables
	if _, ok := t["__Index2"]; !ok {
		return false
	}

	_, ok := t["__Index2_Expr"]
	return ok
}

func (db *DB) str2expr(expr string) (expression, error) {
	db.exprCacheMu.Lock()
	e := db.exprCache[expr]
	db.exprCacheMu.Unlock()
	if e != nil {
		return e, nil
	}

	e, err := compileExpr(expr)
	if err != nil {
		return nil, err
	}

	db.exprCacheMu.Lock()
	for k := range db.exprCache {
		if len(db.exprCache) < 1000 {
			break
		}

		delete(db.exprCache, k)
	}
	db.exprCache[expr] = e
	db.exprCacheMu.Unlock()
	return e, nil
}

// Name returns the name of the DB.
func (db *DB) Name() string { return db.store.Name() }

// Run compiles and executes a statement list.  It returns, if applicable, a
// RecordSet slice and/or an index and error.
//
// For more details please see DB.Execute
//
// Run is safe for concurrent use by multiple goroutines.
func (db *DB) Run(ctx *TCtx, ql string, arg ...interface{}) (rs []Recordset, index int, err error) {
	l, err := Compile(ql)
	if err != nil {
		return nil, -1, err
	}

	return db.Execute(ctx, l, arg...)
}

func (db *DB) run(ctx *TCtx, ql string, arg ...interface{}) (rs []Recordset, index int, err error) {
	l, err := compile(ql)
	if err != nil {
		return nil, -1, err
	}

	return db.Execute(ctx, l, arg...)
}

// Compile parses the ql statements from src and returns a compiled list for
// DB.Execute or an error if any.
//
// Compile is safe for concurrent use by multiple goroutines.
func Compile(src string) (List, error) {
	l := newLexer(src)
	if yyParse(l) != 0 {
		return List{}, l.errs[0]
	}

	return List{l.list, l.params}, nil
}

func compileExpr(src string) (expression, error) {
	l := newLexer(src)
	l.inj = parseExpression
	if yyParse(l) != 0 {
		return nil, l.errs[0]
	}

	return l.expr, nil
}

func compile(src string) (List, error) {
	l := newLexer(src)
	l.root = true
	if yyParse(l) != 0 {
		return List{}, l.errs[0]
	}

	return List{l.list, l.params}, nil
}

// MustCompile is like Compile but panics if the ql statements in src cannot be
// compiled. It simplifies safe initialization of global variables holding
// compiled statement lists for DB.Execute.
//
// MustCompile is safe for concurrent use by multiple goroutines.
func MustCompile(src string) List {
	list, err := Compile(src)
	if err != nil {
		panic("ql: Compile(" + strconv.Quote(src) + "): " + err.Error()) // panic ok here
	}

	return list
}

func mustCompile(src string) List {
	list, err := compile(src)
	if err != nil {
		panic("ql: compile(" + strconv.Quote(src) + "): " + err.Error()) // panic ok here
	}

	return list
}

// Execute executes statements in a list while substituting QL paramaters from
// arg.
//
// The resulting []Recordset corresponds to the SELECT FROM statements in the
// list.
//
// If err != nil then index is the zero based index of the failed QL statement.
// Empty statements do not count.
//
// The FSM STT describing the relations between DB states, statements and the
// ctx parameter.
//
//  +-----------+---------------------+------------------+------------------+------------------+
//  |\  Event   |                     |                  |                  |                  |
//  | \-------\ |     BEGIN           |                  |                  |    Other         |
//  |   State  \|     TRANSACTION     |      COMMIT      |     ROLLBACK     |    statement     |
//  +-----------+---------------------+------------------+------------------+------------------+
//  | RD        | if PC == nil        | return error     | return error     | DB.RLock         |
//  |           |     return error    |                  |                  | Execute(1)       |
//  | CC == nil |                     |                  |                  | DB.RUnlock       |
//  | TNL == 0  | DB.Lock             |                  |                  |                  |
//  |           | CC = PC             |                  |                  |                  |
//  |           | TNL++               |                  |                  |                  |
//  |           | DB.BeginTransaction |                  |                  |                  |
//  |           | State = WR          |                  |                  |                  |
//  +-----------+---------------------+------------------+------------------+------------------+
//  | WR        | if PC == nil        | if PC != CC      | if PC != CC      | if PC == nil     |
//  |           |     return error    |     return error |     return error |     DB.Rlock     |
//  | CC != nil |                     |                  |                  |     Execute(1)   |
//  | TNL != 0  | if PC != CC         | DB.Commit        | DB.Rollback      |     RUnlock      |
//  |           |     DB.Lock         | TNL--            | TNL--            | else if PC != CC |
//  |           |     CC = PC         | if TNL == 0      | if TNL == 0      |     return error |
//  |           |                     |     CC = nil     |     CC = nil     | else             |
//  |           | TNL++               |     State = RD   |     State = RD   |     Execute(2)   |
//  |           | DB.BeginTransaction |     DB.Unlock    |     DB.Unlock    |                  |
//  +-----------+---------------------+------------------+------------------+------------------+
//  CC: Curent transaction context
//  PC: Passed transaction context
//  TNL: Transaction nesting level
//
// Lock, Unlock, RLock, RUnlock semantics above are the same as in
// sync.RWMutex.
//
// (1): Statement list is executed outside of a transaction. Attempts to update
// the DB will fail, the execution context is read-only. Other statements with
// read only context will execute concurrently. If any statement fails, the
// execution of the statement list is aborted.
//
// Note that the RLock/RUnlock surrounds every single "other" statement when it
// is executed outside of a transaction. If read consistency is required by a
// list of more than one statement then an explicit BEGIN TRANSACTION / COMMIT
// or ROLLBACK wrapper must be provided. Otherwise the state of the DB may
// change in between executing any two out-of-transaction statements.
//
// (2): Statement list is executed inside an isolated transaction. Execution of
// statements can update the DB, the execution context is read-write. If any
// statement fails, the execution of the statement list is aborted and the DB
// is automatically rolled back to the TNL which was active before the start of
// execution of the statement list.
//
// Execute is safe for concurrent use by multiple goroutines, but one must
// consider the blocking issues as discussed above.
//
// ACID
//
// Atomicity: Transactions are atomic. Transactions can be nested. Commit or
// rollbacks work on the current transaction level. Transactions are made
// persistent only on the top level commit. Reads made from within an open
// transaction are dirty reads.
//
// Consistency: Transactions bring the DB from one structurally consistent
// state to other structurally consistent state.
//
// Isolation: Transactions are isolated. Isolation is implemented by
// serialization.
//
// Durability: Transactions are durable. A two phase commit protocol and a
// write ahead log is used. Database is recovered after a crash from the write
// ahead log automatically on open.
func (db *DB) Execute(ctx *TCtx, l List, arg ...interface{}) (rs []Recordset, index int, err error) {
	// Sanitize args
	for i, v := range arg {
		switch x := v.(type) {
		case nil, bool, complex64, complex128, float32, float64, string,
			int8, int16, int32, int64, int,
			uint8, uint16, uint32, uint64, uint,
			*big.Int, *big.Rat, []byte, time.Duration, time.Time:
		case big.Int:
			arg[i] = &x
		case big.Rat:
			arg[i] = &x
		default:
			return nil, 0, fmt.Errorf("cannot use arg[%d] (type %T):unsupported type", i, v)
		}
	}

	tnl0 := db.tnl
	if ctx != nil {
		ctx.LastInsertID, ctx.RowsAffected = 0, 0
	}

	list := l.l
	for _, s := range list {
		r, err := db.run1(ctx, s, arg...)
		if err != nil {
			for db.tnl > tnl0 {
				if _, e2 := db.run1(ctx, rollbackStmt{}); e2 != nil {
					err = e2
				}
			}
			return rs, index, err
		}

		if r != nil {
			rs = append(rs, r)
		}
	}
	return
}

func (db *DB) run1(pc *TCtx, s stmt, arg ...interface{}) (rs Recordset, err error) {
	//dbg("=================================================================")
	//dbg("BEFORE %s", s)
	//dumpTables4(db)
	//defer func() {
	//	dbg("AFTER")
	//	dumpTables4(db)
	//	dbg("*****************************************************************")
	//}()
	//dbg("%v", s)
	db.mu.Lock()
	switch db.rw {
	case false:
		switch s.(type) {
		case beginTransactionStmt:
			defer db.mu.Unlock()
			if pc == nil {
				return nil, errors.New("BEGIN TRANSACTION: cannot start a transaction in nil TransactionCtx")
			}

			if err = db.store.BeginTransaction(); err != nil {
				return
			}

			db.beginTransaction()
			db.rwmu.Lock()
			db.cc = pc
			db.tnl++
			db.rw = true
			return
		case commitStmt:
			defer db.mu.Unlock()
			return nil, errCommitNotInTransaction
		case rollbackStmt:
			defer db.mu.Unlock()
			return nil, errRollbackNotInTransaction
		default:
			if s.isUpdating() {
				db.mu.Unlock()
				return nil, fmt.Errorf("attempt to update the DB outside of a transaction")
			}

			db.rwmu.RLock() // can safely grab before Unlock
			db.mu.Unlock()
			defer db.rwmu.RUnlock()
			return s.exec(&execCtx{db, arg}) // R/O tctx
		}
	default: // case true:
		switch s.(type) {
		case beginTransactionStmt:
			defer db.mu.Unlock()

			if pc == nil {
				return nil, errBeginTransNoCtx
			}

			if pc != db.cc {
				for db.rw == true {
					db.mu.Unlock() // Transaction isolation
					db.mu.Lock()
				}

				db.rw = true
				db.rwmu.Lock()
			}

			if err = db.store.BeginTransaction(); err != nil {
				return
			}

			db.beginTransaction()
			db.cc = pc
			db.tnl++
			return
		case commitStmt:
			defer db.mu.Unlock()
			if pc != db.cc {
				return nil, fmt.Errorf("invalid passed transaction context")
			}

			db.commit()
			err = db.store.Commit()
			db.tnl--
			if db.tnl != 0 {
				return
			}

			db.cc = nil
			db.rw = false
			db.rwmu.Unlock()
			return
		case rollbackStmt:
			defer db.mu.Unlock()
			defer func() { pc.LastInsertID = db.root.lastInsertID }()
			if pc != db.cc {
				return nil, fmt.Errorf("invalid passed transaction context")
			}

			db.rollback()
			err = db.store.Rollback()
			db.tnl--
			if db.tnl != 0 {
				return
			}

			db.cc = nil
			db.rw = false
			db.rwmu.Unlock()
			return
		default:
			if pc == nil {
				if s.isUpdating() {
					db.mu.Unlock()
					return nil, fmt.Errorf("attempt to update the DB outside of a transaction")
				}

				db.mu.Unlock() // must Unlock before RLock
				db.rwmu.RLock()
				defer db.rwmu.RUnlock()
				return s.exec(&execCtx{db, arg})
			}

			defer db.mu.Unlock()
			defer func() { pc.LastInsertID = db.root.lastInsertID }()
			if pc != db.cc {
				return nil, fmt.Errorf("invalid passed transaction context")
			}

			return s.exec(&execCtx{db, arg})
		}
	}
}

// Flush ends the transaction collecting window, if applicable. IOW, if the DB
// is dirty, it schedules a 2PC (WAL + DB file) commit on the next outer most
// DB.Commit or performs it synchronously if there's currently no open
// transaction.
//
// The collecting window is an implementation detail and future versions of
// Flush may become a no operation while keeping the operation semantics.
func (db *DB) Flush() (err error) {
	return nil
}

// Close will close the DB. Successful Close is idempotent.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.store == nil {
		return nil
	}

	if db.tnl != 0 {
		return fmt.Errorf("cannot close DB while open transaction exist")
	}

	err := db.store.Close()
	db.root, db.store = nil, nil
	return err
}

func (db *DB) do(r recordset, f func(data []interface{}) (bool, error)) (err error) {
	db.mu.Lock()
	switch db.rw {
	case false:
		db.rwmu.RLock() // can safely grab before Unlock
		db.mu.Unlock()
		defer db.rwmu.RUnlock()
	default: // case true:
		if r.tx == nil {
			db.mu.Unlock() // must Unlock before RLock
			db.rwmu.RLock()
			defer db.rwmu.RUnlock()
			break
		}

		defer db.mu.Unlock()
		if r.tx != db.cc {
			return fmt.Errorf("invalid passed transaction context")
		}
	}

	return r.do(r.ctx, func(id interface{}, data []interface{}) (bool, error) {
		if err = expand(data); err != nil {
			return false, err
		}

		return f(data)
	})
}

func (db *DB) beginTransaction() { //TODO Rewrite, must use much smaller undo info!
	root := *db.root
	root.parent = db.root
	root.tables = make(map[string]*table, len(db.root.tables))
	var tprev *table
	for t := db.root.thead; t != nil; t = t.tnext {
		t2 := t.clone()
		root.tables[t2.name] = t2
		t2.tprev = tprev
		switch {
		case tprev == nil:
			root.thead = t2
		default:
			tprev.tnext = t2
		}
		tprev = t2
	}
	db.root = &root
}

func (db *DB) rollback() {
	db.root = db.root.parent
}

func (db *DB) commit() {
	db.root.parent = db.root.parent.parent
}

// Type represents a QL type (bigint, int, string, ...)
type Type int

// Values of ColumnInfo.Type.
const (
	BigInt     Type = qBigInt
	BigRat          = qBigRat
	Blob            = qBlob
	Bool            = qBool
	Complex128      = qComplex128
	Complex64       = qComplex64
	Duration        = qDuration
	Float32         = qFloat32
	Float64         = qFloat64
	Int16           = qInt16
	Int32           = qInt32
	Int64           = qInt64
	Int8            = qInt8
	String          = qString
	Time            = qTime
	Uint16          = qUint16
	Uint32          = qUint32
	Uint64          = qUint64
	Uint8           = qUint8
)

// String implements fmt.Stringer.
func (t Type) String() string {
	return typeStr(int(t))
}

// ColumnInfo provides meta data describing a table column.
type ColumnInfo struct {
	Name       string // Column name.
	Type       Type   // Column type (BigInt, BigRat, ...).
	NotNull    bool   // Column cannot be NULL.
	Constraint string // Constraint expression, if any.
	Default    string // Default expression, if any.
}

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	// Table name.
	Name string

	// Table schema. Columns are listed in the order in which they appear
	// in the schema.
	Columns []ColumnInfo
}

// IndexInfo provides meta data describing a DB index.  It corresponds to the
// statement
//
//	CREATE INDEX Name ON Table (Column);
type IndexInfo struct {
	Name           string   // Index name
	Table          string   // Table name.
	Column         string   // Column name.
	Unique         bool     // Wheter the index is unique.
	ExpressionList []string // Index expression list.
}

// DbInfo provides meta data describing a DB.
type DbInfo struct {
	Name    string      // DB name.
	Tables  []TableInfo // Tables in the DB.
	Indices []IndexInfo // Indices in the DB.
}

func (db *DB) info() (r *DbInfo, err error) {
	_, hasColumn2 := db.root.tables["__Column2"]
	r = &DbInfo{Name: db.Name()}
	for nm, t := range db.root.tables {
		ti := TableInfo{Name: nm}
		m := map[string]*ColumnInfo{}
		if hasColumn2 {
			rs, err := selectColumn2.l[0].exec(&execCtx{db: db})
			if err != nil {
				return nil, err
			}

			if err := rs.(recordset).do(
				&execCtx{db: db, arg: []interface{}{nm}},
				func(id interface{}, data []interface{}) (bool, error) {
					ci := &ColumnInfo{NotNull: data[1].(bool), Constraint: data[2].(string), Default: data[3].(string)}
					m[data[0].(string)] = ci
					return true, nil
				},
			); err != nil {
				return nil, err
			}
		}
		for _, c := range t.cols {
			ci := ColumnInfo{Name: c.name, Type: Type(c.typ)}
			if c2 := m[c.name]; c2 != nil {
				ci.NotNull = c2.NotNull
				ci.Constraint = c2.Constraint
				ci.Default = c2.Default
			}
			ti.Columns = append(ti.Columns, ci)
		}
		r.Tables = append(r.Tables, ti)
		for i, x := range t.indices {
			if x == nil {
				continue
			}

			var cn string
			switch {
			case i == 0:
				cn = "id()"
			default:
				cn = t.cols0[i-1].name
			}
			r.Indices = append(r.Indices, IndexInfo{x.name, nm, cn, x.unique, []string{cn}})
		}
		var a []string
		for k := range t.indices2 {
			a = append(a, k)
		}
		for _, k := range a {
			x := t.indices2[k]
			a = a[:0]
			for _, e := range x.exprList {
				a = append(a, e.String())
			}
			r.Indices = append(r.Indices, IndexInfo{k, nm, "", x.unique, a})
		}
	}
	return
}

// Info provides meta data describing a DB or an error if any. It locks the DB
// to obtain the result.
func (db *DB) Info() (r *DbInfo, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.info()
}

type constraint struct {
	expr expression // If expr == nil: constraint is 'NOT NULL'
}

type outerJoinRset struct {
	typ    int // leftJoin, rightJoin, fullJoin
	src    *crossJoinRset2
	source []interface{}
	on     expression
}

func (r *outerJoinRset) plan(ctx *execCtx) (rset2, error) {
	c := &crossJoinRset{}
	for i, v := range r.src.rsets {
		c.sources = append(c.sources, []interface{}{v, r.src.names[i]})
	}
	c.sources = append(c.sources, r.source)
	rs2, err := c.plan(ctx)
	if err != nil {
		return nil, err
	}

	c2 := rs2.(*crossJoinRset2)
	switch r.typ {
	case 0:
		return &leftJoinRset2{
			on:     r.on,
			rsets:  c2.rsets,
			names:  c2.names,
			right:  len(c2.rsets[len(c2.rsets)-1].fieldNames()),
			fields: rs2.fieldNames(),
		}, nil
	case 1:
		return &rightJoinRset2{
			leftJoinRset2{
				on:     r.on,
				rsets:  c2.rsets,
				names:  c2.names,
				right:  len(c2.rsets[len(c2.rsets)-1].fieldNames()),
				fields: rs2.fieldNames(),
			},
		}, nil
	case 2:
		return &fullJoinRset2{
			leftJoinRset2{
				on:     r.on,
				rsets:  c2.rsets,
				names:  c2.names,
				right:  len(c2.rsets[len(c2.rsets)-1].fieldNames()),
				fields: rs2.fieldNames(),
			},
		}, nil
	default:
		panic("internal error 009")
	}
}

type leftJoinRset2 struct {
	on     expression
	rsets  []rset2
	names  []string
	right  int
	fields []string
}

type rightJoinRset2 struct {
	leftJoinRset2
}

type fullJoinRset2 struct {
	leftJoinRset2
}

func (r *leftJoinRset2) fieldNames() []string { return r.fields }

func (r *leftJoinRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error {
	m := map[interface{}]interface{}{}
	ids := map[string]interface{}{}
	var g func([]interface{}, []rset2, int) error
	var match bool
	g = func(prefix []interface{}, rsets []rset2, x int) (err error) {
		return rsets[0].do(ctx, func(id interface{}, in []interface{}) (bool, error) {
			ids[r.names[x]] = id
			row := append(prefix, in...)
			if len(rsets) > 1 {
				if len(rsets) == 2 {
					match = false
				}
				if err = g(row, rsets[1:], x+1); err != nil {
					return false, err
				}

				if len(rsets) != 2 || match {
					return true, nil
				}

				ids[r.names[x+1]] = nil
				return f(ids, append(row, make([]interface{}, r.right)...))
			}

			for i, fld := range r.fields {
				if fld != "" {
					m[fld] = row[i]
				}
			}

			val, err := r.on.eval(ctx, m, ctx.arg)
			if err != nil {
				return false, err
			}

			if val == nil {
				return true, nil
			}

			x, ok := val.(bool)
			if !ok {
				return false, fmt.Errorf("invalid ON expression %s (value of type %T)", val, val)
			}

			if !x {
				return true, nil
			}

			match = true
			return f(ids, row)
		})
	}
	return g(nil, r.rsets, 0)
}

func (r *rightJoinRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error {
	right := r.right
	left := len(r.fields) - right
	n := len(r.rsets)
	m := map[interface{}]interface{}{}
	ids := map[string]interface{}{}
	var g func([]interface{}, []rset2, int) error
	var match bool
	nf := len(r.fields)
	fields := append(append([]string(nil), r.fields[nf-right:]...), r.fields[:nf-right]...)
	g = func(prefix []interface{}, rsets []rset2, x int) (err error) {
		return rsets[0].do(ctx, func(id interface{}, in []interface{}) (bool, error) {
			ids[r.names[x]] = id
			row := append(prefix, in...)
			if len(rsets) > 1 {
				if len(rsets) == n {
					match = false
				}
				if err = g(row, rsets[1:], x+1); err != nil {
					return false, err
				}

				if len(rsets) != n || match {
					return true, nil
				}

				for i := 0; i < n-1; i++ {
					ids[r.names[i]] = nil
				}

				// rigth, left -> left, right
				return f(ids, append(make([]interface{}, left), row[:right]...))
			}

			for i, fld := range fields {
				if fld != "" {
					m[fld] = row[i]
				}
			}

			val, err := r.on.eval(ctx, m, ctx.arg)
			if err != nil {
				return false, err
			}

			if val == nil {
				return true, nil
			}

			x, ok := val.(bool)
			if !ok {
				return false, fmt.Errorf("invalid ON expression %s (value of type %T)", val, val)
			}

			if !x {
				return true, nil
			}

			match = true
			// rigth, left -> left, right
			return f(ids, append(append([]interface{}(nil), row[right:]...), row[:right]...))
		})
	}
	return g(nil, append([]rset2{r.rsets[n-1]}, r.rsets[:n-1]...), 0)
}

func (r *fullJoinRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error {
	b3 := b.TreeNew(func(a, b interface{}) int {
		x := a.(int64)
		y := b.(int64)
		if x < y {
			return -1
		}

		if x == y {
			return 0
		}

		return 1
	})
	m := map[interface{}]interface{}{}
	ids := map[string]interface{}{}
	var g func([]interface{}, []rset2, int) error
	var match bool
	var rid int64
	firstR := true
	g = func(prefix []interface{}, rsets []rset2, x int) (err error) {
		return rsets[0].do(ctx, func(id interface{}, in []interface{}) (bool, error) {
			ids[r.names[x]] = id
			row := append(prefix, in...)
			if len(rsets) > 1 {
				if len(rsets) == 2 {
					match = false
					rid = 0
				}
				if err = g(row, rsets[1:], x+1); err != nil {
					return false, err
				}

				if len(rsets) == 2 {
					firstR = false
				}
				if len(rsets) != 2 || match {
					return true, nil
				}

				ids[r.names[x+1]] = nil
				return f(ids, append(row, make([]interface{}, r.right)...))
			}

			rid++
			if firstR {
				b3.Set(rid, in)
			}
			for i, fld := range r.fields {
				if fld != "" {
					m[fld] = row[i]
				}
			}

			val, err := r.on.eval(ctx, m, ctx.arg)
			if err != nil {
				return false, err
			}

			if val == nil {
				return true, nil
			}

			x, ok := val.(bool)
			if !ok {
				return false, fmt.Errorf("invalid ON expression %s (value of type %T)", val, val)
			}

			if !x {
				return true, nil
			}

			match = true
			b3.Delete(rid)
			return f(ids, row)
		})
	}
	if err := g(nil, r.rsets, 0); err != nil {
		return err
	}

	it, err := b3.SeekFirst()
	if err != nil {
		return noEOF(err)
	}

	pref := make([]interface{}, len(r.fields)-r.right)
	for {
		_, v, err := it.Next()
		if err != nil {
			return noEOF(err)
		}

		more, err := f(nil, append(pref, v.([]interface{})...))
		if err != nil || !more {
			return err
		}
	}
}
