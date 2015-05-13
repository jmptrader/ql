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

	"github.com/cznic/strutil"
)

const (
	noNames = iota
	returnNames
	onlyNames
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
	//TODO- _ rset = (*selectRset)(nil)
	_ rset = (*selectStmt)(nil)
	_ rset = (*tableRset)(nil)
	_ rset = (*whereRset)(nil)

	_ rset2 = (*crossJoinRset2)(nil)
	_ rset2 = (*distinctRset2)(nil)
	_ rset2 = (*groupByRset2)(nil)
	//_ rset2 = (*limitRset2)(nil)
	//_ rset2 = (*offsetRset2)(nil)
	_ rset2 = (*orderByRset2)(nil)
	//_ rset2 = (*outerJoinRset2)(nil)
	_ rset2 = (*selectRset2)(nil)
	//_ rset2 = (*selectStmt2)(nil) // There's no selectStmt2.
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

// Do implements Recordset.
func (r recordset) Do(names bool, f func(data []interface{}) (more bool, err error)) (err error) {
	nm := noNames
	if names {
		nm = returnNames
	}
	return r.ctx.db.do(r, nm, f)
}

// Fields implements Recordset.
func (r recordset) Fields() (names []string, err error) {
	err = r.ctx.db.do(
		r,
		onlyNames,
		func(data []interface{}) (bool, error) {
			for _, v := range data {
				s, ok := v.(string)
				if !ok {
					return false, fmt.Errorf("got %T(%v), expected string (RecordSet.Fields)", v, v)
				}
				names = append(names, s)
			}
			return false, nil
		},
	)
	return
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
func (r recordset) Rows(limit, offset int) (rows [][]interface{}, err error) {
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

//TODO- func (r *groupByRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	t, err := ctx.db.store.CreateTemp(true)
//TODO- 	if err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	defer func() {
//TODO- 		if derr := t.Drop(); derr != nil && err == nil {
//TODO- 			err = derr
//TODO- 		}
//TODO- 	}()
//TODO-
//TODO- 	var flds []*fld
//TODO- 	var gcols []*col
//TODO- 	var cols []*col
//TODO- 	ok := false
//TODO- 	k := make([]interface{}, len(r.colNames)) //LATER optimize when len(r.cols) == 0
//TODO- 	if err = r.src.do(ctx, onlyNames, func(rid interface{}, in []interface{}) (more bool, err error) {
//TODO- 		if ok {
//TODO- 			infer(in, &cols)
//TODO- 			for i, c := range gcols {
//TODO- 				k[i] = in[c.index]
//TODO- 			}
//TODO- 			h0, err := t.Get(k)
//TODO- 			if err != nil {
//TODO- 				return false, err
//TODO- 			}
//TODO-
//TODO- 			var h int64
//TODO- 			if len(h0) != 0 {
//TODO- 				h, _ = h0[0].(int64)
//TODO- 			}
//TODO- 			nh, err := t.Create(append([]interface{}{h, nil}, in...)...)
//TODO- 			if err != nil {
//TODO- 				return false, err
//TODO- 			}
//TODO-
//TODO- 			for i, c := range gcols {
//TODO- 				k[i] = in[c.index]
//TODO- 			}
//TODO- 			err = t.Set(k, []interface{}{nh})
//TODO- 			if err != nil {
//TODO- 				return false, err
//TODO- 			}
//TODO-
//TODO- 			return true, nil
//TODO- 		}
//TODO-
//TODO- 		ok = true
//TODO- 		flds = in[0].([]*fld)
//TODO- 		for _, c := range r.colNames {
//TODO- 			i := findFldIndex(flds, c)
//TODO- 			if i < 0 {
//TODO- 				return false, fmt.Errorf("unknown column %s", c)
//TODO- 			}
//TODO-
//TODO- 			gcols = append(gcols, &col{name: c, index: i})
//TODO- 		}
//TODO- 		return !onlyNames, nil
//TODO- 	}); err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	if onlyNames {
//TODO- 		_, err := f(nil, []interface{}{flds})
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	it, err := t.SeekFirst()
//TODO- 	if err != nil {
//TODO- 		return noEOF(err)
//TODO- 	}
//TODO-
//TODO- 	for i, v := range flds {
//TODO- 		cols[i].name = v.name
//TODO- 		cols[i].index = i
//TODO- 	}
//TODO-
//TODO- 	var data []interface{}
//TODO- 	var more bool
//TODO- 	for more, err = f(nil, []interface{}{t, cols}); more && err == nil; more, err = f(nil, data) {
//TODO- 		_, data, err = it.Next()
//TODO- 		if err != nil {
//TODO- 			return noEOF(err)
//TODO- 		}
//TODO- 	}
//TODO- 	return err
//TODO- }

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

	for i, v := range r.src.fieldNames() {
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
// The only reliable way, in the general case, how to get field names of a
// recordset is to execute the Do method with the names parameter set to true.
// Any SELECT can return different fields on different runs, provided the
// columns of some of the underlying tables involved were altered in between
// and the query sports the SELECT * form.  Then the fields are not really
// known until the first query result row materializes.  The problem is that
// some queries can be costly even before that first row is computed.  If only
// the field names is what is required in some situation then executing such
// costly query could be prohibitively expensive.
//
// The Fields method provides an alternative. It computes the recordset fields
// while ignoring table data, WHERE clauses, predicates and without evaluating
// any expressions nor any functions.
//
// The result of Fields can be obviously imprecise if tables are altered before
// running Do later. In exchange, calling Fields is cheap - compared to
// actually computing a first row of a query having, say cross joins on n
// relations (1^n is always 1, n ∈ N).
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

//TODO- func (r *distinctRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	t, err := ctx.db.store.CreateTemp(true)
//TODO- 	if err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	defer func() {
//TODO- 		if derr := t.Drop(); derr != nil && err == nil {
//TODO- 			err = derr
//TODO- 		}
//TODO- 	}()
//TODO-
//TODO- 	var flds []*fld
//TODO- 	ok := false
//TODO- 	if err = r.src.do(ctx, onlyNames, func(id interface{}, in []interface{}) (more bool, err error) {
//TODO- 		if ok {
//TODO- 			if err = t.Set(in, nil); err != nil {
//TODO- 				return false, err
//TODO- 			}
//TODO-
//TODO- 			return true, nil
//TODO- 		}
//TODO-
//TODO- 		flds = in[0].([]*fld)
//TODO- 		ok = true
//TODO- 		return true && !onlyNames, nil
//TODO- 	}); err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	if onlyNames {
//TODO- 		_, err := f(nil, []interface{}{flds})
//TODO- 		return noEOF(err)
//TODO- 	}
//TODO-
//TODO- 	it, err := t.SeekFirst()
//TODO- 	if err != nil {
//TODO- 		return noEOF(err)
//TODO- 	}
//TODO-
//TODO- 	var data []interface{}
//TODO- 	var more bool
//TODO- 	for more, err = f(nil, []interface{}{flds}); more && err == nil; more, err = f(nil, data) {
//TODO- 		data, _, err = it.Next()
//TODO- 		if err != nil {
//TODO- 			return noEOF(err)
//TODO- 		}
//TODO- 	}
//TODO- 	return err
//TODO- }

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

//TODO- func (r *orderByRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	t, err := ctx.db.store.CreateTemp(r.asc)
//TODO- 	if err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	defer func() {
//TODO- 		if derr := t.Drop(); derr != nil && err == nil {
//TODO- 			err = derr
//TODO- 		}
//TODO- 	}()
//TODO-
//TODO- 	m := map[interface{}]interface{}{}
//TODO- 	var flds []*fld
//TODO- 	ok := false
//TODO- 	k := make([]interface{}, len(r.by)+1)
//TODO- 	id := int64(-1)
//TODO- 	if err = r.src.do(ctx, onlyNames, func(rid interface{}, in []interface{}) (more bool, err error) {
//TODO- 		id++
//TODO- 		if ok {
//TODO- 			for i, fld := range flds {
//TODO- 				if nm := fld.name; nm != "" {
//TODO- 					m[nm] = in[i]
//TODO- 				}
//TODO- 			}
//TODO- 			m["$id"] = rid
//TODO- 			for i, expr := range r.by {
//TODO- 				val, err := expr.eval(ctx, m, ctx.arg)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				if val != nil {
//TODO- 					val, ordered, err := isOrderedType(val)
//TODO- 					if err != nil {
//TODO- 						return false, err
//TODO- 					}
//TODO-
//TODO- 					if !ordered {
//TODO- 						return false, fmt.Errorf("cannot order by %v (type %T)", val, val)
//TODO-
//TODO- 					}
//TODO- 				}
//TODO-
//TODO- 				k[i] = val
//TODO- 			}
//TODO- 			k[len(r.by)] = id
//TODO- 			if err = t.Set(k, in); err != nil {
//TODO- 				return false, err
//TODO- 			}
//TODO-
//TODO- 			return true, nil
//TODO- 		}
//TODO-
//TODO- 		ok = true
//TODO- 		flds = in[0].([]*fld)
//TODO- 		return true && !onlyNames, nil
//TODO- 	}); err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	if onlyNames {
//TODO- 		_, err = f(nil, []interface{}{flds})
//TODO- 		return noEOF(err)
//TODO- 	}
//TODO-
//TODO- 	it, err := t.SeekFirst()
//TODO- 	if err != nil {
//TODO- 		if err != io.EOF {
//TODO- 			return err
//TODO- 		}
//TODO-
//TODO- 		_, err = f(nil, []interface{}{flds})
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	var data []interface{}
//TODO- 	var more bool
//TODO- 	for more, err = f(nil, []interface{}{flds}); more && err == nil; more, err = f(nil, data) {
//TODO- 		_, data, err = it.Next()
//TODO- 		if err != nil {
//TODO- 			return noEOF(err)
//TODO- 		}
//TODO- 	}
//TODO- 	return
//TODO- }

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

//TODO- func (r *whereRset) doIndexedBool(t *table, en indexIterator, v bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	m, err := f(nil, []interface{}{t.flds()})
//TODO- 	if !m || err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	for {
//TODO- 		k, h, err := en.Next()
//TODO- 		if err != nil {
//TODO- 			return noEOF(err)
//TODO- 		}
//TODO-
//TODO- 		switch x := k[0].(type) {
//TODO- 		case nil:
//TODO- 			panic("internal error 052") // nil should sort before true
//TODO- 		case bool:
//TODO- 			if x != v {
//TODO- 				return nil
//TODO- 			}
//TODO- 		default:
//TODO- 			panic("internal error 078")
//TODO- 		}
//TODO-
//TODO- 		if _, err := tableRset("").doOne(t, h, f); err != nil {
//TODO- 			return err
//TODO- 		}
//TODO- 	}
//TODO- }
//TODO-
//TODO- func (r *whereRset) tryBinOp(execCtx *execCtx, t *table, id *ident, v value, op int, f func(id interface{}, data []interface{}) (more bool, err error)) (bool, error) {
//TODO- 	c := findCol(t.cols0, id.s)
//TODO- 	if c == nil {
//TODO- 		return false, fmt.Errorf("undefined column: %s", id.s)
//TODO- 	}
//TODO-
//TODO- 	xCol := t.indices[c.index+1]
//TODO- 	if xCol == nil { // no index for this column
//TODO- 		return false, nil
//TODO- 	}
//TODO-
//TODO- 	data := []interface{}{v.val}
//TODO- 	cc := *c
//TODO- 	cc.index = 0
//TODO- 	if err := typeCheck(data, []*col{&cc}); err != nil {
//TODO- 		return true, err
//TODO- 	}
//TODO-
//TODO- 	v.val = data[0]
//TODO- 	ex := &binaryOperation{op, nil, v}
//TODO- 	switch op {
//TODO- 	case '<', le:
//TODO- 		v.val = false // first value collating after nil
//TODO- 		fallthrough
//TODO- 	case eq, ge:
//TODO- 		m, err := f(nil, []interface{}{t.flds()})
//TODO- 		if !m || err != nil {
//TODO- 			return true, err
//TODO- 		}
//TODO-
//TODO- 		en, _, err := xCol.x.Seek([]interface{}{v.val})
//TODO- 		if err != nil {
//TODO- 			return true, noEOF(err)
//TODO- 		}
//TODO-
//TODO- 		for {
//TODO- 			k, h, err := en.Next()
//TODO- 			if k == nil {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			if err != nil {
//TODO- 				return true, noEOF(err)
//TODO- 			}
//TODO-
//TODO- 			ex.l = value{k[0]}
//TODO- 			eval, err := ex.eval(execCtx, nil, nil)
//TODO- 			if err != nil {
//TODO- 				return true, err
//TODO- 			}
//TODO-
//TODO- 			if !eval.(bool) {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			if _, err := tableRset("").doOne(t, h, f); err != nil {
//TODO- 				return true, err
//TODO- 			}
//TODO- 		}
//TODO- 	case '>':
//TODO- 		m, err := f(nil, []interface{}{t.flds()})
//TODO- 		if !m || err != nil {
//TODO- 			return true, err
//TODO- 		}
//TODO-
//TODO- 		en, err := xCol.x.SeekLast()
//TODO- 		if err != nil {
//TODO- 			return true, noEOF(err)
//TODO- 		}
//TODO-
//TODO- 		for {
//TODO- 			k, h, err := en.Prev()
//TODO- 			if k == nil {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			if err != nil {
//TODO- 				return true, noEOF(err)
//TODO- 			}
//TODO-
//TODO- 			ex.l = value{k[0]}
//TODO- 			eval, err := ex.eval(execCtx, nil, nil)
//TODO- 			if err != nil {
//TODO- 				return true, err
//TODO- 			}
//TODO-
//TODO- 			if !eval.(bool) {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			if _, err := tableRset("").doOne(t, h, f); err != nil {
//TODO- 				return true, err
//TODO- 			}
//TODO- 		}
//TODO- 	default:
//TODO- 		panic("internal error 053")
//TODO- 	}
//TODO- }
//TODO-
//TODO- func (r *whereRset) tryBinOpID(execCtx *execCtx, t *table, v value, op int, f func(id interface{}, data []interface{}) (more bool, err error)) (bool, error) {
//TODO- 	xCol := t.indices[0]
//TODO- 	if xCol == nil { // no index for id()
//TODO- 		return false, nil
//TODO- 	}
//TODO-
//TODO- 	data := []interface{}{v.val}
//TODO- 	if err := typeCheck(data, []*col{&col{typ: qInt64}}); err != nil {
//TODO- 		return true, err
//TODO- 	}
//TODO-
//TODO- 	v.val = data[0]
//TODO- 	ex := &binaryOperation{op, nil, v}
//TODO- 	switch op {
//TODO- 	case '<', le:
//TODO- 		v.val = int64(1)
//TODO- 		fallthrough
//TODO- 	case eq, ge:
//TODO- 		m, err := f(nil, []interface{}{t.flds()})
//TODO- 		if !m || err != nil {
//TODO- 			return true, err
//TODO- 		}
//TODO-
//TODO- 		en, _, err := xCol.x.Seek([]interface{}{v.val})
//TODO- 		if err != nil {
//TODO- 			return true, noEOF(err)
//TODO- 		}
//TODO-
//TODO- 		for {
//TODO- 			k, h, err := en.Next()
//TODO- 			if k == nil {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			if err != nil {
//TODO- 				return true, noEOF(err)
//TODO- 			}
//TODO-
//TODO- 			ex.l = value{k[0]}
//TODO- 			eval, err := ex.eval(execCtx, nil, nil)
//TODO- 			if err != nil {
//TODO- 				return true, err
//TODO- 			}
//TODO-
//TODO- 			if !eval.(bool) {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			if _, err := tableRset("").doOne(t, h, f); err != nil {
//TODO- 				return true, err
//TODO- 			}
//TODO- 		}
//TODO- 	case '>':
//TODO- 		m, err := f(nil, []interface{}{t.flds()})
//TODO- 		if !m || err != nil {
//TODO- 			return true, err
//TODO- 		}
//TODO-
//TODO- 		en, err := xCol.x.SeekLast()
//TODO- 		if err != nil {
//TODO- 			return true, noEOF(err)
//TODO- 		}
//TODO-
//TODO- 		for {
//TODO- 			k, h, err := en.Prev()
//TODO- 			if k == nil {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			if err != nil {
//TODO- 				return true, noEOF(err)
//TODO- 			}
//TODO-
//TODO- 			ex.l = value{k[0]}
//TODO- 			eval, err := ex.eval(execCtx, nil, nil)
//TODO- 			if err != nil {
//TODO- 				return true, err
//TODO- 			}
//TODO-
//TODO- 			if !eval.(bool) {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			if _, err := tableRset("").doOne(t, h, f); err != nil {
//TODO- 				return true, err
//TODO- 			}
//TODO- 		}
//TODO- 	default:
//TODO- 		panic("internal error 071")
//TODO- 	}
//TODO- }
//TODO-
//TODO- func (r *whereRset) tryUseIndex(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) (bool, error) {
//TODO- 	//TODO(indices) support IS [NOT] NULL
//TODO- 	c, ok := r.src.(*crossJoinRset)
//TODO- 	if !ok {
//TODO- 		return false, nil
//TODO- 	}
//TODO-
//TODO- 	tabName, ok := c.isSingleTable()
//TODO- 	if !ok || isSystemName[tabName] {
//TODO- 		return false, nil
//TODO- 	}
//TODO-
//TODO- 	t := ctx.db.root.tables[tabName]
//TODO- 	if t == nil {
//TODO- 		return true, fmt.Errorf("table %s does not exist", tabName)
//TODO- 	}
//TODO-
//TODO- 	if !t.hasIndices() {
//TODO- 		return false, nil
//TODO- 	}
//TODO-
//TODO- 	//LATER WHERE column1 boolOp column2 ...
//TODO- 	//LATER WHERE !column (rewritable as: column == false)
//TODO- 	switch ex := r.expr.(type) {
//TODO- 	case *unaryOperation: // WHERE !column
//TODO- 		if ex.op != '!' {
//TODO- 			return false, nil
//TODO- 		}
//TODO-
//TODO- 		switch operand := ex.v.(type) {
//TODO- 		case *ident:
//TODO- 			c := findCol(t.cols0, operand.s)
//TODO- 			if c == nil { // no such column
//TODO- 				return false, fmt.Errorf("unknown column %s", ex)
//TODO- 			}
//TODO-
//TODO- 			if c.typ != qBool { // not a bool column
//TODO- 				return false, nil
//TODO- 			}
//TODO-
//TODO- 			xCol := t.indices[c.index+1]
//TODO- 			if xCol == nil { // column isn't indexed
//TODO- 				return false, nil
//TODO- 			}
//TODO-
//TODO- 			en, _, err := xCol.x.Seek([]interface{}{false})
//TODO- 			if err != nil {
//TODO- 				return false, noEOF(err)
//TODO- 			}
//TODO-
//TODO- 			return true, r.doIndexedBool(t, en, false, f)
//TODO- 		default:
//TODO- 			return false, nil
//TODO- 		}
//TODO- 	case *ident: // WHERE column
//TODO- 		c := findCol(t.cols0, ex.s)
//TODO- 		if c == nil { // no such column
//TODO- 			return false, fmt.Errorf("unknown column %s", ex)
//TODO- 		}
//TODO-
//TODO- 		if c.typ != qBool { // not a bool column
//TODO- 			return false, nil
//TODO- 		}
//TODO-
//TODO- 		xCol := t.indices[c.index+1]
//TODO- 		if xCol == nil { // column isn't indexed
//TODO- 			return false, nil
//TODO- 		}
//TODO-
//TODO- 		en, _, err := xCol.x.Seek([]interface{}{true})
//TODO- 		if err != nil {
//TODO- 			return false, noEOF(err)
//TODO- 		}
//TODO-
//TODO- 		return true, r.doIndexedBool(t, en, true, f)
//TODO- 	case *binaryOperation:
//TODO- 		//DONE handle id()
//TODO- 		var invOp int
//TODO- 		switch ex.op {
//TODO- 		case '<':
//TODO- 			invOp = '>'
//TODO- 		case le:
//TODO- 			invOp = ge
//TODO- 		case eq:
//TODO- 			invOp = eq
//TODO- 		case '>':
//TODO- 			invOp = '<'
//TODO- 		case ge:
//TODO- 			invOp = le
//TODO- 		default:
//TODO- 			return false, nil
//TODO- 		}
//TODO-
//TODO- 		switch lhs := ex.l.(type) {
//TODO- 		case *call:
//TODO- 			if !(lhs.f == "id" && len(lhs.arg) == 0) {
//TODO- 				return false, nil
//TODO- 			}
//TODO-
//TODO- 			switch rhs := ex.r.(type) {
//TODO- 			case parameter:
//TODO- 				v, err := rhs.eval(ctx, nil, ctx.arg)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				return r.tryBinOpID(ctx, t, value{v}, ex.op, f)
//TODO- 			case value:
//TODO- 				return r.tryBinOpID(ctx, t, rhs, ex.op, f)
//TODO- 			default:
//TODO- 				return false, nil
//TODO- 			}
//TODO- 		case *ident:
//TODO- 			switch rhs := ex.r.(type) {
//TODO- 			case parameter:
//TODO- 				v, err := rhs.eval(ctx, nil, ctx.arg)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				return r.tryBinOp(ctx, t, lhs, value{v}, ex.op, f)
//TODO- 			case value:
//TODO- 				return r.tryBinOp(ctx, t, lhs, rhs, ex.op, f)
//TODO- 			default:
//TODO- 				return false, nil
//TODO- 			}
//TODO- 		case parameter:
//TODO- 			switch rhs := ex.r.(type) {
//TODO- 			case *call:
//TODO- 				if !(rhs.f == "id" && len(rhs.arg) == 0) {
//TODO- 					return false, nil
//TODO- 				}
//TODO-
//TODO- 				v, err := lhs.eval(ctx, nil, ctx.arg)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				return r.tryBinOpID(ctx, t, value{v}, invOp, f)
//TODO- 			case *ident:
//TODO- 				v, err := lhs.eval(ctx, nil, ctx.arg)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				return r.tryBinOp(ctx, t, rhs, value{v}, invOp, f)
//TODO- 			default:
//TODO- 				return false, nil
//TODO- 			}
//TODO- 		case value:
//TODO- 			switch rhs := ex.r.(type) {
//TODO- 			case *call:
//TODO- 				if !(rhs.f == "id" && len(rhs.arg) == 0) {
//TODO- 					return false, nil
//TODO- 				}
//TODO-
//TODO- 				return r.tryBinOpID(ctx, t, lhs, invOp, f)
//TODO- 			case *ident:
//TODO- 				return r.tryBinOp(ctx, t, rhs, lhs, invOp, f)
//TODO- 			default:
//TODO- 				return false, nil
//TODO- 			}
//TODO- 		default:
//TODO- 			return false, nil
//TODO- 		}
//TODO- 	default:
//TODO- 		return false, nil
//TODO- 	}
//TODO- }
//TODO-
//TODO- func (r *whereRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	//dbg("====")
//TODO- 	if !onlyNames {
//TODO- 		if ok, err := r.tryUseIndex(ctx, f); ok || err != nil {
//TODO- 			//dbg("ok %t, err %v", ok, err)
//TODO- 			return err
//TODO- 		}
//TODO- 	}
//TODO-
//TODO- 	//dbg("not using indices")
//TODO- 	m := map[interface{}]interface{}{}
//TODO- 	var flds []*fld
//TODO- 	ok := false
//TODO- 	return r.src.do(ctx, onlyNames, func(rid interface{}, in []interface{}) (more bool, err error) {
//TODO- 		if ok {
//TODO- 			for i, fld := range flds {
//TODO- 				if nm := fld.name; nm != "" {
//TODO- 					m[nm] = in[i]
//TODO- 				}
//TODO- 			}
//TODO- 			m["$id"] = rid
//TODO- 			val, err := r.expr.eval(ctx, m, ctx.arg)
//TODO- 			if err != nil {
//TODO- 				return false, err
//TODO- 			}
//TODO-
//TODO- 			if val == nil {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			x, ok := val.(bool)
//TODO- 			if !ok {
//TODO- 				return false, fmt.Errorf("invalid WHERE expression %s (value of type %T)", val, val)
//TODO- 			}
//TODO-
//TODO- 			if !x {
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			return f(rid, in)
//TODO- 		}
//TODO-
//TODO- 		flds = in[0].([]*fld)
//TODO- 		ok = true
//TODO- 		m, err := f(nil, in)
//TODO- 		return m && !onlyNames, err
//TODO- 	})
//TODO- }

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
	src  rset
}

//TODO- func (r *offsetRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	m := map[interface{}]interface{}{}
//TODO- 	var flds []*fld
//TODO- 	var ok, eval bool
//TODO- 	var off uint64
//TODO- 	return r.src.do(ctx, onlyNames, func(rid interface{}, in []interface{}) (more bool, err error) {
//TODO- 		if ok {
//TODO- 			if !eval {
//TODO- 				for i, fld := range flds {
//TODO- 					if nm := fld.name; nm != "" {
//TODO- 						m[nm] = in[i]
//TODO- 					}
//TODO- 				}
//TODO- 				m["$id"] = rid
//TODO- 				val, err := r.expr.eval(ctx, m, ctx.arg)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				if val == nil {
//TODO- 					return true, nil
//TODO- 				}
//TODO-
//TODO- 				if off, err = limOffExpr(val); err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				eval = true
//TODO- 			}
//TODO- 			if off > 0 {
//TODO- 				off--
//TODO- 				return true, nil
//TODO- 			}
//TODO-
//TODO- 			return f(rid, in)
//TODO- 		}
//TODO-
//TODO- 		flds = in[0].([]*fld)
//TODO- 		ok = true
//TODO- 		m, err := f(nil, in)
//TODO- 		return m && !onlyNames, err
//TODO- 	})
//TODO- }

func (r *offsetRset) plan(ctx *execCtx) (rset2, error) { panic("TODO") }

type limitRset struct {
	expr expression
	src  rset
}

//TODO- func (r *limitRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	m := map[interface{}]interface{}{}
//TODO- 	var flds []*fld
//TODO- 	var ok, eval bool
//TODO- 	var lim uint64
//TODO- 	return r.src.do(ctx, onlyNames, func(rid interface{}, in []interface{}) (more bool, err error) {
//TODO- 		if ok {
//TODO- 			if !eval {
//TODO- 				for i, fld := range flds {
//TODO- 					if nm := fld.name; nm != "" {
//TODO- 						m[nm] = in[i]
//TODO- 					}
//TODO- 				}
//TODO- 				m["$id"] = rid
//TODO- 				val, err := r.expr.eval(ctx, m, ctx.arg)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				if val == nil {
//TODO- 					return true, nil
//TODO- 				}
//TODO-
//TODO- 				if lim, err = limOffExpr(val); err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				eval = true
//TODO- 			}
//TODO- 			switch lim {
//TODO- 			case 0:
//TODO- 				return false, nil
//TODO- 			default:
//TODO- 				lim--
//TODO- 				return f(rid, in)
//TODO- 			}
//TODO- 		}
//TODO-
//TODO- 		flds = in[0].([]*fld)
//TODO- 		ok = true
//TODO- 		m, err := f(nil, in)
//TODO- 		return m && !onlyNames, err
//TODO- 	})
//TODO- }

func (r *limitRset) plan(ctx *execCtx) (rset2, error) { panic("TODO") }

//TODO- type selectRset struct {
//TODO- 	flds []*fld
//TODO- 	src  rset2
//TODO- }

//TODO- func (r *selectRset) doGroup(grp *groupByRset, ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	if onlyNames {
//TODO- 		if len(r.flds) != 0 {
//TODO- 			_, err := f(nil, []interface{}{r.flds})
//TODO- 			return err
//TODO- 		}
//TODO-
//TODO- 		return grp.do(ctx, true, f)
//TODO- 	}
//TODO-
//TODO- 	var t temp
//TODO- 	var cols []*col
//TODO- 	out := make([]interface{}, len(r.flds))
//TODO- 	ok := false
//TODO- 	rows := 0
//TODO- 	if err = r.src.do(ctx, onlyNames, func(rid interface{}, in []interface{}) (more bool, err error) {
//TODO- 		if ok {
//TODO- 			h := in[0].(int64)
//TODO- 			m := map[interface{}]interface{}{}
//TODO- 			for h != 0 {
//TODO- 				in, err = t.Read(nil, h, cols...)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				rec := in[2:]
//TODO- 				for i, c := range cols {
//TODO- 					if nm := c.name; nm != "" {
//TODO- 						m[nm] = rec[i]
//TODO- 					}
//TODO- 				}
//TODO- 				m["$id"] = rid
//TODO- 				for _, fld := range r.flds {
//TODO- 					if _, err = fld.expr.eval(ctx, m, ctx.arg); err != nil {
//TODO- 						return false, err
//TODO- 					}
//TODO- 				}
//TODO-
//TODO- 				h = in[0].(int64)
//TODO- 			}
//TODO- 			m["$agg"] = true
//TODO- 			for i, fld := range r.flds {
//TODO- 				if out[i], err = fld.expr.eval(ctx, m, ctx.arg); err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO- 			}
//TODO- 			rows++
//TODO- 			return f(nil, out)
//TODO- 		}
//TODO-
//TODO- 		ok = true
//TODO- 		rows++
//TODO- 		t = in[0].(temp)
//TODO- 		cols = in[1].([]*col)
//TODO- 		if len(r.flds) == 0 {
//TODO- 			r.flds = make([]*fld, len(cols))
//TODO- 			for i, v := range cols {
//TODO- 				r.flds[i] = &fld{expr: &ident{v.name}, name: v.name}
//TODO- 			}
//TODO- 			out = make([]interface{}, len(r.flds))
//TODO- 		}
//TODO- 		m, err := f(nil, []interface{}{r.flds})
//TODO- 		return m && !onlyNames, err
//TODO- 	}); err != nil || onlyNames {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	switch rows {
//TODO- 	case 0:
//TODO- 		more, err := f(nil, []interface{}{r.flds})
//TODO- 		if !more || err != nil {
//TODO- 			return err
//TODO- 		}
//TODO-
//TODO- 		fallthrough
//TODO- 	case 1:
//TODO- 		m := map[interface{}]interface{}{"$agg0": true} // aggregate empty record set
//TODO- 		for i, fld := range r.flds {
//TODO- 			if out[i], err = fld.expr.eval(ctx, m, ctx.arg); err != nil {
//TODO- 				return
//TODO- 			}
//TODO- 		}
//TODO- 		_, err = f(nil, out)
//TODO- 	}
//TODO- 	return
//TODO- }
//TODO-
//TODO- func (r *selectRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	if grp, ok := r.src.(*groupByRset); ok {
//TODO- 		return r.doGroup(grp, ctx, onlyNames, f)
//TODO- 	}
//TODO-
//TODO- 	if len(r.flds) == 0 {
//TODO- 		return r.src.do(ctx, onlyNames, f)
//TODO- 	}
//TODO-
//TODO- 	if onlyNames {
//TODO- 		_, err := f(nil, []interface{}{r.flds})
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	var flds []*fld
//TODO- 	m := map[interface{}]interface{}{}
//TODO- 	ok := false
//TODO- 	return r.src.do(ctx, onlyNames, func(rid interface{}, in []interface{}) (more bool, err error) {
//TODO- 		if ok {
//TODO- 			for i, fld := range flds {
//TODO- 				if nm := fld.name; nm != "" {
//TODO- 					m[nm] = in[i]
//TODO- 				}
//TODO- 			}
//TODO- 			m["$id"] = rid
//TODO- 			out := make([]interface{}, len(r.flds))
//TODO- 			for i, fld := range r.flds {
//TODO- 				if out[i], err = fld.expr.eval(ctx, m, ctx.arg); err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO- 			}
//TODO- 			m, err := f(rid, out)
//TODO- 			return m, err
//TODO-
//TODO- 		}
//TODO-
//TODO- 		ok = true
//TODO- 		flds = in[0].([]*fld)
//TODO- 		m, err := f(nil, []interface{}{r.flds})
//TODO- 		return m && !onlyNames, err
//TODO- 	})
//TODO- }

type selectRset2 struct {
	flds   []*fld
	src    rset2
	fields []string
}

func (r *selectRset2) plan(ctx *execCtx) (rset2, error) {
	if len(r.flds) == 0 {
		return r.src, nil
	}

	r.fields = make([]string, len(r.flds))
	for i, v := range r.flds {
		r.fields[i] = v.name
	}
	return r, nil
}

func (r *selectRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	if _, ok := r.src.(*groupByRset2); ok { //TODO different plan, also possible conflict with optimizer
		return r.doGroup(ctx, f)
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

//TODO- func (r tableRset) doIndex(xname string, x btreeIndex, ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	flds := []*fld{&fld{name: xname}}
//TODO- 	m, err := f(nil, []interface{}{flds})
//TODO- 	if onlyNames {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	if !m || err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	en, err := x.SeekFirst()
//TODO- 	if err != nil {
//TODO- 		return noEOF(err)
//TODO- 	}
//TODO-
//TODO- 	var id int64
//TODO- 	for {
//TODO- 		k, _, err := en.Next()
//TODO- 		if err != nil {
//TODO- 			return noEOF(err)
//TODO- 		}
//TODO-
//TODO- 		id++
//TODO- 		m, err := f(id, k)
//TODO- 		if !m || err != nil {
//TODO- 			return err
//TODO- 		}
//TODO- 	}
//TODO- }
//TODO-
//TODO- func (tableRset) doOne(t *table, h int64, f func(id interface{}, data []interface{}) (more bool, err error)) ( /* next handle */ int64, error) {
//TODO- 	cols := t.cols
//TODO- 	ncols := len(cols)
//TODO- 	rec, err := t.store.Read(nil, h, cols...)
//TODO- 	if err != nil {
//TODO- 		return -1, err
//TODO- 	}
//TODO-
//TODO- 	h = rec[0].(int64)
//TODO- 	if n := ncols + 2 - len(rec); n > 0 {
//TODO- 		rec = append(rec, make([]interface{}, n)...)
//TODO- 	}
//TODO-
//TODO- 	for i, c := range cols {
//TODO- 		if x := c.index; 2+x < len(rec) {
//TODO- 			rec[2+i] = rec[2+x]
//TODO- 			continue
//TODO- 		}
//TODO-
//TODO- 		rec[2+i] = nil //DONE +test (#571)
//TODO- 	}
//TODO- 	m, err := f(rec[1], rec[2:2+ncols]) // 0:next, 1:id
//TODO- 	if !m || err != nil {
//TODO- 		return -1, err
//TODO- 	}
//TODO-
//TODO- 	return h, nil
//TODO- }
//TODO-
//TODO- func (r tableRset) doSysTable(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	flds := []*fld{&fld{name: "Name"}, &fld{name: "Schema"}}
//TODO- 	m, err := f(nil, []interface{}{flds})
//TODO- 	if onlyNames {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	if !m || err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	rec := make([]interface{}, 2)
//TODO- 	di, err := ctx.db.info()
//TODO- 	if err != nil {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	var id int64
//TODO- 	for _, ti := range di.Tables {
//TODO- 		rec[0] = ti.Name
//TODO- 		a := []string{}
//TODO- 		for _, ci := range ti.Columns {
//TODO- 			s := ""
//TODO- 			if ci.NotNull {
//TODO- 				s += " NOT NULL"
//TODO- 			}
//TODO- 			if c := ci.Constraint; c != "" {
//TODO- 				s += " " + c
//TODO- 			}
//TODO- 			if d := ci.Default; d != "" {
//TODO- 				s += " DEFAULT " + d
//TODO- 			}
//TODO- 			a = append(a, fmt.Sprintf("%s %s%s", ci.Name, ci.Type, s))
//TODO- 		}
//TODO- 		rec[1] = fmt.Sprintf("CREATE TABLE %s (%s);", ti.Name, strings.Join(a, ", "))
//TODO- 		id++
//TODO- 		m, err := f(id, rec)
//TODO- 		if !m || err != nil {
//TODO- 			return err
//TODO- 		}
//TODO- 	}
//TODO- 	return
//TODO- }
//TODO-
//TODO- func (r tableRset) doSysColumn(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	flds := []*fld{&fld{name: "TableName"}, &fld{name: "Ordinal"}, &fld{name: "Name"}, &fld{name: "Type"}}
//TODO- 	m, err := f(nil, []interface{}{flds})
//TODO- 	if onlyNames {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	if !m || err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	rec := make([]interface{}, 4)
//TODO- 	di, err := ctx.db.info()
//TODO- 	if err != nil {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	var id int64
//TODO- 	for _, ti := range di.Tables {
//TODO- 		rec[0] = ti.Name
//TODO- 		var ix int64
//TODO- 		for _, ci := range ti.Columns {
//TODO- 			ix++
//TODO- 			rec[1] = ix
//TODO- 			rec[2] = ci.Name
//TODO- 			rec[3] = ci.Type.String()
//TODO- 			id++
//TODO- 			m, err := f(id, rec)
//TODO- 			if !m || err != nil {
//TODO- 				return err
//TODO- 			}
//TODO- 		}
//TODO- 	}
//TODO- 	return
//TODO- }
//TODO-
//TODO- func (r tableRset) doSysIndex(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	flds := []*fld{&fld{name: "TableName"}, &fld{name: "ColumnName"}, &fld{name: "Name"}, &fld{name: "IsUnique"}}
//TODO- 	m, err := f(nil, []interface{}{flds})
//TODO- 	if onlyNames {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	if !m || err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	rec := make([]interface{}, 4)
//TODO- 	di, err := ctx.db.info()
//TODO- 	if err != nil {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	var id int64
//TODO- 	for _, xi := range di.Indices {
//TODO- 		rec[0] = xi.Table
//TODO- 		rec[1] = xi.Column
//TODO- 		rec[2] = xi.Name
//TODO- 		rec[3] = xi.Unique
//TODO- 		id++
//TODO- 		m, err := f(id, rec)
//TODO- 		if !m || err != nil {
//TODO- 			return err
//TODO- 		}
//TODO- 	}
//TODO- 	return
//TODO- }
//TODO-
//TODO- func (r tableRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	switch r {
//TODO- 	case "__Table":
//TODO- 		return r.doSysTable(ctx, onlyNames, f)
//TODO- 	case "__Column":
//TODO- 		return r.doSysColumn(ctx, onlyNames, f)
//TODO- 	case "__Index":
//TODO- 		return r.doSysIndex(ctx, onlyNames, f)
//TODO- 	}
//TODO-
//TODO- 	t, ok := ctx.db.root.tables[string(r)]
//TODO- 	if !ok && isTesting {
//TODO- 		if _, x0 := ctx.db.root.findIndexByName(string(r)); x0 != nil {
//TODO- 			switch x := x0.(type) {
//TODO- 			case *indexedCol:
//TODO- 				return r.doIndex(x.name, x.x, ctx, onlyNames, f)
//TODO- 			case *index2:
//TODO- 				return r.doIndex(string(r), x.x, ctx, onlyNames, f)
//TODO- 			default:
//TODO- 				panic("internal error 079")
//TODO- 			}
//TODO- 		}
//TODO- 	}
//TODO-
//TODO- 	if !ok {
//TODO- 		return fmt.Errorf("table %s does not exist", r)
//TODO- 	}
//TODO-
//TODO- 	m, err := f(nil, []interface{}{t.flds()})
//TODO- 	if onlyNames {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	if !m || err != nil {
//TODO- 		return
//TODO- 	}
//TODO-
//TODO- 	for h := t.head; h > 0 && err == nil; h, err = r.doOne(t, h, f) {
//TODO- 	}
//TODO- 	return
//TODO- }

func (r tableRset) plan(ctx *execCtx) (rset2, error) {
	switch r {
	case "__Table":
		panic("TODO")
	case "__Column":
		panic("TODO")
	case "__Index":
		panic("TODO")
	}

	t, ok := ctx.db.root.tables[string(r)]
	if !ok && isTesting {
		if _, x0 := ctx.db.root.findIndexByName(string(r)); x0 != nil {
			panic("TODO")
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

//TODO- func (r *crossJoinRset) tables() []struct {
//TODO- 	i            int
//TODO- 	name, rename string
//TODO- } {
//TODO- 	var ret []struct {
//TODO- 		i            int
//TODO- 		name, rename string
//TODO- 	}
//TODO- 	//dbg("---- %p", r)
//TODO- 	for i, pair0 := range r.sources {
//TODO- 		//dbg("%d/%d, %#v", i, len(r.sources), pair0)
//TODO- 		pair := pair0.([]interface{})
//TODO- 		altName := pair[1].(string)
//TODO- 		switch x := pair[0].(type) {
//TODO- 		case string: // table name
//TODO- 			if altName == "" {
//TODO- 				altName = x
//TODO- 			}
//TODO- 			ret = append(ret, struct {
//TODO- 				i            int
//TODO- 				name, rename string
//TODO- 			}{i, x, altName})
//TODO- 		}
//TODO- 	}
//TODO- 	return ret
//TODO- }

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

//TODO- func (r *crossJoinRset) isSingleTable() (string, bool) {
//TODO- 	sources := r.sources
//TODO- 	if len(sources) != 1 {
//TODO- 		return "", false
//TODO- 	}
//TODO-
//TODO- 	pair := sources[0].([]interface{})
//TODO- 	s, ok := pair[0].(string)
//TODO- 	return s, ok
//TODO- }
//TODO-
//TODO- func (r *crossJoinRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
//TODO- 	rsets := make([]rset, len(r.sources))
//TODO- 	altNames := make([]string, len(r.sources))
//TODO- 	//dbg(".... %p", r)
//TODO- 	for i, pair0 := range r.sources {
//TODO- 		pair := pair0.([]interface{})
//TODO- 		//dbg("%d: %#v", len(pair), pair)
//TODO- 		altName := pair[1].(string)
//TODO- 		switch x := pair[0].(type) {
//TODO- 		case string: // table name
//TODO- 			rsets[i] = tableRset(x)
//TODO- 			if altName == "" {
//TODO- 				altName = x
//TODO- 			}
//TODO- 		case *selectStmt:
//TODO- 			rsets[i] = x
//TODO- 		default:
//TODO- 			log.Panic("internal error 055")
//TODO- 		}
//TODO- 		altNames[i] = altName
//TODO- 	}
//TODO-
//TODO- 	if len(rsets) == 1 {
//TODO- 		return rsets[0].do(ctx, onlyNames, f)
//TODO- 	}
//TODO-
//TODO- 	var flds []*fld
//TODO- 	fldsSent := false
//TODO- 	iq := 0
//TODO- 	stop := false
//TODO- 	ids := map[string]interface{}{}
//TODO- 	var g func([]interface{}, []rset, int) error
//TODO- 	g = func(prefix []interface{}, rsets []rset, x int) (err error) {
//TODO- 		rset := rsets[0]
//TODO- 		rsets = rsets[1:]
//TODO- 		ok := false
//TODO- 		return rset.do(ctx, onlyNames, func(id interface{}, in []interface{}) (more bool, err error) {
//TODO- 			if onlyNames && fldsSent {
//TODO- 				stop = true
//TODO- 				return false, nil
//TODO- 			}
//TODO-
//TODO- 			if ok {
//TODO- 				ids[altNames[x]] = id
//TODO- 				if len(rsets) != 0 {
//TODO- 					return true, g(append(prefix, in...), rsets, x+1)
//TODO- 				}
//TODO-
//TODO- 				m, err := f(ids, append(prefix, in...))
//TODO- 				if !m {
//TODO- 					stop = true
//TODO- 				}
//TODO- 				return m && !stop, err
//TODO- 			}
//TODO-
//TODO- 			ok = true
//TODO- 			if !fldsSent {
//TODO- 				f0 := append([]*fld(nil), in[0].([]*fld)...)
//TODO- 				q := altNames[iq]
//TODO- 				for i, elem := range f0 {
//TODO- 					nf := &fld{}
//TODO- 					*nf = *elem
//TODO- 					switch {
//TODO- 					case q == "":
//TODO- 						nf.name = ""
//TODO- 					case nf.name != "":
//TODO- 						nf.name = fmt.Sprintf("%s.%s", altNames[iq], nf.name)
//TODO- 					}
//TODO- 					f0[i] = nf
//TODO- 				}
//TODO- 				iq++
//TODO- 				flds = append(flds, f0...)
//TODO- 			}
//TODO- 			if len(rsets) == 0 && !fldsSent {
//TODO- 				fldsSent = true
//TODO- 				more, err = f(nil, []interface{}{flds})
//TODO- 				if !more {
//TODO- 					stop = true
//TODO- 				}
//TODO- 				return more && !stop, err
//TODO- 			}
//TODO-
//TODO- 			return !stop, nil
//TODO- 		})
//TODO- 	}
//TODO- 	return g(nil, rsets, 0)
//TODO- }

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
		if rs2, err = src.(rset).plan(ctx); err != nil {
			return nil, err
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
		//dbg("r2.fields %v", r2.fields)
		r2.rsets[i] = rs2
	}
	if len(r2.rsets) == 1 {
		return r2.rsets[0], nil
	}

	return &r2, nil
}

type crossJoinRset2 struct {
	rsets  []rset2
	names  []string
	fields []string
}

func (r *crossJoinRset2) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
	ids := map[string]interface{}{}
	var g func([]interface{}, []rset2, int) error
	g = func(prefix []interface{}, rsets []rset2, x int) (err error) {
		rset := rsets[0]
		rsets = rsets[1:]
		return rset.do(ctx, func(id interface{}, in []interface{}) (bool, error) {
			ids[r.names[x]] = id
			if len(rsets) != 0 {
				return true, g(append(prefix, in...), rsets, x+1)
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

func (db *DB) do(r recordset, names int, f func(data []interface{}) (bool, error)) (err error) {
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

	panic("TODO")
	//ok := false
	//return r.do(r.ctx, names == onlyNames, func(id interface{}, data []interface{}) (more bool, err error) {
	//	if ok {
	//		if err = expand(data); err != nil {
	//			return
	//		}

	//		return f(data)
	//	}

	//	ok = true
	//	done := false
	//	switch names {
	//	case noNames:
	//		return true, nil
	//	case onlyNames:
	//		done = true
	//		fallthrough
	//	default: // returnNames
	//		flds := data[0].([]*fld)
	//		a := make([]interface{}, len(flds))
	//		for i, v := range flds {
	//			a[i] = v.name
	//		}
	//		more, err := f(a)
	//		return more && !done, err

	//	}
	//})
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
	typ       int // leftJoin, rightJoin, fullJoin
	crossJoin *crossJoinRset
	source    []interface{}
	on        expression
}

//TODO- func (o *outerJoinRset) do(ctx *execCtx, onlyNames bool, f func(id interface{}, data []interface{}) (more bool, err error)) error {
//TODO- 	sources := append(o.crossJoin.sources, o.source)
//TODO- 	var b3 *b.Tree
//TODO- 	switch o.typ {
//TODO- 	case rightJoin: // switch last two sources
//TODO- 		n := len(sources)
//TODO- 		sources[n-2], sources[n-1] = sources[n-1], sources[n-2]
//TODO- 	case fullJoin:
//TODO- 		b3 = b.TreeNew(func(a, b interface{}) int {
//TODO- 			x := a.(int64)
//TODO- 			y := b.(int64)
//TODO- 			if x < y {
//TODO- 				return -1
//TODO- 			}
//TODO-
//TODO- 			if x == y {
//TODO- 				return 0
//TODO- 			}
//TODO-
//TODO- 			return 1
//TODO- 		})
//TODO- 	}
//TODO- 	rsets := make([]rset, len(sources))
//TODO- 	altNames := make([]string, len(sources))
//TODO- 	for i, pair0 := range sources {
//TODO- 		pair := pair0.([]interface{})
//TODO- 		altName := pair[1].(string)
//TODO- 		switch x := pair[0].(type) {
//TODO- 		case string: // table name
//TODO- 			rsets[i] = tableRset(x)
//TODO- 			if altName == "" {
//TODO- 				altName = x
//TODO- 			}
//TODO- 		case *selectStmt:
//TODO- 			rsets[i] = x
//TODO- 		default:
//TODO- 			log.Panic("internal error 074")
//TODO- 		}
//TODO- 		altNames[i] = altName
//TODO- 	}
//TODO-
//TODO- 	var flds, leftFlds, rightFlds []*fld
//TODO- 	var nF, nP, nL, nR int
//TODO- 	fldsSent := false
//TODO- 	iq := 0
//TODO- 	stop := false
//TODO- 	ids := map[string]interface{}{}
//TODO- 	m := map[interface{}]interface{}{}
//TODO- 	var g func([]interface{}, []rset, int) error
//TODO- 	var match bool
//TODO- 	var rid int64
//TODO- 	firstR := true
//TODO- 	g = func(prefix []interface{}, rsets []rset, x int) (err error) {
//TODO- 		rset := rsets[0]
//TODO- 		rsets = rsets[1:]
//TODO- 		ok := false
//TODO- 		return rset.do(ctx, onlyNames, func(id interface{}, in []interface{}) (bool, error) {
//TODO- 			if onlyNames && fldsSent {
//TODO- 				stop = true
//TODO- 				return false, nil
//TODO- 			}
//TODO-
//TODO- 			if ok {
//TODO- 				ids[altNames[x]] = id
//TODO- 				if len(rsets) != 0 {
//TODO- 					newPrefix := append(prefix, in...)
//TODO- 					match = false
//TODO- 					rid = 0
//TODO- 					if err := g(newPrefix, rsets, x+1); err != nil {
//TODO- 						return false, err
//TODO- 					}
//TODO-
//TODO- 					if len(newPrefix) < nP+nL {
//TODO- 						return true, nil
//TODO- 					}
//TODO-
//TODO- 					firstR = false
//TODO- 					if match {
//TODO- 						return true, nil
//TODO- 					}
//TODO-
//TODO- 					row := append(newPrefix, make([]interface{}, len(rightFlds))...)
//TODO- 					switch o.typ {
//TODO- 					case rightJoin:
//TODO- 						row2 := row[:nP:nP]
//TODO- 						row2 = append(row2, row[nP+nL:]...)
//TODO- 						row2 = append(row2, row[nP:nP+nL]...)
//TODO- 						row = row2
//TODO- 					}
//TODO- 					more, err := f(ids, row)
//TODO- 					if !more {
//TODO- 						stop = true
//TODO- 					}
//TODO- 					return more && !stop, err
//TODO- 				}
//TODO-
//TODO- 				// prefix: left "table" row
//TODO- 				// in: right "table" row
//TODO- 				row := append(prefix, in...)
//TODO- 				switch o.typ {
//TODO- 				case rightJoin:
//TODO- 					row2 := row[:nP:nP]
//TODO- 					row2 = append(row2, row[nP+nL:]...)
//TODO- 					row2 = append(row2, row[nP:nP+nL]...)
//TODO- 					row = row2
//TODO- 				case fullJoin:
//TODO- 					rid++
//TODO- 					if !firstR {
//TODO- 						break
//TODO- 					}
//TODO-
//TODO- 					b3.Set(rid, in)
//TODO- 				}
//TODO- 				for i, fld := range flds {
//TODO- 					if nm := fld.name; nm != "" {
//TODO- 						m[nm] = row[i]
//TODO- 					}
//TODO- 				}
//TODO-
//TODO- 				val, err := o.on.eval(ctx, m, ctx.arg)
//TODO- 				if err != nil {
//TODO- 					return false, err
//TODO- 				}
//TODO-
//TODO- 				if val == nil {
//TODO- 					return true && !stop, nil
//TODO- 				}
//TODO-
//TODO- 				x, ok := val.(bool)
//TODO- 				if !ok {
//TODO- 					return false, fmt.Errorf("invalid ON expression %s (value of type %T)", val, val)
//TODO- 				}
//TODO-
//TODO- 				if !x {
//TODO- 					return true && !stop, nil
//TODO- 				}
//TODO-
//TODO- 				match = true
//TODO- 				if o.typ == fullJoin {
//TODO- 					b3.Delete(rid)
//TODO- 				}
//TODO- 				more, err := f(ids, row)
//TODO- 				if !more {
//TODO- 					stop = true
//TODO- 				}
//TODO- 				return more && !stop, err
//TODO- 			}
//TODO-
//TODO- 			ok = true
//TODO- 			if !fldsSent {
//TODO- 				f0 := append([]*fld(nil), in[0].([]*fld)...)
//TODO- 				q := altNames[iq]
//TODO- 				for i, elem := range f0 {
//TODO- 					nf := &fld{}
//TODO- 					*nf = *elem
//TODO- 					switch {
//TODO- 					case q == "":
//TODO- 						nf.name = ""
//TODO- 					case nf.name != "":
//TODO- 						nf.name = fmt.Sprintf("%s.%s", altNames[iq], nf.name)
//TODO- 					}
//TODO- 					f0[i] = nf
//TODO- 				}
//TODO- 				iq++
//TODO- 				flds = append(flds, f0...)
//TODO- 				leftFlds = append([]*fld(nil), rightFlds...)
//TODO- 				rightFlds = append([]*fld(nil), f0...)
//TODO- 			}
//TODO- 			if len(rsets) == 0 && !fldsSent {
//TODO- 				fldsSent = true
//TODO- 				nF = len(flds)
//TODO- 				nL = len(leftFlds)
//TODO- 				nR = len(rightFlds)
//TODO- 				nP = nF - nL - nR
//TODO- 				x := flds
//TODO- 				switch o.typ {
//TODO- 				case rightJoin:
//TODO- 					x = x[:nP:nP]
//TODO- 					x = append(x, rightFlds...)
//TODO- 					x = append(x, leftFlds...)
//TODO- 					flds = x
//TODO- 				}
//TODO- 				more, err := f(nil, []interface{}{x})
//TODO- 				if !more {
//TODO- 					stop = true
//TODO- 				}
//TODO- 				return more && !stop, err
//TODO- 			}
//TODO-
//TODO- 			return !stop, nil
//TODO- 		})
//TODO- 	}
//TODO- 	if err := g(nil, rsets, 0); err != nil {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	if o.typ != fullJoin {
//TODO- 		return nil
//TODO- 	}
//TODO-
//TODO- 	it, err := b3.SeekFirst()
//TODO- 	if err != nil {
//TODO- 		return err
//TODO- 	}
//TODO-
//TODO- 	pref := make([]interface{}, nP+nL)
//TODO- 	for {
//TODO- 		_, v, err := it.Next()
//TODO- 		if err != nil { // No more items
//TODO- 			return nil
//TODO- 		}
//TODO-
//TODO- 		more, err := f(nil, append(pref, v.([]interface{})...))
//TODO- 		if err != nil {
//TODO- 			return err
//TODO- 		}
//TODO-
//TODO- 		if !more {
//TODO- 			return nil
//TODO- 		}
//TODO- 	}
//TODO- }

func (r *outerJoinRset) plan(ctx *execCtx) (rset2, error) { panic("TODO") }
