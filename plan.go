// Copyright 2015 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ql

import (
	"fmt"
	"strings"

	"github.com/cznic/b"
)

var (
	_ plan = (*crossJoinDefaultPlan)(nil)
	_ plan = (*distinctDefaultPlan)(nil)
	_ plan = (*filterDefaultPlan)(nil)
	_ plan = (*fullJoinDefaultPlan)(nil)
	_ plan = (*groupByDefaultPlan)(nil)
	_ plan = (*leftJoinDefaultPlan)(nil)
	_ plan = (*limitDefaultPlan)(nil)
	_ plan = (*offsetDefaultPlan)(nil)
	_ plan = (*orderByDefaultPlan)(nil)
	_ plan = (*rightJoinDefaultPlan)(nil)
	_ plan = (*selectFieldsDefaultPlan)(nil)
	_ plan = (*selectIndexDefaultPlan)(nil)
	_ plan = (*sysColumnDefaultPlan)(nil)
	_ plan = (*sysIndexDefaultPlan)(nil)
	_ plan = (*sysTableDefaultPlan)(nil)
	_ plan = (*tableDefaultPlan)(nil)
)

type plan interface {
	do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error
	fieldNames() []string
	filter(expr expression) (plan, error)
	filterUsingIndex(expr expression) (plan, error)
}

type filterDefaultPlan struct {
	plan
	expr expression
}

func (r *filterDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	m := map[interface{}]interface{}{}
	fields := r.plan.fieldNames()
	return r.plan.do(ctx, func(rid interface{}, data []interface{}) (bool, error) {
		for i, v := range fields {
			m[v] = data[i]
		}
		m["$id"] = rid
		val, err := r.expr.eval(ctx, m, ctx.arg)
		if err != nil {
			return false, err
		}

		if val == nil { //TODO bug if expr is 'IS NULL' ?
			return true, nil
		}

		x, ok := val.(bool)
		if !ok {
			return false, fmt.Errorf("invalid boolean expression %s (value of type %T)", val, val)
		}

		if !x {
			return true, nil
		}

		return f(rid, data)
	})
}

type crossJoinDefaultPlan struct {
	rsets  []plan
	names  []string
	fields []string
}

func (r *crossJoinDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *crossJoinDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	dbg("", expr)
	for i, v := range r.names {
		e2, err := expr.clone(nil, v)
		if err != nil {
			return nil, err
		}

		dbg("", i, e2)
		p2, err := r.rsets[i].filterUsingIndex(e2)
		if err != nil {
			return nil, err
		}

		if p2 != nil {
			panic("TODO")
		}
	}
	return nil, nil
}

func (r *crossJoinDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
	if len(r.rsets) == 1 {
		return r.rsets[0].do(ctx, f)
	}

	ids := map[string]interface{}{}
	var g func([]interface{}, []plan, int) error
	g = func(prefix []interface{}, rsets []plan, x int) (err error) {
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

func (r *crossJoinDefaultPlan) fieldNames() []string { return r.fields }

type distinctDefaultPlan struct {
	src    plan
	fields []string
}

func (r *distinctDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *distinctDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r *distinctDefaultPlan) fieldNames() []string { return r.fields }

func (r *distinctDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
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

type groupByDefaultPlan struct {
	colNames []string
	src      plan
	fields   []string
}

func (r *groupByDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *groupByDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	p2, err := r.src.filterUsingIndex(expr)
	if err != nil {
		return nil, err
	}

	if p2 != nil {
		panic("TODO")
	}

	return nil, nil
}

func (r *groupByDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
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

func (r *groupByDefaultPlan) fieldNames() []string { return r.fields }

type selectIndexDefaultPlan struct {
	nm string
	x  interface{}
}

func (r *selectIndexDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *selectIndexDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r *selectIndexDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
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

func (r *selectIndexDefaultPlan) fieldNames() []string {
	return []string{r.nm}
}

type limitDefaultPlan struct {
	expr   expression
	src    plan
	fields []string
}

func (r *limitDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *limitDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r *limitDefaultPlan) fieldNames() []string { return r.fields }

func (r *limitDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) (err error) {
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

type offsetDefaultPlan struct {
	expr   expression
	src    plan
	fields []string
}

func (r *offsetDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *offsetDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r *offsetDefaultPlan) fieldNames() []string { return r.fields }

func (r *offsetDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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

type orderByDefaultPlan struct {
	asc    bool
	by     []expression
	src    plan
	fields []string
}

func (r *orderByDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *orderByDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r *orderByDefaultPlan) fieldNames() []string { return r.fields }

func (r *orderByDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
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

type selectFieldsDefaultPlan struct {
	flds   []*fld
	src    plan
	fields []string
}

func (r *selectFieldsDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *selectFieldsDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	//TODO (297) p2, err := r.src.filterUsingIndex(expr)
	//TODO (297) if err != nil {
	//TODO (297) 	return nil, err
	//TODO (297) }

	//TODO (297) if p2 != nil {
	//TODO (297) 	panic("TODO")
	//TODO (297) }

	return nil, nil
}

func (r *selectFieldsDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	if _, ok := r.src.(*groupByDefaultPlan); ok { //TODO different plan, also possible conflict with optimizer
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

func (r *selectFieldsDefaultPlan) doGroup(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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

func (r *selectFieldsDefaultPlan) fieldNames() []string { return r.fields }

type sysColumnDefaultPlan struct{}

func (r *sysColumnDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r sysColumnDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r sysColumnDefaultPlan) fieldNames() []string {
	return []string{"TableName", "Ordinal", "Name", "Type"}
}

func (r sysColumnDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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

type sysIndexDefaultPlan struct{}

func (r *sysIndexDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r sysIndexDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r sysIndexDefaultPlan) fieldNames() []string {
	return []string{"TableName", "ColumnName", "Name", "IsUnique"}
}

func (r sysIndexDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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

type sysTableDefaultPlan struct{}

func (r *sysTableDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r sysTableDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r sysTableDefaultPlan) fieldNames() []string { return []string{"Name", "Schema"} }

func (r sysTableDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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

type tableDefaultPlan struct {
	t      *table
	fields []string
}

func (r *tableDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *tableDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	cols := mentionedColumns(expr)
	for _, v := range r.fields {
		delete(cols, v)
	}
	for k := range cols {
		return nil, fmt.Errorf("unknown column %s", k)
	}

	dbg("", r.t.name, r.fields, expr)
	if !r.t.hasIndices() {
		return nil, nil
	}

	sexpr := expr.String()
	for _, ix := range r.t.indices2 {
		if len(ix.exprList) != 1 {
			continue
		}

		if ix.sources[0] != sexpr {
			continue
		}

		panic("TODO")
	}

	switch x := expr.(type) {
	case *binaryOperation:
		panic("TODO")
	default:
		dbg("%T(%v)", x, x)
		panic("TODO")
	}

	panic("TODO")
}

func (r *tableDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
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

func (r *tableDefaultPlan) fieldNames() []string { return r.fields }

type leftJoinDefaultPlan struct {
	on     expression
	rsets  []plan
	names  []string
	right  int
	fields []string
}

func (r *leftJoinDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *leftJoinDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

type rightJoinDefaultPlan struct {
	leftJoinDefaultPlan
}

func (r *rightJoinDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

type fullJoinDefaultPlan struct {
	leftJoinDefaultPlan
}

func (r *fullJoinDefaultPlan) filter(expr expression) (plan, error) {
	panic("TODO")
}

func (r *fullJoinDefaultPlan) filterUsingIndex(expr expression) (plan, error) {
	panic("TODO")
}

func (r *leftJoinDefaultPlan) fieldNames() []string { return r.fields }

func (r *leftJoinDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error {
	m := map[interface{}]interface{}{}
	ids := map[string]interface{}{}
	var g func([]interface{}, []plan, int) error
	var match bool
	g = func(prefix []interface{}, rsets []plan, x int) (err error) {
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

func (r *rightJoinDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error {
	right := r.right
	left := len(r.fields) - right
	n := len(r.rsets)
	m := map[interface{}]interface{}{}
	ids := map[string]interface{}{}
	var g func([]interface{}, []plan, int) error
	var match bool
	nf := len(r.fields)
	fields := append(append([]string(nil), r.fields[nf-right:]...), r.fields[:nf-right]...)
	g = func(prefix []interface{}, rsets []plan, x int) (err error) {
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
	return g(nil, append([]plan{r.rsets[n-1]}, r.rsets[:n-1]...), 0)
}

func (r *fullJoinDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error {
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
	var g func([]interface{}, []plan, int) error
	var match bool
	var rid int64
	firstR := true
	g = func(prefix []interface{}, rsets []plan, x int) (err error) {
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
