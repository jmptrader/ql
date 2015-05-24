// Copyright 2015 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cznic/b"
	"github.com/cznic/strutil"
)

// Note: All plans must have a pointer receiver. Enables planA == planB operation.
var (
	_ plan = (*crossJoinDefaultPlan)(nil)
	_ plan = (*distinctDefaultPlan)(nil)
	_ plan = (*explainDefaultPlan)(nil)
	_ plan = (*filterDefaultPlan)(nil)
	_ plan = (*fullJoinDefaultPlan)(nil)
	_ plan = (*groupByDefaultPlan)(nil)
	_ plan = (*indexBoolPlan)(nil)
	_ plan = (*indexEqPlan)(nil)
	_ plan = (*indexGePlan)(nil)
	_ plan = (*indexGtPlan)(nil)
	_ plan = (*indexLePlan)(nil)
	_ plan = (*indexLtPlan)(nil)
	_ plan = (*indexNePlan)(nil)
	_ plan = (*indexNotNullPlan)(nil)
	_ plan = (*indexNullPlan)(nil)
	_ plan = (*leftJoinDefaultPlan)(nil)
	_ plan = (*limitDefaultPlan)(nil)
	_ plan = (*nullPlan)(nil)
	_ plan = (*offsetDefaultPlan)(nil)
	_ plan = (*orderByDefaultPlan)(nil)
	_ plan = (*rightJoinDefaultPlan)(nil)
	_ plan = (*selectFieldsDefaultPlan)(nil)
	_ plan = (*selectFieldsGroupPlan)(nil)
	_ plan = (*selectIndexDefaultPlan)(nil)
	_ plan = (*sysColumnDefaultPlan)(nil)
	_ plan = (*sysIndexDefaultPlan)(nil)
	_ plan = (*sysTableDefaultPlan)(nil)
	_ plan = (*tableDefaultPlan)(nil)
	_ plan = (*tableNilPlan)(nil)
)

type plan interface {
	do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error
	explain(w strutil.Formatter)
	fieldNames() []string
	filterUsingIndex(expr expression) (p plan, indicesSought []string, err error)
	hasID() bool
}

func isTableOrIndex(p plan) bool {
	switch p.(type) {
	case
		*tableDefaultPlan,
		*indexEqPlan,
		*indexLtPlan,
		*indexLePlan,
		*indexGePlan,
		*indexGtPlan,
		*indexBoolPlan,
		*sysTableDefaultPlan,
		*sysColumnDefaultPlan,
		*sysIndexDefaultPlan:
		return true
	default:
		return false
	}
}

type explainDefaultPlan struct {
	s stmt
}

func (r *explainDefaultPlan) hasID() bool { return false }

func (r *explainDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (more bool, err error)) error {
	var buf bytes.Buffer
	switch x := r.s.(type) {
	default:
		w := strutil.IndentFormatter(&buf, "│   ")
		x.explain(ctx, w)
	}

	a := bytes.Split(buf.Bytes(), []byte{'\n'})
	for _, v := range a[:len(a)-1] {
		if more, err := f(nil, []interface{}{string(v)}); !more || err != nil {
			return err
		}
	}
	return nil
}

func (r *explainDefaultPlan) explain(w strutil.Formatter) {
	return
}

func (r *explainDefaultPlan) fieldNames() []string {
	return []string{""}
}

func (r *explainDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
}

type filterDefaultPlan struct {
	plan
	expr expression
	is   []string
}

func (r *filterDefaultPlan) hasID() bool { return r.plan.hasID() }

func (r *filterDefaultPlan) explain(w strutil.Formatter) {
	r.plan.explain(w)
	w.Format("┌Filter on %v\n", r.expr)
	if len(r.is) != 0 {
		w.Format("│Possibly useful indices\n")
		m := map[string]bool{}
		for _, v := range r.is {
			if !m[v] {
				m[v] = true
				n := ""
				for _, r := range v {
					if r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_' {
						n += string(r)
						continue
					}

					n += "_"
				}
				for strings.Contains(n, "__") {
					n = strings.Replace(n, "__", "_", -1)
				}
				for strings.HasSuffix(n, "_") {
					n = n[:len(n)-1]
				}
				w.Format("│CREATE INDEX x%s ON %s;\n", n, v)
			}
		}
	}
	w.Format("└Output field names %v\n", qnames(r.plan.fieldNames()))
}

func (r *filterDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	m := map[interface{}]interface{}{}
	fields := r.plan.fieldNames()
	return r.plan.do(ctx, func(rid interface{}, data []interface{}) (bool, error) {
		for i, v := range fields {
			m[v] = data[i]
		}
		m["$id"] = rid
		val, err := r.expr.eval(ctx, m)
		if err != nil {
			return false, err
		}

		if val == nil {
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

func (r *crossJoinDefaultPlan) hasID() bool { return false }

func (r *crossJoinDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Compute Cartesian product of%i\n")
	for i, v := range r.rsets {
		sel := !isTableOrIndex(v)
		if sel {
			w.Format("┌Iterate all rows of virtual table %q%i\n", r.names[i])
		}
		v.explain(w)
		if sel {
			w.Format("%u└Output field names %v\n", qnames(v.fieldNames()))
		}
	}
	w.Format("%u└Output field names %v\n", qnames(r.fields))
}

func (r *crossJoinDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	for i, v := range r.names {
		e2, err := expr.clone(nil, v)
		if err != nil {
			return nil, nil, err
		}

		p2, x, err := r.rsets[i].filterUsingIndex(e2)
		if err != nil {
			return nil, nil, err
		}

		if p2 != nil {
			r.rsets[i] = p2
			return r, x, nil
		}
	}
	return nil, nil, nil
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

func (r *distinctDefaultPlan) hasID() bool { return false }

func (r *distinctDefaultPlan) explain(w strutil.Formatter) {
	r.src.explain(w)
	w.Format("┌Compute distinct rows\n└Output field names %v\n", r.fields)
}

func (r *distinctDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
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

func (r *groupByDefaultPlan) hasID() bool { return false }

func (r *groupByDefaultPlan) explain(w strutil.Formatter) {
	r.src.explain(w)
	switch {
	case len(r.colNames) == 0: //TODO this case should not exist for this plan, should become tableDefaultPlan
		w.Format("┌Group by distinct rows")
	default:
		w.Format("┌Group by")
		for _, v := range r.colNames {
			w.Format(" %s,", v)
		}
	}
	w.Format("\n└Output field names %v\n", qnames(r.fields))
}

func (r *groupByDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
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
			return fmt.Errorf("unknown field %s", c)
		}

		gcols = append(gcols, &col{name: c, index: i})
	}
	k := make([]interface{}, len(r.colNames)) //TODO optimize when len(r.cols) == 0, should become tableDefaultPlan
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

func (r *selectIndexDefaultPlan) hasID() bool { return false }

func (r *selectIndexDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Iterate all values of index %q\n└Output field names N/A\n", r.nm)
}

func (r *selectIndexDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
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

func (r *limitDefaultPlan) hasID() bool { return r.src.hasID() }

func (r *limitDefaultPlan) explain(w strutil.Formatter) {
	r.src.explain(w)
	w.Format("┌Pass first %v records\n└Output field names %v\n", r.expr, r.fields)
}

func (r *limitDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
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
			val, err := r.expr.eval(ctx, m)
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

func (r *offsetDefaultPlan) hasID() bool { return r.src.hasID() }

func (r *offsetDefaultPlan) explain(w strutil.Formatter) {
	r.src.explain(w)
	w.Format("┌Skip first %v records\n└Output field names %v\n", r.expr, qnames(r.fields))
}

func (r *offsetDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
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
			val, err := r.expr.eval(ctx, m)
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

func (r *orderByDefaultPlan) hasID() bool { return r.src.hasID() }

func (r *orderByDefaultPlan) explain(w strutil.Formatter) {
	r.src.explain(w)
	w.Format("┌Order%s by", map[bool]string{false: " descending"}[r.asc])
	for _, v := range r.by {
		w.Format(" %s,", v)
	}
	w.Format("\n└Output field names %v\n", qnames(r.fields))
}

func (r *orderByDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
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
			val, err := expr.eval(ctx, m)
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

func (r *selectFieldsDefaultPlan) hasID() bool { return r.src.hasID() }

func (r *selectFieldsDefaultPlan) explain(w strutil.Formatter) {
	//TODO check for non existing fields
	r.src.explain(w)
	w.Format("┌Evaluate")
	for _, v := range r.flds {
		w.Format(" %s as %s,", v.expr, fmt.Sprintf("%q", v.name))
	}
	w.Format("\n└Output field names %v\n", qnames(r.fields))
}

func (r *selectFieldsDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
}

func (r *selectFieldsDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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
			var err error
			if out[i], err = fld.expr.eval(ctx, m); err != nil {
				return false, err
			}
		}
		return f(rid, out)
	})
}

func (r *selectFieldsDefaultPlan) fieldNames() []string { return r.fields }

type selectFieldsGroupPlan struct {
	flds   []*fld
	src    *groupByDefaultPlan
	fields []string
}

func (r *selectFieldsGroupPlan) hasID() bool { return false }

func (r *selectFieldsGroupPlan) explain(w strutil.Formatter) {
	//TODO check for non existing fields
	r.src.explain(w)
	w.Format("┌Evaluate")
	for _, v := range r.flds {
		w.Format(" %s as %s,", v.expr, fmt.Sprintf("%q", v.name))
	}
	w.Format("\n└Output field names %v\n", qnames(r.fields))
}

func (r *selectFieldsGroupPlan) fieldNames() []string { return r.fields }

func (r *selectFieldsGroupPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
}

func (r *selectFieldsGroupPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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
					if _, err = fld.expr.eval(ctx, m); err != nil {
						return false, err
					}
				}

				h = in[0].(int64)
			}
			m["$agg"] = true
			for i, fld := range r.flds {
				if out[i], err = fld.expr.eval(ctx, m); err != nil {
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
		if out[i], err = fld.expr.eval(ctx, m); err != nil {
			return err
		}
	}
	_, err = f(nil, out)

	return err
}

type sysColumnDefaultPlan struct{}

func (r *sysColumnDefaultPlan) hasID() bool { return false }

func (r *sysColumnDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Iterate all rows of table \"__Column\"\n└Output field names %v\n", qnames(r.fieldNames()))
}

func (r *sysColumnDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
}

func (r *sysColumnDefaultPlan) fieldNames() []string {
	return []string{"TableName", "Ordinal", "Name", "Type"}
}

func (r *sysColumnDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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

func (r *sysIndexDefaultPlan) hasID() bool { return false }

func (r *sysIndexDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Iterate all rows of table \"__Index\"\n└Output field names %v\n", qnames(r.fieldNames()))
}

func (r *sysIndexDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
}

func (r *sysIndexDefaultPlan) fieldNames() []string {
	return []string{"TableName", "ColumnName", "Name", "IsUnique"}
}

func (r *sysIndexDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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

func (r *sysTableDefaultPlan) hasID() bool { return false }

func (r *sysTableDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Iterate all rows of table \"__Table\"\n└Output field names %v\n", qnames(r.fieldNames()))
}

func (r *sysTableDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
}

func (r *sysTableDefaultPlan) fieldNames() []string { return []string{"Name", "Schema"} }

func (r *sysTableDefaultPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) error {
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

type tableNilPlan struct {
	t *table
}

func (r *tableNilPlan) hasID() bool { return true }

func (r *tableNilPlan) explain(w strutil.Formatter) {
	w.Format("┌Iterate all rows of table %q\n└Output field names %v\n", r.t.name, qnames(r.fieldNames()))
}

func (r *tableNilPlan) fieldNames() []string { return []string{} }

func (r *tableNilPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil
}

func (r *tableNilPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t := r.t
	h := t.head
	cols := t.cols
	for h > 0 {
		rec, err := t.store.Read(nil, h, cols...) // 0:next, 1:id, 2...: data
		if err != nil {
			return err
		}

		if m, err := f(rec[1], nil); !m || err != nil {
			return err
		}

		h = rec[0].(int64) // next
	}
	return nil
}

type tableDefaultPlan struct {
	t      *table
	fields []string
}

func (r *tableDefaultPlan) hasID() bool { return true }

func (r *tableDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Iterate all rows of table %q\n└Output field names %v\n", r.t.name, qnames(r.fields))
}

func (r *tableDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	t := r.t
	cols := mentionedColumns(expr)
	for _, v := range r.fields {
		delete(cols, v)
	}
	for k := range cols {
		return nil, nil, fmt.Errorf("unknown field %s", k)
	}

	is := []string{fmt.Sprintf("%s(%s)", t.name, expr.String())}

	//TODOvar sexpr string
	//TODOfor _, ix := range t.indices2 {
	//TODO	if len(ix.exprList) != 1 {
	//TODO		continue
	//TODO	}

	//TODO	if sexpr == "" {
	//TODO		sexpr = expr.String()
	//TODO	}
	//TODO	if ix.sources[0] != sexpr {
	//TODO		continue
	//TODO	}

	//TODO}

	switch x := expr.(type) {
	case *binaryOperation:
		ok, cn, rval, err := x.isIdentRelOpVal()
		if err != nil {
			return nil, nil, err
		}

		if !ok {
			return nil, is, nil
		}

		c, ix := t.findIndexByColName(cn)
		if ix == nil { // Column cn has no index.
			is = append(is, fmt.Sprintf("%s(%s)", t.name, cn))
			return nil, is, nil
		}

		if rval, err = typeCheck1(rval, c); err != nil {
			return nil, nil, err
		}

		switch x.op {
		case eq:
			return &indexEqPlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
				rval,
			}, nil, nil
		case '<':
			return &indexLtPlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
				rval,
			}, nil, nil
		case le:
			return &indexLePlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
				rval,
			}, nil, nil
		case ge:
			return &indexGePlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
				rval,
			}, nil, nil
		case '>':
			return &indexGtPlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
				rval,
			}, nil, nil
		case neq:
			return &indexNePlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
				rval,
			}, nil, nil
		default:
			panic("internal error 069")
		}
	case *ident:
		cn := x.s
		for _, v := range t.cols {
			if v.name != cn {
				continue
			}

			if v.typ != qBool {
				return nil, nil, nil
			}

			xi := v.index + 1 // 0: id()
			if xi >= len(t.indices) {
				return nil, nil, nil
			}

			ix := t.indices[xi]
			if ix == nil { // Column cn has no index.
				is = append(is, fmt.Sprintf("%s(%s)", t.name, cn))
				return nil, is, nil
			}

			return &indexBoolPlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
			}, nil, nil
		}
	case *isNull:
		ok, cn := isColumnExpression(x.expr)
		if !ok {
			return nil, nil, nil
		}
		_, ix := t.findIndexByColName(cn)
		if ix == nil { // Column cn has no index.
			is = append(is, fmt.Sprintf("%s(%s)", t.name, cn))
			return nil, is, nil
		}

		switch {
		case x.not:
			return &indexNotNullPlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
			}, nil, nil
		default:
			return &indexNullPlan{
				tableDefaultPlan{t: t, fields: append([]string(nil), r.fields...)},
				ix.name,
				ix.x,
			}, nil, nil
		}
	default:
		return nil, is, nil //TODO
	}

	return nil, is, nil
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

type indexNotNullPlan struct { // column IS NULL
	tableDefaultPlan
	xn string
	x  btreeIndex
}

func (r *indexNotNullPlan) hasID() bool { return true }

func (r *indexNotNullPlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows of table %q using index %q where the indexed value IS NOT NULL\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, qnames(r.fieldNames()),
	)
}

func (r *indexNotNullPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

func (r *indexNotNullPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t := r.t
	it, err := r.x.SeekLast()
	if err != nil {
		return noEOF(err)
	}
	for {
		k, h, err := it.Prev()
		if err != nil {
			return noEOF(err)
		}

		if k[0] == nil {
			return nil
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}
}

type nullPlan struct {
	fields []string
}

func (r *nullPlan) hasID() bool { return false }

func (r *nullPlan) fieldNames() []string { return r.fields }

func (r *nullPlan) explain(w strutil.Formatter) {
	w.Format("┌Iterate no rows\n└Output field names %v\n", qnames(r.fields))
}

func (r *nullPlan) do(*execCtx, func(interface{}, []interface{}) (bool, error)) error {
	return nil
}

func (r *nullPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return r, nil, nil
}

type indexNullPlan struct { // column IS NULL
	tableDefaultPlan
	xn string
	x  btreeIndex
}

func (r *indexNullPlan) hasID() bool { return true }

func (r *indexNullPlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows of table %q using index %q where the indexed value IS NULL\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, qnames(r.fieldNames()),
	)
}

func (r *indexNullPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

func (r *indexNullPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t := r.t
	it, err := r.x.SeekFirst()
	if err != nil {
		return noEOF(err)
	}
	for {
		k, h, err := it.Next()
		if err != nil {
			return noEOF(err)
		}

		if k[0] != nil {
			return nil
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}
}

type indexNePlan struct { // column != val
	tableDefaultPlan
	xn  string
	x   btreeIndex
	val interface{}
}

func (r *indexNePlan) hasID() bool { return true }

func (r *indexNePlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows of table %q using index %q where the indexed value != %v\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, value{r.val}, qnames(r.fields),
	)
}

func (r *indexNePlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

func (r *indexNePlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t := r.t
	it, err := r.x.SeekLast()
	if err != nil {
		return noEOF(err)
	}
	for {
		k, h, err := it.Prev()
		if err != nil {
			return noEOF(err)
		}

		if k[0] == nil {
			return nil
		}

		if k[0] == r.val {
			break
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}

	if it, _, err = r.x.Seek([]interface{}{r.val}); err != nil {
		return noEOF(err)
	}

	if _, _, err := it.Prev(); err != nil { // discard equal value
		return noEOF(err)
	}

	for {
		k, h, err := it.Prev()
		if err != nil {
			return noEOF(err)
		}

		if k[0] == nil {
			return nil
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}

	return nil
}

type indexEqPlan struct { // column == val
	tableDefaultPlan
	xn  string
	x   btreeIndex
	val interface{}
}

func (r *indexEqPlan) hasID() bool { return true }

func (r *indexEqPlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows of table %q using index %q where the indexed value == %v\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, value{r.val}, qnames(r.fields),
	)
}

func (r *indexEqPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

func (r *indexEqPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t := r.t
	it, _, err := r.x.Seek([]interface{}{r.val})
	if err != nil {
		return noEOF(err)
	}

	for {
		k, h, err := it.Next()
		if err != nil {
			return noEOF(err)
		}

		if k[0] != r.val {
			return nil
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}
}

type indexBoolPlan struct { // column (of type bool)
	tableDefaultPlan
	xn string
	x  btreeIndex
}

func (r *indexBoolPlan) hasID() bool { return true }

func (r *indexBoolPlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows of table %q using index %q where the indexed value == true\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, qnames(r.fields),
	)
}

func (r *indexBoolPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

func (r *indexBoolPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	t := r.t
	it, _, err := r.x.Seek([]interface{}{true})
	if err != nil {
		return noEOF(err)
	}

	for {
		k, h, err := it.Next()
		if err != nil {
			return noEOF(err)
		}

		if k[0] != true {
			return nil
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}
}

type indexGePlan struct { // column <= val
	tableDefaultPlan
	xn  string
	x   btreeIndex
	val interface{}
}

func (r *indexGePlan) hasID() bool { return true }

func (r *indexGePlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows of table %q using index %q where the indexed value is >= %v\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, value{r.val}, qnames(r.fields),
	)
}

func (r *indexGePlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

func (r *indexGePlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	if err != nil {
		return err
	}

	t := r.t
	it, _, err := r.x.Seek([]interface{}{r.val})
	if err != nil {
		return noEOF(err)
	}

	for {
		_, h, err := it.Next()
		if err != nil {
			return noEOF(err)
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}
}

type indexLePlan struct { // column <= val
	tableDefaultPlan
	xn  string
	x   btreeIndex
	val interface{}
}

func (r *indexLePlan) hasID() bool { return true }

func (r *indexLePlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows of table %q using index %q where the indexed value is <= %v\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, value{r.val}, qnames(r.fields),
	)
}

func (r *indexLePlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

func (r *indexLePlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	cmp0, err := newBinaryOperation(le, &ident{}, value{val: r.val})
	if err != nil {
		return err
	}

	cmp := cmp0.(*binaryOperation)
	t := r.t
	it, err := r.x.SeekFirst()
	if err != nil {
		return noEOF(err)
	}

	for {
		k, h, err := it.Next()
		if err != nil {
			return noEOF(err)
		}

		cmp.l = value{val: k[0]}
		v, err := cmp.eval(ctx, nil)
		if err != nil {
			return err
		}

		if v == nil {
			continue
		}

		if !v.(bool) {
			return nil
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}
}

type indexGtPlan struct { // column > val
	tableDefaultPlan
	xn  string
	x   btreeIndex
	val interface{}
}

func (r *indexGtPlan) hasID() bool { return true }

func (r *indexGtPlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows on table %q using index %q where the indexed value is > %v\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, value{r.val}, qnames(r.fields),
	)
}

func (r *indexGtPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

func (r *indexGtPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	cmp0, err := newBinaryOperation('>', &ident{}, value{val: r.val})
	if err != nil {
		return err
	}

	cmp := cmp0.(*binaryOperation)
	t := r.t
	it, err := r.x.SeekLast()
	if err != nil {
		return noEOF(err)
	}

	for {
		k, h, err := it.Prev()
		if err != nil {
			return noEOF(err)
		}

		cmp.l = value{val: k[0]}
		v, err := cmp.eval(ctx, nil)
		if err != nil {
			return err
		}

		if v == nil {
			continue
		}

		if !v.(bool) {
			return nil
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}
}

type indexLtPlan struct { // column < val
	tableDefaultPlan
	xn  string
	x   btreeIndex
	val interface{}
}

func (r *indexLtPlan) hasID() bool { return true }

func (r *indexLtPlan) explain(w strutil.Formatter) {
	w.Format(
		"┌Iterate all rows of table %q using index %q where the indexed value is < %v\n└Output field names %v\n",
		r.tableDefaultPlan.t.name, r.xn, value{r.val}, qnames(r.fields),
	)
}

func (r *indexLtPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	//TODO p2, err := r.tableDefaultPlan.filterUsingIndex(expr)
	//TODO if err != nil {
	//TODO 	return nil, err
	//TODO }

	//TODO if p2 == nil {
	//TODO 	return nil, nil
	//TODO }

	return nil, nil, nil //TODO
}

func (r *indexLtPlan) do(ctx *execCtx, f func(id interface{}, data []interface{}) (bool, error)) (err error) {
	cmp0, err := newBinaryOperation('<', &ident{}, value{val: r.val})
	if err != nil {
		return err
	}

	cmp := cmp0.(*binaryOperation)
	t := r.t
	it, err := r.x.SeekFirst()
	if err != nil {
		return noEOF(err)
	}

	for {
		k, h, err := it.Next()
		if err != nil {
			return noEOF(err)
		}

		if len(k) == 0 {
			continue
		}

		cmp.l = value{val: k[0]}
		v, err := cmp.eval(ctx, nil)
		if err != nil {
			return err
		}

		if v == nil {
			continue
		}

		if !v.(bool) {
			return nil
		}

		id, data, err := t.row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(id, data); err != nil || !more {
			return err
		}
	}
}

type leftJoinDefaultPlan struct {
	on     expression
	rsets  []plan
	names  []string
	right  int
	fields []string
}

func (r *leftJoinDefaultPlan) hasID() bool { return false }

func (r *leftJoinDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Compute Cartesian product of%i\n")
	for i, v := range r.rsets {
		sel := !isTableOrIndex(v)
		if sel {
			w.Format("┌Iterate all rows of virtual table %q%i\n", r.names[i])
		}
		v.explain(w)
		if sel {
			w.Format("%u└Output field names %v\n", qnames(v.fieldNames()))
		}
	}
	w.Format("Extend the product with all NULL rows of %q when no match for %v%u\n", r.names[len(r.names)-1], r.on)
	w.Format("└Output field names %v\n", qnames(r.fields))
}

func (r *leftJoinDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

type rightJoinDefaultPlan struct {
	leftJoinDefaultPlan
}

func (r *rightJoinDefaultPlan) hasID() bool { return false }

func (r *rightJoinDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Compute Cartesian product of%i\n")
	for i, v := range r.rsets {
		sel := !isTableOrIndex(v)
		if sel {
			w.Format("┌Iterate all rows of virtual table %q%i\n", r.names[i])
		}
		v.explain(w)
		if sel {
			w.Format("%u└Output field names %v\n", qnames(v.fieldNames()))
		}
	}
	w.Format("Extend the product with all NULL rows of all but %q when no match for %v%u\n", r.names[len(r.names)-1], r.on)
	w.Format("└Output field names %v\n", qnames(r.fields))
}

func (r *rightJoinDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
}

type fullJoinDefaultPlan struct {
	leftJoinDefaultPlan
}

func (r *fullJoinDefaultPlan) hasID() bool { return false }

func (r *fullJoinDefaultPlan) explain(w strutil.Formatter) {
	w.Format("┌Compute Cartesian product of%i\n")
	for i, v := range r.rsets {
		sel := !isTableOrIndex(v)
		if sel {
			w.Format("┌Iterate all rows of virtual table %q%i\n", r.names[i])
		}
		v.explain(w)
		if sel {
			w.Format("%u└Output field names %v\n", qnames(v.fieldNames()))
		}
	}
	w.Format("Extend the product with all NULL rows of %q when no match for %v\n", r.names[len(r.names)-1], r.on)
	w.Format("Extend the product with all NULL rows of all but %q when no match for %v%u\n", r.names[len(r.names)-1], r.on)
	w.Format("└Output field names %v\n", qnames(r.fields))
}

func (r *fullJoinDefaultPlan) filterUsingIndex(expr expression) (plan, []string, error) {
	return nil, nil, nil //TODO
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

			val, err := r.on.eval(ctx, m)
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

			val, err := r.on.eval(ctx, m)
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

			val, err := r.on.eval(ctx, m)
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
