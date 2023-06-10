package minidal

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"unicode"
)

type IModel interface {
	Insert(data Object) (int, error)
	InsertBulk(data ...Object) (Object, error)
	Update(where Object, data Object) (int, error)
	Delete(where Object) (int, error)
	Find(where Object) ([]*Object, error)
	First(where Object) (Object, error)
}

type ISerializer interface {
	New() interface{}
}

type Object map[string]interface{}

func (t Object) New() interface{} {
	return Object{}
}

type Model struct {
	tablename string
	schema    any
	DAL       DB
}

func (m Model) Insert(data Object) (int64, error) {

	query, values, err := __insert(m.tablename, data)
	if err != nil {
		panic("could not initialize Insert query!")
	}

	result, err := m.DAL.Exec(query, values)

	if err != nil {
		return 0, fmt.Errorf("error: %v", err)
	}

	return result.LastInsertId()

}

func (m Model) InsertBulk(data ...Object) (Object, error) {

	ctx := context.Background()

	cs := func(t *sql.Tx) (any, error) {

		query, values, err := __insertBulk(m.tablename, data)
		if err != nil {
			return nil, err
		}

		rs, err := t.ExecContext(ctx, query, values...)
		if err != nil {
			return nil, err
		}

		iid, err := rs.LastInsertId()
		if err != nil {
			return nil, err
		}

		arows, err := rs.RowsAffected()
		if err != nil {
			return nil, err
		}

		result := Object{
			"LastInsertId": iid,
			"RowsAffected": arows,
		}

		return result, nil

	}

	res, err := m.DAL.Tx(ctx, cs)

	return res.(Object), err

}

func (m Model) Update(where Object, data Object, or ...bool) (int64, error) {

	glue := AND
	if len(or) > 0 && or[0] {
		glue = OR
	}

	query, values, err := __update(m.tablename, string(glue), where, data)
	if err != nil {
		panic("could not initialize Update query")
	}

	result, err := m.DAL.Exec(query, values)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()

}

func (m Model) Delete(where Object, or ...bool) (int64, error) {
	glue := AND
	if len(or) > 0 && or[0] {
		glue = OR
	}

	query, values, err := __delete(m.tablename, string(glue), where)
	if err != nil {
		panic("could not initialize Delete query")
	}

	result, err := m.DAL.Exec(query, values)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()

}

func (m Model) First(where Object, or ...bool) (any, error) {

	glue := AND
	if len(or) > 0 && or[0] {
		glue = OR
	}

	query, values, err := __first(m.tablename, string(glue), where)
	if err != nil {
		panic("could not initialize First query")
	}

	outMap, err := m.DAL.Query(query, values)

	if err != nil {
		return nil, err
	}

	if len(outMap) != 1 {
		return nil, fmt.Errorf("EmptyResult")
	}

	out, err := m.Deserialize(outMap[0])
	if err != nil {
		return nil, err
	}

	return out, nil

}

func (m Model) Find(args ...any) (interface{}, error) {

	w := Object{}
	ob := Object{
		"id": DESC,
	}

	glue := AND
	if len(args) >= 1 {
		w = args[0].(Object)
		if len(args) >= 2 {
			ob = args[1].(Object)
			if len(args) >= 3 && args[2] == OR {
				glue = OR
			}
		}

	}

	query, values, err := __find(m.tablename, string(glue), w, ob)

	if err != nil {
		panic("could not initialize Find query")
	}

	outMap, err := m.DAL.Query(query, values)

	if err != nil {
		return nil, err
	}

	out, err := m.Deserialize(outMap...)
	if err != nil {
		return nil, err
	}

	return out, nil

}

func CopyFieldsToNewStruct(structType interface{}, object Object) (interface{}, error) {

	s := reflect.New(reflect.TypeOf(structType).Elem())
	for k, v := range object {

		r := []rune(k)
		r[0] = unicode.ToUpper(r[0])
		name := string(r)
		field := s.Elem().FieldByName(name)

		if !field.CanSet() {
			return nil, fmt.Errorf("cannot set %s field value", name)
		}

		val := reflect.ValueOf(v)
		if fmt.Sprintf("%v", field.Type()) == "string" &&
			fmt.Sprintf("%v", reflect.TypeOf(v)) == "[]uint8" {
			val := string(val.Interface().([]byte))
			field.Set(reflect.ValueOf(val))
		} else {
			field.Set(val)
		}

	}

	return s.Elem().Interface(), nil
}

func (m *Model) Deserialize(os ...Object) (any, error) {

	if len(os) < 1 {
		panic("Cannot not deserialize empty list of objects")
	}

	i := 0
	out := []interface{}{}

	for _, o := range os {

		s, err := CopyFieldsToNewStruct(m.schema, o)
		if err != nil {
			return nil, err
		}

		i += 1
		out = append(out, s)

	}

	if i == 1 {
		return out[0], nil
	}

	return out, nil

}
