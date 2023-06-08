package minidal

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type Driver string

const (
	SQLite Driver = "sqlite3"
	MySQL  Driver = "mysql"
)

type DataSourceName string

type SortOrder string

const (
	ASC  SortOrder = "ASC"
	DESC SortOrder = "DESC"
)

type Object map[string]interface{}

func __where(where Object, values []any, startIndex int) ([]string, []any, int, error) {

	setWhere := make([]string, len(where))

	j := 0
	for k := range where {
		if where[k] == nil {
			setWhere[j] = k + " IS ?"
		} else {
			setWhere[j] = k + " = ?"
		}
		values[startIndex] = where[k]
		startIndex += 1
		j += 1
	}
	return setWhere, values, startIndex, nil

}

func __orderby(clause Object) ([]string, error) {

	setOrderBy := make([]string, len(clause))

	j := 0
	for k := range clause {
		setOrderBy[j] = k + " " + string(clause[k].(SortOrder))
		j += 1
	}
	return setOrderBy, nil

}

func __insert(table string, data Object) (string, []any, error) {

	columns := make([]string, len(data))
	placeholders := make([]string, len(data))
	values := make([]any, len(data))

	i := 0
	for k := range data {
		columns[i] = k
		placeholders[i] = "?"
		values[i] = data[k]
		i += 1
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(columns, ","), strings.Join(placeholders, ",")), values, nil

}

func __insertBulk(table string, data []Object) (string, []any, error) {

	columns := []string{}
	placeholders := [][]string{}
	values := make([]any, 0)

	for j, idata := range data {

		i := 0
		placeholder := []string{}

		for k := range idata {

			placeholder = []string{}

			if j == 0 {
				columns = append(columns, k)
			}

			placeholder = append(placeholder, "?")

			values = append(values, idata[k])

			i += 1
		}

		placeholders = append(placeholders, placeholder)

	}

	setValues := []string{}
	for j := range placeholders {
		setValues = append(setValues, "("+strings.Join(placeholders[j], ",")+")")
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(columns, ","), strings.Join(setValues, ",")), values, nil

}

func __update(table string, glue string, where Object, data Object) (string, []any, error) {

	set := make([]string, len(data))
	values := make([]any, len(data)+len(where))

	i := 0
	for k := range data {
		set[i] = k + " = ?"
		values[i] = data[k]
		i += 1
	}

	setWhere, values, i, err := __where(where, values, i)

	if err != nil {
		panic(fmt.Errorf("Invalid Where Clause %v", where))
	}

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(set, ","), strings.Join(setWhere, glue)), values, nil
}

func __first(table string, glue string, where Object) (string, []any, error) {

	i := 0
	values := make([]any, len(where))
	setWhere, values, i, err := __where(where, values, i)

	if err != nil {
		panic(fmt.Errorf("Invalid Where Clause %v", where))
	}

	return fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1", table, strings.Join(setWhere, glue)), values, nil

}

func __find(table string, glue string, where Object, orderby Object) (string, []any, error) {

	i := 0
	values := make([]any, len(where))
	setWhere, values, i, err := __where(where, values, i)
	if err != nil {
		panic(fmt.Errorf("Invalid Where Clause %v", where))
	}

	whereClause := ""
	if len(values) > 0 {
		whereClause = "WHERE "
	}

	setOrderBy, err := __orderby(orderby)

	if err != nil {
		panic(fmt.Errorf("Invalid OrderBy Clause %v", orderby))
	}

	return fmt.Sprintf("SELECT * FROM %s %s%s ORDER BY %s", table, whereClause, strings.Join(setWhere, " "+glue+" "), strings.Join(setOrderBy, ", ")), values, nil

}

func __delete(table string, glue string, where Object) (string, []any, error) {

	values := make([]any, len(where))

	i := 0
	deleteWhere, values, i, err := __where(where, values, i)
	if err != nil {
		panic(fmt.Errorf("Invalid Where Clause %v", where))
	}

	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(deleteWhere, glue)), values, nil

}

func SetField(obj interface{}, name string, value interface{}) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	//srcFieldValue := structFieldValue.Addr().Pointer()
	//valValue := reflect.ValueOf(value)

	//fmt.Printf("%v", valFieldValue)
	//valFieldValue := valValue.FieldByName(name)
	//valFieldValue.Elem().Set(structFieldValue.Convert(valFieldValue.Type()))
	return nil

}

func (m Model) Fill(o Object, s *interface{}) error {
	for k, v := range o {
		err := SetField(s, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

type IModel interface {
	Insert(data Object) (int, error)
	InsertBulk(data ...Object) ([]int, error)
	Update(where Object, data Object) (int, error)
	Delete(where Object) (int, error)
	Find(where Object) ([]*Object, error)
	First(where Object) (Object, error)
}

type Model struct {
	tablename string
	DAL       DAL
	Type      *interface{}
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

	return Object(res.(Object)), err

}

func (m Model) Update(where Object, data Object, or ...bool) (int64, error) {

	glue := "AND"
	if len(or) > 0 && or[0] == true {
		glue = "OR"
	}

	query, values, err := __update(m.tablename, glue, where, data)
	if err != nil {
		panic("could not initialize Update query")
	}

	result, err := m.DAL.Exec(query, values)

	return result.RowsAffected()

}

func (m Model) Delete(where Object, or ...bool) (int64, error) {
	glue := "AND"
	if len(or) > 0 && or[0] == true {
		glue = "OR"
	}

	query, values, err := __delete(m.tablename, glue, where)
	if err != nil {
		panic("could not initialize Delete query")
	}

	result, err := m.DAL.Exec(query, values)

	return result.RowsAffected()

}

func (m Model) First(where Object, or ...bool) (Object, error) {

	glue := "AND"
	if len(or) > 0 && or[0] == true {
		glue = "OR"
	}

	query, values, err := __first(m.tablename, glue, where)
	if err != nil {
		panic("could not initialize First query")
	}

	out, err := m.DAL.Query(query, values)

	if err != nil {
		return nil, err
	}

	if len(out) != 1 {
		return nil, fmt.Errorf("EmptyResult")
	}

	return out[0], nil

}

func (m Model) Find(args ...any) ([]Object, error) {

	w := Object{}
	ob := Object{
		"id": DESC,
	}

	glue := "AND"
	if len(args) >= 1 {
		w = args[0].(Object)
		if len(args) >= 2 {
			ob = args[1].(Object)
			if len(args) >= 3 && args[2] == true {
				glue = "OR"
			}
		}

	}

	query, values, err := __find(m.tablename, glue, w, ob)

	if err != nil {
		panic("could not initialize Find query")
	}

	out, err := m.DAL.Query(query, values)

	if err != nil {
		return nil, err
	}

	return out, nil

}

type IDAL interface {
	Model(tablename string) (IModel, error)
}

type DAL struct {
	driver Driver
	uri    DataSourceName
	DB     *sql.DB
}

func (dal DAL) Model(tablename string, t any) Model {
	return Model{
		tablename: tablename,
		DAL:       dal,
		//Type:      t,
	}
}

func (dal DAL) Connect() (DAL, error) {
	db, err := sql.Open(string(dal.driver), string(dal.uri))
	if err != nil {
		return dal, err
	}
	dal.DB = db
	return dal, nil
}

func (dal DAL) Close() {
	db := dal.DB
	defer db.Close()
	dal.DB = nil
}

func (dal DAL) Exec(query string, values ...[]any) (sql.Result, error) {

	stmt, err := dal.DB.Prepare(query)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	if len(values) > 0 {
		return stmt.Exec(values[0]...)
	}

	return stmt.Exec()

}

func (dal DAL) Query(query string, values ...any) ([]Object, error) {

	stmt, err := dal.DB.Prepare(query)
	if err != nil {
		panic("could not prepare statement with query: " + query)
	}

	defer stmt.Close()

	var rs *sql.Rows

	fb := NewFieldBinding()
	if len(values) > 0 {
		rs, err = stmt.Query(values[0].([]any)...)
	} else {
		rs, err = stmt.Query()
	}

	if err != nil {
		return nil, err
	}

	var fArr []string
	if fArr, err = rs.Columns(); err != nil {
		return nil, err
	}

	defer rs.Close()

	fb.PutFields(fArr)

	outArr := []Object{}

	for rs.Next() {
		if err := rs.Scan(fb.GetFieldPtrArr()...); err != nil {
			return nil, err
		}
		outArr = append(outArr, fb.GetFieldArr())
	}

	return outArr, nil

}

func (dal DAL) Tx(ctx context.Context, qFn func(t *sql.Tx) (any, error)) (any, error) {

	tx, err := dal.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelDefault})
	if err != nil {
		return nil, err
	}

	rs, err := qFn(tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return nil, fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return nil, err
	}

	return rs, tx.Commit()

}

func NewDAL(driver Driver, uri DataSourceName) DAL {

	var dal = DAL{
		driver: driver,
		uri:    uri,
	}

	return dal

}
