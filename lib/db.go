package minidal

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

type DataSource string

type IDB interface {
	Model(options ...any) (IModel, error)

	Connect() (DB, error)
	Close() error
	Exec(query string, values ...[]any) (sql.Result, error)
	Query(query string, values ...any) ([]Object, error)
	Tx()
}

type DB struct {
	driver Driver
	uri    DataSource
	DB     *sql.DB
}

func (dal DB) Model(options ...any) Model {

	if len(options) < 1 || len(options) > 2 {
		panic("invalid model ivocation please provide a tablename or a schema")
	}

	var schema any
	var tablename string

	if len(options) >= 1 {
		if reflect.TypeOf(options[0]).String() == "string" {
			tablename = options[0].(string)
		} else {
			schema = options[0]
		}
	}

	if len(options) == 2 {

		if reflect.TypeOf(options[1]).String() == "string" {
			tablename = options[1].(string)
		} else {
			schema = options[1]
		}

	}

	return Model{
		tablename: tablename,
		schema:    schema,
		DAL:       dal,
	}

}

func (dal DB) Connect() (DB, error) {
	db, err := sql.Open(string(dal.driver), string(dal.uri))
	if err != nil {
		return dal, err
	}
	dal.DB = db
	return dal, nil
}

func (dal *DB) Close() {
	defer dal.DB.Close()
}

func (dal DB) Exec(query string, values ...[]any) (sql.Result, error) {

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

func (dal DB) Query(query string, values ...any) ([]Object, error) {

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

func (dal DB) Tx(ctx context.Context, qFn func(t *sql.Tx) (any, error)) (any, error) {

	tx, err := dal.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelDefault})
	if err != nil {
		return nil, err
	}

	cstd := make(chan interface{})
	cerr := make(chan error)

	go func() {
		rs, err := qFn(tx)
		cerr <- err
		cstd <- rs
	}()

	err = <-cerr
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return nil, fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return <-cstd, nil

}

func NewDB(driver Driver, uri DataSource) DB {

	var dal = DB{
		driver: driver,
		uri:    uri,
	}

	return dal

}
