package minidal

import (
	"fmt"
	"strings"
)

type Driver string

const (
	SQLite Driver = "sqlite3" // tested with "github.com/mattn/go-sqlite3"
	MySQL  Driver = "mysql"   // tested with "github.com/go-sql-driver/mysql"
)

type SortOrder string

const (
	ASC  SortOrder = "ASC"
	DESC SortOrder = "DESC"
)

type GlueOperator string

const (
	AND GlueOperator = "AND"
	OR  GlueOperator = "OR"
)

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

	setWhere, values, _, err := __where(where, values, i)

	if err != nil {
		panic(fmt.Errorf("invalid Where Clause %v", where))
	}

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(set, ","), strings.Join(setWhere, glue)), values, nil
}

func __first(table string, glue string, where Object) (string, []any, error) {

	i := 0
	values := make([]any, len(where))
	setWhere, values, _, err := __where(where, values, i)

	if err != nil {
		panic(fmt.Errorf("invalid Where Clause %v", where))
	}

	return fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1", table, strings.Join(setWhere, glue)), values, nil

}

func __find(table string, glue string, where Object, orderby Object) (string, []any, error) {

	i := 0
	values := make([]any, len(where))
	setWhere, values, _, err := __where(where, values, i)
	if err != nil {
		panic(fmt.Errorf("invalid Where Clause %v", where))
	}

	whereClause := ""
	if len(values) > 0 {
		whereClause = "WHERE "
	}

	setOrderBy, err := __orderby(orderby)

	if err != nil {
		panic(fmt.Errorf("invalid OrderBy Clause %v", orderby))
	}

	return fmt.Sprintf("SELECT * FROM %s %s%s ORDER BY %s", table, whereClause, strings.Join(setWhere, " "+glue+" "), strings.Join(setOrderBy, ", ")), values, nil

}

func __delete(table string, glue string, where Object) (string, []any, error) {

	values := make([]any, len(where))

	i := 0
	deleteWhere, values, _, err := __where(where, values, i)
	if err != nil {
		panic(fmt.Errorf("invalid Where Clause %v", where))
	}

	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(deleteWhere, glue)), values, nil

}
