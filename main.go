package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	minidal "github.com/jafrmartins/go-minidal/lib"

	//_ "github.com/mattn/go-sqlite3"
	_ "github.com/go-sql-driver/mysql"
)

type Demo struct {
	id      int64  `json:"intValue"`
	message string `json:"stringValue"`
	enabled int    `json:"intValue"`
}

func (d *Demo) New(o minidal.Object) (*Demo, error) {
	for k, v := range o {
		err := minidal.SetField(d, k, v)
		if err != nil {
			return d, err
		}
	}
	return d, nil
}

func main() {

	//var dialect = minidal.SQLite
	//var connectionString = "./sqlite.db"
	var dialect = minidal.MySQL
	var connectionString = "root:password@tcp(localhost:3306)/demo"
	var modelName = "demo"

	os.Remove(connectionString)

	fmt.Println("GODAL Example")

	db, err := minidal.NewDAL(dialect, minidal.DataSourceName(connectionString)).Connect()
	defer db.DB.Close()

	if err != nil {
		panic(errors.New("Could not connect to database"))
	}

	/*_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS demo (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		message TEXT NOT NULL,
		enabled INTEGER DEFAULT 1 NOT NULL
	);`)
	*/

	_, err = db.Exec(`
	DROP TABLE IF EXISTS demo;
	`)

	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`
	CREATE TABLE demo (
		id int NOT NULL AUTO_INCREMENT,
		message varchar(255) NOT NULL,
		enabled int NOT NULL DEFAULT 1,
		PRIMARY KEY (id)
	);`)

	if err != nil {
		panic(err)
	}

	Model := db.Model(modelName, Demo{})

	id, err := Model.Insert(minidal.Object{
		"message": modelName + " inserted!",
	})

	if err != nil {
		panic(err)
	}

	println("Insert InsertID:" + strconv.Itoa(int(id)))

	model, err := Model.First(minidal.Object{
		"id": id,
	})

	if err != nil {
		panic(err)
	}

	println("message: " + string(model["message"].([]byte)))

	rows, err := Model.Update(minidal.Object{
		"id": id,
	}, minidal.Object{
		"message": modelName + " updated!",
	})

	if err != nil {
		panic(err)
	}

	println("Update AffectedRows:" + strconv.Itoa(int(rows)))

	model, err = Model.First(minidal.Object{
		"id": id,
	})

	//d := &Demo{}
	//d, err = d.New(model)

	//fmt.Printf("Custom Model: %+v\n", d)

	if err != nil {
		panic(err)
	}

	println("message: " + string(model["message"].([]byte)))

	rs, err := db.Query(`
	SELECT message FROM demo WHERE id = ?
	`, []any{1})

	println("message: " + string(rs[0]["message"].([]byte)))

	rows, err = Model.Delete(minidal.Object{
		"id": id,
	})

	println("Delete AffectedRows:" + strconv.Itoa(int(rows)))

	if err != nil {
		panic(err)
	}

	result, err := Model.InsertBulk(minidal.Object{
		"message": modelName + " bulk inserted!",
	}, minidal.Object{
		"message": modelName + " also bulk inserted!",
	})

	fmt.Printf("BulkInsert: %v\n", result)
	id = result["LastInsertId"].(int64)

	rows, err = Model.Update(minidal.Object{
		"id": result["LastInsertId"].(int64),
	}, minidal.Object{
		"enabled": 0,
	})

	if err != nil {
		panic(err)
	}

	models, err := Model.Find(minidal.Object{
		"enabled": 1,
		"id":      3,
	}, minidal.Object{"id": minidal.DESC}, true)

	if err != nil {
		panic(err)
	}

	fmt.Printf("Find Models: %+v\n", models)

	rs, err = db.Query(`
	SELECT * FROM demo WHERE id = ?
	`, []any{2})

	fmt.Printf("selected id 2: %s\n", strconv.Itoa(int(rs[0]["id"].(int64))))

}
