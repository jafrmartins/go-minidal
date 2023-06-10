package main

import (
	"fmt"
	"strconv"

	minidal "github.com/jafrmartins/go-minidal/lib"

	//_ "github.com/mattn/go-sqlite3"
	_ "github.com/go-sql-driver/mysql"
)

type Demo struct {
	Id      int64
	Message string
	Enabled int64
}

func main() {

	var tablename = "demo"

	/*

		var dialect = minidal.SQLite
		var connectionString minidal.DataSource = "sqlite.db"

		os.Remove(string(connectionString))

		_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message TEXT NOT NULL,
			enabled INTEGER DEFAULT 1 NOT NULL
		);`, tablename))

	*/

	var dialect = minidal.MySQL
	var connectionString minidal.DataSource = "root:password@tcp(localhost:3306)/demo"

	db, err := minidal.NewDB(dialect, connectionString).Connect()
	defer db.Close()

	if err != nil {
		panic(err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	DROP TABLE IF EXISTS %s;
	`, tablename))

	if err != nil {
		panic(err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	CREATE TABLE %s (
		id int NOT NULL AUTO_INCREMENT,
		message varchar(255) NOT NULL,
		enabled int NOT NULL DEFAULT 1,
		PRIMARY KEY (id)
	);`, tablename))

	if err != nil {
		panic(err)
	}

	//DemoModel := db.Model(tablename)
	DemoModel := db.Model("demo", &Demo{})
	//DemoModel := db.Model(&Demo{})

	id, err := DemoModel.Insert(minidal.Object{
		"message": tablename + " inserted!",
	})

	if err != nil {
		panic(err)
	}

	println("Insert InsertID:" + strconv.Itoa(int(id)))

	model, err := DemoModel.First(minidal.Object{
		"id": id,
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("Custom Model: %+v\n", model)

	//println("message: " + string(model["message"].(string)))

	rows, err := DemoModel.Update(minidal.Object{
		"id": id,
	}, minidal.Object{
		"message": tablename + " updated!",
	})

	if err != nil {
		panic(err)
	}

	println("Update AffectedRows:" + strconv.Itoa(int(rows)))

	model, err = DemoModel.First(minidal.Object{
		"id": id,
	})

	fmt.Printf("Custom Model Message:%s\n", model.(Demo).Message)

	if err != nil {
		panic(err)
	}

	println("message: " + string(model.(Demo).Message))

	rows, err = DemoModel.Delete(minidal.Object{
		"id": id,
	})

	println("Delete AffectedRows:" + strconv.Itoa(int(rows)))

	if err != nil {
		panic(err)
	}

	result, err := DemoModel.InsertBulk(minidal.Object{
		"message": tablename + " bulk inserted!",
	}, minidal.Object{
		"message": tablename + " also bulk inserted!",
	})

	fmt.Printf("BulkInsert: %v\n", result)
	id = result["LastInsertId"].(int64)

	rows, err = DemoModel.Update(minidal.Object{
		"id": result["LastInsertId"].(int64),
	}, minidal.Object{
		"enabled": 0,
	})

	if err != nil {
		panic(err)
	}

	models, err := DemoModel.Find(minidal.Object{
		"enabled": 1,
		"id":      3,
	}, minidal.Object{"id": minidal.DESC}, minidal.OR)

	if err != nil {
		panic(err)
	}

	fmt.Printf("Find Models: %+v\n", models)

	rs, err := db.Query(`
	SELECT * FROM demo WHERE id = ?
	`, []any{2})

	if err != nil {
		panic(err)
	}

	model, err = DemoModel.Deserialize(rs...)

	if err != nil {
		panic(err)
	}

	fmt.Printf("selected id 2: %s\n", strconv.Itoa(int(model.(Demo).Id)))

}
