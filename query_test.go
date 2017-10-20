package ormy

import (
	"testing"
	_ "github.com/lib/pq"
	_ "fmt"
	_ "database/sql"
	"fmt"
	"database/sql"
	"log"
	"os"
)

type Thing struct {
	Col string `db:"col"`
	Two string `db:"two"`
}

func getDatabase() (*sql.DB, error) {
	dbName := os.Getenv("CI_DB")

	if len(dbName) == 0 {
		dbName = "postgres"
		return sql.Open("postgres", "user=GREX password=QWERTY dbname=" + dbName + " sslmode=disable")
	} else {
		return sql.Open("postgres", "user=postgres password= dbname=" + dbName + " sslmode=disable")
	}
}

func TestSimpleFetchOneShouldError(t *testing.T) {
	var db, _ = getDatabase()
	setupTable(db)

	v := struct {
		Col string `db:"col"`
		Two string `db:"two"`
	}{}

	err := newOrmy(db) .Query.Select("SELECT col, 3 three, $1 two FROM tmp", "Two").One(&v)

	log.Print(err)
}

func setupTable(db *sql.DB) {
	db.Exec("CREATE TABLE IF NOT EXISTS tmp (col VARCHAR(10))")
	db.Exec("INSERT INTO tmp (col) VALUES($1) WHERE NOT EXISTS(SELECT * FROM tmp)", "1")
}

func TestSimpleFetchOneShouldSucceed(t *testing.T) {
	var db, _ = getDatabase()
	setupTable(db)

	v := struct {
		Col  string  `db:"col"`
		Two  float32 `db:"two"`
		Four int64   `db:"four"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, 3 three, 4 four, 2.0 two FROM tmp").One(&v)

	if err != nil {
		t.Error("Should not return an error when fields can be mapped")
	}
}

func TestSimpleFetchAllShouldError(t *testing.T) {
	var db, _ = getDatabase()
	setupTable(db)

	v := []struct {
		Col string `db:"col"`
		Two string `db:"two"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, 3 three, $1 two FROM tmp", "Two").One(&v)

	fmt.Println(err)

	if err == nil {
		t.Error("Should return an error when fields cannot be mapped")
	}
}

func TestSimpleFetchAllShouldSucceed(t *testing.T) {
	var db, _ = getDatabase()
	setupTable(db)

	v := []struct {
		Col string `db:"col"`
		Two string `db:"two"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, 3 three, $1 two FROM tmp", "Two").All(&v)

	if err != nil {
		t.Error("Should not return error, got ", err)
	}
}
