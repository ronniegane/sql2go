package sql2go

import (
	"testing"
	_ "github.com/lib/pq"
	_ "fmt"
	_ "database/sql"
	"database/sql"
	"os"
	"fmt"
)

type Thing struct {
	Col string `db:"col"`
	Two string `db:"two"`
}

func getDatabase() (*sql.DB, error) {
	dbName := os.Getenv("CI_DB")

	if len(dbName) == 0 {
		dbName = "postgres"
		return sql.Open("postgres", "user=GREX password=QWERTY dbname="+dbName+" sslmode=disable")
	} else {
		return sql.Open("postgres", "user=postgres password= dbname="+dbName+" sslmode=disable")
	}
}

func TestSimpleFetchOneShouldError(t *testing.T) {
	var db, _ = getDatabase()
	setupTable(db)

	v := struct {
		Col string `db:"col"`
		Two string `db:"two"`
	}{}

	Connect(db).Query("").FetchOne(&v)
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

	err := Connect(db).Query("SELECT col, 3 three, 4 four, 2.0 two FROM tmp").FetchOne(&v)

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

	err := Connect(db).Query("SELECT col, 3 three, $1 two FROM tmp", "Two").FetchOne(&v)

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

	err := Connect(db).Query("SELECT col, 3 three, $1 two FROM tmp", "Two").Fetch(&v)

	if err != nil {
		t.Error("Should not return error, got ", err)
	}
}


func TestAddParameterSucceed(t *testing.T) {
	var db, _ = getDatabase()
	setupTable(db)

	v := struct {
		Col  string  `db:"col"`
		Two  float32 `db:"two"`
		Four int64   `db:"four"`
	}{}

	ref := Connect(db)

	q := ref.Query("SELECT col, 3 three, 4 four, :param two FROM tmp").AddParameter("param", 2.02)

	err := q.FetchOne(&v)

	q = ref.Query("SELECT col, 3 three, 4 four, :param two FROM tmp").AddParameter("param", 2.03)

	err = q.FetchOne(&v)

	if err != nil {
		t.Error("Should not return an error parameter should be mapped")
	}
}



func TestNotEnoughParameters(t *testing.T) {
	ref := Connect(nil)

	q := ref.Query("SELECT col, 3 three, :param2 four, :param two FROM tmp").AddParameter("param", 2.02)

	if q.err == nil {
		t.Error("Should fail when there are unbound parameters")
	}
}

func TestExec(t *testing.T){
	var db, _ = getDatabase()
	setupTable(db)

	_, err := Connect(db).Query("INSERT INTO tmp (col) VALUES($1)", "MAUI").Exec()

	if err != nil {
		t.Error("Should not error on insert")
	}
}

func BenchmarkParameterBind(b *testing.B){
	ref := Connect(nil)
	q := ref.Query("SELECT col, 3 three, 4 four, :param two FROM tmp")

	b.ResetTimer()

	//for n := 0; n < b.N; n++ {
		q = ref.Query("SELECT col, :param three, 4 four, :param2 two FROM tmp")
		q.AddParameter("param", 1)

		fmt.Println(q.Stmt, q.err)
	//}
}