package ormy

import (
	"testing"
	_ "github.com/lib/pq"
	_ "fmt"
	_ "database/sql"
	"fmt"
	"database/sql"
)

type Thing struct {
	Col   string `db:"col"`
	Two   string `db:"two"`
}

func TestSimpleFetchOneShouldError(t *testing.T) {
	var db, _ = sql.Open("postgres", "user=GREX password=QWERTY dbname=postgres sslmode=disable")

	v := struct {
		Col   string `db:"col"`
		Two   string `db:"two"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, 3 three, $1 two FROM tmp", "Two").One(&v)

	if err == nil {
		t.Error("Should return an error when fields cannot be mapped")
	}
}


func TestSimpleFetchOneShouldSucceed(t *testing.T) {
	var db, _ = sql.Open("postgres", "user=GREX password=QWERTY dbname=postgres sslmode=disable")

	v := struct {
		Col   string `db:"col"`
		Two   float32 `db:"two"`
		Four  int64 `db:"four"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, 3 three, 4 four, 2.0 two FROM tmp").One(&v)

	if err != nil {
		t.Error("Should not return an error when fields can be mapped")
	}
}

func TestSimpleFetchAllShouldError(t *testing.T) {
	var db, _ = sql.Open("postgres", "user=GREX password=QWERTY dbname=postgres sslmode=disable")

	v := []struct {
		Col   string `db:"col"`
		Two   string `db:"two"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, 3 three, $1 two FROM tmp", "Two").One(&v)

	fmt.Println(err)

	if err == nil {
		t.Error("Should return an error when fields cannot be mapped")
	}
}


func TestSimpleFetchAllShouldSucceed(t *testing.T) {
	var db, _ = sql.Open("postgres", "user=GREX password=QWERTY dbname=postgres sslmode=disable")

	v := []struct {
		Col   string `db:"col"`
		Two   string `db:"two"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, 3 three, $1 two FROM tmp  LIMIT 2", "Two").All(&v)

	fmt.Println(err)

	fmt.Println(v)
}
