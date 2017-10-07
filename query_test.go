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

func TestSimpleStructFetchShouldError(t *testing.T) {
	var db, _ = sql.Open("postgres", "user=USER password=PASS dbname=postgres sslmode=disable")

	v := struct {
		Col   string `db:"col"`
		Two   string `db:"two"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, 3 three, $1 two FROM tmp", "Two").One(&v)

	if err == nil {
		t.Error("Should return an error when fields cannot be mapped")
	}
}


func TestSimpleStructFetchShouldSucceed(t *testing.T) {
	var db, _ = sql.Open("postgres", "user=GREX password=QWERTY dbname=postgres sslmode=disable")

	v := struct {
		Col   string `db:"col"`
		Two   string `db:"two"`
	}{}

	err := newOrmy(db).Query.Select("SELECT col, $1 two FROM tmp", "Two").One(&v)

	if err != nil {
		t.Error("Should not return an error when fields can be mapped")
	}

	fmt.Println(v)
}