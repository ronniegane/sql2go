package sql2go

import (
	"errors"
	"reflect"
	"fmt"
	"regexp"
	"sync"
	"database/sql"
	"strconv"
)

//Used to check if there are unbound parameters following bind loop
var placeholderPresent = regexp.MustCompile(`(\$[0-9])\b`)

//Houses pointer to DB, Query and map of types to their binding functions, includes mutex for concurrent access safety
type Sql2go struct {
	db        *sql.DB
	Query     func(stmt string, params ... interface{}) *SimpleQuery
	funcs     map[string]func(interface{}) string
	structMap map[string]map[string]int
	sync.RWMutex
}

// Contract for query structs
type Query interface {
	Select(stmt string) error
}

//Simply query struct, contains pointer back to parent
type SimpleQuery struct {
	sql2go   *Sql2go
	FetchOne func(v interface{}) error
	Fetch    func(v interface{}) error
	Stmt     string
	err      error
	sync.RWMutex
}

/**
 Create a new Sql2go and binder for primitive parameter binding
 */
func Connect(db *sql.DB) *Sql2go {
	sql2go := &Sql2go{
		db:        db,
		funcs:     make(map[string]func(interface{}) string),
		structMap: make(map[string]map[string]int),
	}

	sql2go.Query = func(stmt string, params ... interface{}) *SimpleQuery {
		var rows *sql.Rows

		q := &SimpleQuery{
			Stmt:   stmt,
			sql2go: sql2go,
		}

		q.err = bindParameters(q, params)

		if q.err != nil {
			return q
		}

		if placeholderPresent.MatchString(q.Stmt) {
			q.err = errors.New("not enough parameters")
			return q
		}

		q.FetchOne = func(dest interface{}) error {
			rows, q.err = q.sql2go.db.Query(q.Stmt)

			defer rows.Close()

			return scan(q, dest, rows, reflect.Struct)
		}

		q.Fetch = func(dest interface{}) error {
			rows, q.err = q.sql2go.db.Query(q.Stmt)

			defer rows.Close()

			return scan(q, dest, rows, reflect.Slice)
		}

		return q
	}

	sql2go.InitialiseBinder()

	return sql2go
}

func (sql2go *Sql2go) InitialiseBinder() {
	sql2go.RWMutex.Lock()

	var intFunc = func(v interface{}) string {
		return fmt.Sprintf("%d", v)
	}

	var floatFunc = func(v interface{}) string {
		return fmt.Sprintf("%f", v)
	}

	//Strings
	sql2go.funcs["string"] = func(v interface{}) string {
		return fmt.Sprintf("'%s'", v)
	}

	//Integers
	sql2go.funcs["int"] = intFunc
	sql2go.funcs["int"] = intFunc
	sql2go.funcs["int8"] = intFunc
	sql2go.funcs["int16"] = intFunc
	sql2go.funcs["int32"] = intFunc
	sql2go.funcs["int64"] = intFunc
	sql2go.funcs["uint"] = intFunc
	sql2go.funcs["uint8"] = intFunc
	sql2go.funcs["uint16"] = intFunc
	sql2go.funcs["uint32"] = intFunc
	sql2go.funcs["uint64"] = intFunc

	//Floats
	sql2go.funcs["float32"] = floatFunc
	sql2go.funcs["float64"] = floatFunc

	sql2go.funcs["bool"] = func(v interface{}) string {
		return fmt.Sprintf("%t", v)
	}

	sql2go.RWMutex.Unlock()
}

/**
	Attempt to scan rows into a structs, or arrays of structs
 */
func scan(sq *SimpleQuery, dest interface{}, rows *sql.Rows, expectedKind reflect.Kind) error {
	var shadow reflect.Value
	var value reflect.Value
	var baseType reflect.Type
	var fieldMap map[string]int
	var err error
	var ptrs []interface{}

	//Get the value
	value = reflect.ValueOf(dest)

	//Get pointer to underlying to access type, to determine if it's a slice.
	ptr := reflect.Indirect(value)

	if ptr.Kind() != expectedKind {
		return errors.New(fmt.Sprintf("Unexpected pointer kind: %s expected %s", ptr.Kind(), expectedKind))
	}

	cols, _ := rows.Columns()

	if ptr.Kind() == reflect.Slice {
		//Use slicer
		baseType = ptr.Type().Elem()
		shadow = reflect.New(ptr.Type().Elem())
		shadow = reflect.Indirect(shadow)

		//Get the field to column mapping
		fieldMap = setOrFind(sq, baseType)

		for rows.Next() {
			ptrs, err = mapColumnsToStructFields(cols, shadow, fieldMap)
			err = rows.Scan(ptrs...)

			ptr.Set(reflect.Append(ptr, shadow))
		}

		return err
	} else {
		//Struct
		shadow = value.Elem()
		baseType = ptr.Type()

		//Get the field to column mapping
		fieldMap = setOrFind(sq, baseType)

		//Scan into the pointers
		for rows.Next() {
			ptrs, err = mapColumnsToStructFields(cols, shadow, fieldMap)

			err = rows.Scan(ptrs...)
		}

		return err
	}
}

func mapColumnsToStructFields(cols []string, shadow reflect.Value, fieldMap map[string]int) ([]interface{}, error) {
	var ptrs []interface{}
	ptrs = make([]interface{}, len(cols))

	//fmt.Println(reflect.Indirect(shadow).Kind())

	for i := 0; i < len(cols); i ++ {
		if _, ok := fieldMap[cols[i]]; ok {
			field := shadow.Field(int(fieldMap[cols[i]]))

			ptrs[i] = field.Addr().Interface()
		} else {
			var v string
			ptrs[i] = &v
		}
	}
	return ptrs, nil
}

func setOrFind(sq *SimpleQuery, typ reflect.Type) map[string]int {
	cache := true

	sq.sql2go.RLock()

	//Determine if we should cache the struct fields
	if _, ok := sq.sql2go.structMap[typ.Name()]; ok {
		cache = true
	}

	sq.sql2go.RUnlock()

	if cache {
		sq.sql2go.Lock()

		var fieldMap map[string]int
		fieldMap = make(map[string]int, typ.NumField())

		var field reflect.StructField

		for i := 0; i < typ.NumField(); i++ {
			field = typ.Field(i)

			if &field.Tag != nil {
				fieldMap[field.Tag.Get("db")] = i
			} else {
				fieldMap[field.Name] = i
			}
		}

		sq.sql2go.structMap[typ.Name()] = fieldMap

		sq.sql2go.Unlock()
	}

	return sq.sql2go.structMap[typ.Name()]
}

func bindParameters(sq *SimpleQuery, params []interface{}) error {
	var err error

	if len(params) > 0 {
		for i, v := range params {
			rgx := regexp.MustCompile(`(\$` + strconv.Itoa(i+1) + `)\b`)

			if !rgx.MatchString(sq.Stmt) {
				err = errors.New("error in ordinal binding, could not replace parameter : $" + strconv.Itoa(i+1))
				break
			}

			//Check type and bind
			err = sq.CheckTypeAndBind(v, i, rgx)

			if err != nil {
				return err
				break
			}
		}
	}

	return err
}

func (sq *SimpleQuery) CheckTypeAndBind(value interface{}, ind int, rgx *regexp.Regexp) error {
	t := reflect.TypeOf(value)
	f := (func() func(interface{}) string {
		var f func(interface{}) string

		sq.RLock()
		f = sq.sql2go.funcs[t.Name()]
		sq.RUnlock()

		return f
	})()

	if f == nil {
		return errors.New("Unsupported type: " + t.Name())
	}

	sq.Stmt = rgx.ReplaceAllString(sq.Stmt, f(value))

	return nil
}
