package ormy

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
type Ormy struct {
	db        *sql.DB
	Query     *SimpleQuery
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
	ormy *Ormy
	Stmt string
	sync.RWMutex
}

//Fetchable, houses One and All (TODO) functions
type Fetch struct {
	err error
	One func(v interface{}) error
	All func(v interface{}) error
}

/**
 Create a new Ormy and binder for primitive parameter binding
 */
func newOrmy(db *sql.DB) *Ormy {
	ormy := &Ormy{
		db:        db,
		funcs:     make(map[string]func(interface{}) string),
		structMap: make(map[string]map[string]int),
	}

	ormy.Query = &SimpleQuery{
		Stmt: "",
		ormy: ormy,
	}

	ormy.InitialiseBinder()

	return ormy
}

func (ormy *Ormy) InitialiseBinder() {
	ormy.RWMutex.Lock()

	var intFunc = func(v interface{}) string {
		return fmt.Sprintf("%d", v)
	}

	var floatFunc = func(v interface{}) string {
		return fmt.Sprintf("%f", v)
	}

	//Strings
	ormy.funcs["string"] = func(v interface{}) string {
		return fmt.Sprintf("'%s'", v)
	}

	//Integers
	ormy.funcs["int"] = intFunc
	ormy.funcs["int"] = intFunc
	ormy.funcs["int8"] = intFunc
	ormy.funcs["int16"] = intFunc
	ormy.funcs["int32"] = intFunc
	ormy.funcs["int64"] = intFunc
	ormy.funcs["uint"] = intFunc
	ormy.funcs["uint8"] = intFunc
	ormy.funcs["uint16"] = intFunc
	ormy.funcs["uint32"] = intFunc
	ormy.funcs["uint64"] = intFunc

	//Floats
	ormy.funcs["float32"] = floatFunc
	ormy.funcs["float64"] = floatFunc

	ormy.funcs["bool"] = func(v interface{}) string {
		return fmt.Sprintf("%t", v)
	}

	ormy.RWMutex.Unlock()
}

func (sq *SimpleQuery) Select(stmt string, params ... interface{}) *Fetch {
	sq.Stmt = stmt

	//Bind parameters and create a fetch struct
	fetch := bindParameters(sq, params)

	if placeholderPresent.MatchString(sq.Stmt) {
		fetch.err = errors.New("not enough parameters")
	}

	//Fetch one row
	fetch.One = func(dest interface{}) error {
		if fetch.err != nil {
			return fetch.err
		}

		rows, err := sq.ormy.db.Query(sq.Stmt)

		if err != nil {
			return err
		}

		defer rows.Close()

		return scan(sq, dest, rows, reflect.Struct)
	}

	fetch.All = func(dest interface{}) error {
		if fetch.err != nil {
			return fetch.err
		}

		rows, err := sq.ormy.db.Query(sq.Stmt)

		if err != nil {
			return err
		}

		defer rows.Close()

		return scan(sq, dest, rows, reflect.Slice)
	}

	return fetch
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

	sq.ormy.RLock()

	//Determine if we should cache the struct fields
	if _, ok := sq.ormy.structMap[typ.Name()]; ok {
		cache = true
	}

	sq.ormy.RUnlock()

	if cache {
		sq.ormy.Lock()

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

		sq.ormy.structMap[typ.Name()] = fieldMap

		sq.ormy.Unlock()
	}

	return sq.ormy.structMap[typ.Name()]
}

func bindParameters(sq *SimpleQuery, params []interface{}) *Fetch {
	var err error

	fetch := &Fetch{
		err: nil,
	}

	if len(params) > 0 {
		for i, v := range params {
			rgx := regexp.MustCompile(`(\$` + strconv.Itoa(i+1) + `)\b`)

			if !rgx.MatchString(sq.Stmt) {
				fetch.err = errors.New("error in ordinal binding, could not replace parameter : $" + strconv.Itoa(i+1))
				break
			}

			//Check type and bind
			err = sq.CheckTypeAndBind(v, i, rgx)

			if err != nil {
				fetch.err = err
				break
			}
		}
	}

	fetch.err = err

	return fetch
}


func (sq *SimpleQuery) CheckTypeAndBind(value interface{}, ind int, rgx *regexp.Regexp) error {
	t := reflect.TypeOf(value)
	f := (func() func(interface{}) string {
		var f func(interface{}) string

		sq.RLock()
		f = sq.ormy.funcs[t.Name()]
		sq.RUnlock()

		return f
	})()

	if f == nil {
		return errors.New("Unsupported type: " + t.Name())
	}

	sq.Stmt = rgx.ReplaceAllString(sq.Stmt, f(value))

	return nil
}
