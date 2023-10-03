package ssqlquery

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

var chanByEmptyStructPool = sync.Pool{
	New: func() interface{} {
		return make(chan struct{})
	},
}

var rawBytesPool = sync.Pool{
	New: func() interface{} {
		return new(sql.RawBytes)
	},
}

type cacheTypeInfo struct {
	elemByType reflect.Type
	fieldNames map[string]interface{}
	ch         chan struct{}
}

var (
	split                   = `\/`
	cacheMap                = &sync.Map{}
	IsNotPtrOfStructTypeErr = errors.New(`is not '*[]struct' type`)
	NoFeildStructErr        = errors.New("must be one feild at least")
	FieldNotBeStructErr     = errors.New("field can not be struct")
	FieldNotBeAnonymous     = errors.New("field can not be anonymous")
	NoSQLTagErr             = errors.New("tag 'sql' is empty")
	ColDupErr               = errors.New("not allow dup col")
	DbHandleErr             = errors.New("dbHandle must be *sql.DB or *sql.Tx")
	StructErr               = errors.New("struct not right")
)

type query interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func Query(dbHandle interface{}, objectModel interface{}, q string, args ...interface{}) error {
	var db query

	switch obj := dbHandle.(type) {
	case *sql.DB:
		db = obj
	case *sql.Tx:
		db = obj
	default:
		return DbHandleErr
	}

	// assume objectModel type is *[]struct
	tp := reflect.TypeOf(objectModel)
	kd := tp.Kind()
	switch kd {
	case reflect.Ptr:
		if tp.Elem().Kind() != reflect.Slice {
			return IsNotPtrOfStructTypeErr
		}
		if tp.Elem().Elem().Kind() != reflect.Struct {
			return IsNotPtrOfStructTypeErr
		}
	default:
		return IsNotPtrOfStructTypeErr
	}

	// get value of slice
	result := reflect.ValueOf(objectModel).Elem()

	// get type of struct
	elemByType := reflect.TypeOf(objectModel).Elem().Elem()

	// check field count
	if elemByType.NumField() == 0 {
		return NoFeildStructErr
	}

	ch := chanByEmptyStructPool.Get().(chan struct{})

	// cache type info
	typeMark := elemByType.PkgPath() + "." + elemByType.Name()
	cacheTypeInfoInst, has := cacheMap.LoadOrStore(typeMark, &cacheTypeInfo{elemByType: elemByType, ch: ch})
	real := cacheTypeInfoInst.(*cacheTypeInfo)
	if !has {
		fieldNames := make(map[string]interface{})
		for i := 0; i < elemByType.NumField(); i++ {
			if elemByType.Field(i).Type.Kind() == reflect.Struct {
				cacheMap.Delete(typeMark)
				close(real.ch)
				return FieldNotBeStructErr
			}

			if elemByType.Field(i).Anonymous {
				cacheMap.Delete(typeMark)
				close(real.ch)
				return FieldNotBeAnonymous
			}

			tag := elemByType.Field(i).Tag.Get("sql")
			if tag == "" {
				cacheMap.Delete(typeMark)
				close(real.ch)
				return NoSQLTagErr
			}

			fieldNames[tag+split+fmt.Sprintf("%d", i)] = nil
		}
		real.fieldNames = fieldNames
		close(real.ch)
	} else {
		chanByEmptyStructPool.Put(ch)
		<-real.ch

		// check 'cacheMap' again
		_, ok := cacheMap.Load(typeMark)
		if !ok {
			return StructErr
		}
	}

	// alloc type with cache type info
	elemByValue := reflect.New(elemByType)
	fieldNames := make(map[string]interface{})
	for k, _ := range real.fieldNames {
		fieldKey := strings.Split(k, split)
		index, _ := strconv.Atoi(fieldKey[1])
		fieldNames[fieldKey[0]] = elemByValue.Elem().Field(index).Addr().Interface()
	}

	rows, err := db.Query(q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// map tag 'sql' field
	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	rowTypes := make([]interface{}, 0)
	for _, v := range types {
		if _, ok := fieldNames[v.Name()]; ok {
			rowTypes = append(rowTypes, fieldNames[v.Name()])
		} else {
			rawBytes := rawBytesPool.Get().(*sql.RawBytes)
			defer rawBytesPool.Put(rawBytes)
			rowTypes = append(rowTypes, rawBytes)
		}
	}

	// read row, and append to result
	for rows.Next() {
		err := rows.Scan(rowTypes...)
		if err != nil {
			return err
		}
		result.Set(reflect.Append(result, elemByValue.Elem()))
	}
	return nil
}
