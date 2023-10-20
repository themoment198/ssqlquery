package ssqlquery

import (
	"context"
	"database/sql"
	"errors"
	"golang.org/x/sync/singleflight"
	"reflect"
	"sync"
)

var sfInst singleflight.Group

type cacheTypeInfo struct {
	elemByType reflect.Type
	fieldNames []string
	err        error
}

var (
	cacheMap = &sync.Map{} // typeStr -> cacheTypeInfo
)

var (
	IsNotPtrOfSliceStructTypeErr = errors.New(`is not '*[]struct' type`)
	NoFeildStructErr             = errors.New("must be one feild at least")
	FieldIsNotBaseTypeErr        = errors.New("field must be base type")
	FieldIsAnonymousErr          = errors.New("field can not be anonymous")
	FieldIsNotExportErr          = errors.New("field must be export")
	NoSQLTagErr                  = errors.New("tag 'sql' is empty")
	DbHandleErr                  = errors.New("dbHandle must be *sql.DB or *sql.Tx")
)

type query interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func QueryContext(ctx context.Context, dbHandle interface{}, objectModel interface{}, q string, args ...interface{}) error {
	// check dbHandle
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
	switch tp.Kind() {
	case reflect.Ptr:
		if tp.Elem().Kind() != reflect.Slice {
			return IsNotPtrOfSliceStructTypeErr
		}
		if tp.Elem().Elem().Kind() != reflect.Struct {
			return IsNotPtrOfSliceStructTypeErr
		}
	default:
		return IsNotPtrOfSliceStructTypeErr
	}

	// get value of slice
	result := reflect.ValueOf(objectModel).Elem()

	// get type of struct
	elemByType := tp.Elem().Elem()

	// check field count
	if elemByType.NumField() == 0 {
		return NoFeildStructErr
	}

	// cache type info
	typeName := elemByType.String()

	cti, err, _ := sfInst.Do(typeName, func() (interface{}, error) {
		cti, ok := cacheMap.Load(typeName)
		if ok {
			realCti := cti.(*cacheTypeInfo)
			if realCti.err != nil {
				return nil, realCti.err
			}

			return cti, nil
		}

		realCti := &cacheTypeInfo{
			elemByType: elemByType,
			fieldNames: nil,
			err:        nil,
		}

		for i := 0; i < elemByType.NumField(); i++ {
			sf := elemByType.Field(i)

			// check field
			switch sf.Type.Kind() {
			case reflect.Uintptr, reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Struct, reflect.UnsafePointer:
				realCti.err = FieldIsNotBaseTypeErr
				return nil, FieldIsNotBaseTypeErr
			case reflect.Slice:
				if sf.Type.Elem().Kind() != reflect.Uint8 {
					realCti.err = FieldIsNotBaseTypeErr
					return nil, FieldIsNotBaseTypeErr
				}
			}
			if sf.Anonymous {
				realCti.err = FieldIsAnonymousErr
				return nil, FieldIsAnonymousErr
			}
			if sf.IsExported() == false {
				realCti.err = FieldIsNotExportErr
				return nil, FieldIsNotExportErr
			}
			tag := sf.Tag.Get("sql")
			if tag == "" {
				realCti.err = NoSQLTagErr
				return nil, NoSQLTagErr
			}
			realCti.fieldNames = append(realCti.fieldNames, tag)
		}

		cacheMap.Store(typeName, realCti)
		return realCti, nil
	})
	if err != nil {
		return err
	}
	realCti := cti.(*cacheTypeInfo)

	// alloc struct
	elemByValue := reflect.New(elemByType)
	fieldNames := make(map[string]interface{})
	for k, v := range realCti.fieldNames {
		fieldNames[v] = elemByValue.Elem().Field(k).Addr().Interface()
	}

	rows, err := db.QueryContext(ctx, q, args...)
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
			rowTypes = append(rowTypes, &sqlRawBytesInst)
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

var sqlRawBytesInst = make(sql.RawBytes, 0)
