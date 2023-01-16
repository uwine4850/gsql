package gsql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"gsql/internal/errors"
	"reflect"
)

type Model struct {
	TableName string
	Connector *sql.DB
}

func (m *Model) All(dest interface{}) {
	switch reflect.TypeOf(dest).Elem().Kind() {
	case reflect.Slice:
		destType := reflect.TypeOf(dest).Elem().Elem()
		directValue := reflect.ValueOf(dest)
		direct := reflect.Indirect(directValue)

		if rows, err := m.Connector.Query(fmt.Sprintf("SELECT * FROM %s", m.TableName)); err != nil {
			defer rows.Close()
			panic(err)
		} else {
			defer rows.Close()

			currValueSlice := m.setDestValue(destType, rows, 0)

			for i := 0; i < len(currValueSlice); i++ {
				direct.Set(reflect.Append(direct, currValueSlice[i]))
			}
		}
	case reflect.Struct:
		dValue := reflect.ValueOf(dest)
		destVal := dValue.Elem()
		destType := reflect.TypeOf(dest).Elem()

		if rows, err := m.Connector.Query(fmt.Sprintf("SELECT * FROM %s", m.TableName)); err != nil {
			defer rows.Close()
			panic(err)
		} else {
			defer rows.Close()

			currValueSlice := m.setDestValue(destType, rows, 1)
			destVal.Set(currValueSlice[0])
		}
	default:
		//TODO: Error
	}
}

// Initialization of destination data (structures) considering all data types
// Only those fields that match in the map and structure are filled
func (m *Model) setDestValue(destType reflect.Type, rows *sql.Rows, count int) []reflect.Value {
	var destValueSlice []reflect.Value
	mRows, err := m.GetTableRows(rows, count)
	if err != nil {
		panic(err)
	}
	for _, row := range mRows {
		destVal := reflect.Indirect(reflect.New(destType))
		for i := 0; i < destVal.NumField(); i++ {
			if dfVal := row[destType.Field(i).Tag.Get("db")]; dfVal != nil {
				destValueField := destVal.Field(i)
				switch destType.Field(i).Type.Kind() {
				case reflect.Int:
					v := int64(dfVal.(float64))
					destValueField.SetInt(v)
				case reflect.String:
					v := dfVal.(string)
					destValueField.SetString(v)
				}
			}
		}
		destValueSlice = append(destValueSlice, destVal)
	}
	return destValueSlice
}

// GetTableRows Returns a slice from a map that contains data from a table rows
// Count 0 == all
func (m *Model) GetTableRows(rows *sql.Rows, rowsCount int) ([]map[string]interface{}, error) {
	if rowsCount < 0 {
		return nil, errors.New("the number of rows cannot be less than 0")
	}
	colNames, _ := rows.Columns()
	lenColumns := len(colNames)
	tempLine := make([]interface{}, lenColumns)
	mRows := make([]map[string]interface{}, 0)

	numRow := 0

	for rows.Next() {
		tempLineByte := make([][]byte, lenColumns)

		// Filling the slice with pointers to the storage location of row data
		pTempLineByte := make([]interface{}, lenColumns)
		for i := 0; i < lenColumns; i++ {
			pTempLineByte[i] = &tempLineByte[i]
		}

		// Filling pointers with data from a table
		err := rows.Scan(pTempLineByte...)
		if err != nil {
			panic(err)
		}

		// Assigning row data to maps
		rowMap := make(map[string]interface{}, 1)
		for i := 0; i < lenColumns; i++ {
			json.Unmarshal(tempLineByte[i], &tempLine[i])
			if tempLine[i] == nil {
				rowMap[colNames[i]] = string(tempLineByte[i])
			} else {
				rowMap[colNames[i]] = tempLine[i]
			}
		}
		mRows = append(mRows, rowMap)
		numRow++
		if rowsCount != 0 && numRow >= rowsCount {
			return mRows, nil
		}
	}
	return mRows, nil
}
