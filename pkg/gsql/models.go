package gsql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"gsql/internal/errors"
	"reflect"
	"strconv"
)

type Model struct {
	TableName string
	Connector *sql.DB
}

// All Displays all fields of the selected table
func (m *Model) All(dest interface{}) error {
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
			return err
		} else {
			defer rows.Close()

			currValueSlice := m.setDestValue(destType, rows, 1)
			destVal.Set(currValueSlice[0])
		}
	default:
		return errors.New("Only data types supported are: interface(struct), []interface(struct)")
	}
	return nil
}

// Get Returns only one specific value from a table.
// The map parameter is intended to identify the field for output. For example "id" = "1".
func (m *Model) Get(dest interface{}, where map[string]string) error {
	var key string
	var val string
	for ikey, ival := range where {
		key = ikey
		val = ival
		break
	}

	switch reflect.TypeOf(dest).Elem().Kind() {
	case reflect.Struct:
		if rows, err := m.Connector.Query(fmt.Sprintf("SELECT * FROM %s WHERE `%s`=\"%s\"", m.TableName, key, val)); err != nil {
			defer rows.Close()
			return err
		} else {
			defer rows.Close()
			dVal := reflect.ValueOf(dest)
			destVal := dVal.Elem()
			destType := reflect.TypeOf(dest).Elem()

			currValueSlice := m.setDestValue(destType, rows, 1)
			destVal.Set(currValueSlice[0])
		}
	default:
		return errors.New("Only data types supported are: interface(struct)")
	}
	return nil
}

// Filter Display a slice from data that matches the filter values.
// There can only be one filter value.
func (m *Model) Filter(dest interface{}, where map[string]string) error {
	var key string
	var val string
	for ikey, ival := range where {
		key = ikey
		val = ival
		break
	}

	switch reflect.TypeOf(dest).Elem().Kind() {
	case reflect.Slice:
		destType := reflect.TypeOf(dest).Elem().Elem()
		directValue := reflect.ValueOf(dest)
		direct := reflect.Indirect(directValue)

		if rows, err := m.Connector.Query(fmt.Sprintf("SELECT * FROM %s WHERE `%s`=\"%s\"", m.TableName, key, val)); err != nil {
			defer rows.Close()
			return err
		} else {
			defer rows.Close()

			currValueSlice := m.setDestValue(destType, rows, 0)

			fmt.Println(fmt.Sprintf("SELECT * FROM %s WHERE %s=%s", m.TableName, key, val))

			for i := 0; i < len(currValueSlice); i++ {
				direct.Set(reflect.Append(direct, currValueSlice[i]))
			}
		}
	default:
		return errors.New("Only data types supported are: []interface(struct)")
	}
	return nil
}

// Insert Inserts data into the selected table.
// Can be used as an argument structure or slice of structures.
func (m *Model) Insert(insertStruct interface{}, noInsertFields []string) error {
	switch reflect.TypeOf(insertStruct).Kind() {
	case reflect.Struct:
		if err := m.checkColumnMatches(insertStruct); err != nil {
			panic(err)
		}
		insertData, err := m.GetStructFormatData(insertStruct, noInsertFields)
		if err != nil {
			panic(err)
		}
		iKey, iVal := m.separateKeysAndValuesForInsert(insertData)
		_, err = m.Connector.Query(fmt.Sprintf("INSERT `%s` %s VALUES %s", m.TableName, iKey, iVal))
		if err != nil {
			return err
		}
	case reflect.Slice:
		valueInsertStruct := reflect.ValueOf(insertStruct)
		lenInsertStruct := valueInsertStruct.Len()
		insertDataSlice := make([]map[string]string, 0)

		if err := m.checkColumnMatches(reflect.ValueOf(insertStruct).Index(0).Interface()); err != nil {
			panic(err)
		}

		for i := 0; i < lenInsertStruct; i++ {
			insertData, err := m.GetStructFormatData(reflect.ValueOf(insertStruct).Index(i).Interface(), noInsertFields)
			if err != nil {
				panic(err)
			}
			insertDataSlice = append(insertDataSlice, insertData)
		}

		for _, iData := range insertDataSlice {
			iKey, iVal := m.separateKeysAndValuesForInsert(iData)
			_, err := m.Connector.Query(fmt.Sprintf("INSERT `%s` %s VALUES %s", m.TableName, iKey, iVal))
			if err != nil {
				return err
			}
		}

	}
	return nil
}

// Update Updates data in a table
// It is possible to update one or more rows if you pass a slice as an argument
func (m *Model) Update(updateStruct interface{}, where map[string]string, noUpdateColumns []string) error {
	var key string
	var val string
	for iKey, iVal := range where {
		key = iKey
		val = iVal
		break
	}

	switch reflect.TypeOf(updateStruct).Kind() {
	case reflect.Struct:
		err := m.checkColumnMatches(updateStruct)
		if err != nil {
			panic(err)
		}
		mapStructData, err := m.GetStructFormatData(updateStruct, noUpdateColumns)
		if err != nil {
			panic(err)
		}
		updateString := m.separateKeysAndValuesForUpdate(mapStructData)
		_, err = m.Connector.Query(fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = '%s'",
			m.TableName, updateString, key, val))
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete Deleting a table field
func (m *Model) Delete(where map[string]string) error {
	var key string
	var val string
	for iKey, iVal := range where {
		key = iKey
		val = iVal
		break
	}
	_, err := m.Connector.Query(fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = '%s'", m.TableName, key, val))
	if err != nil {
		return err
	}
	return nil
}

// Checks if structure fields and table columns match
func (m *Model) checkColumnMatches(someStruct interface{}) error {
	query, err := m.Connector.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1;", m.TableName))
	defer query.Close()
	if err != nil {
		panic(err)
	}
	tableColumns, err := query.Columns()

	structFields := make([]string, 0)
	for i := 0; i < reflect.TypeOf(someStruct).NumField(); i++ {
		structFields = append(structFields, reflect.TypeOf(someStruct).Field(i).Tag.Get("db"))
	}
	if err != nil {
		panic(err)
	}
	if len(structFields) != len(tableColumns) {
		return errors.New("number of table columns and structure fields do not match")
	}
	for _, sVal := range structFields {
		fieldNotFound := true
		for _, tVal := range tableColumns {
			if sVal == tVal {
				fieldNotFound = false
			}
		}
		if fieldNotFound {
			return errors.New(fmt.Sprintf("structure field '%s' not found in table '%s'", sVal, m.TableName))
		}
	}
	return nil
}

// GetStructFormatData Returns data to be inserted into a table.
// Can only be used as an argument one structure.
func (m *Model) GetStructFormatData(insertStruct interface{}, skipFields []string) (map[string]string, error) {
	insertData := make(map[string]string, 0)
	insertStructType := reflect.TypeOf(insertStruct)
	insertStructValue := reflect.ValueOf(insertStruct)

	if reflect.TypeOf(insertStruct).Kind() != reflect.Struct && reflect.TypeOf(insertStruct).String() != "reflect.Value" {
		return nil, errors.New("insertStruct only accepts a struct")
	}

	for i := 0; i < insertStructType.NumField(); i++ {
		fName := insertStructType.Field(i).Tag.Get("db")

		// check skip field
		isSkipField := false
		for _, skipField := range skipFields {
			if fName == skipField {
				isSkipField = true
			}
		}
		if isSkipField {
			continue
		}

		switch insertStructValue.Field(i).Type().Kind() {
		case reflect.String:
			insertData[fName] = insertStructValue.Field(i).Interface().(string)
		case reflect.Int:
			insertData[fName] = strconv.Itoa(insertStructValue.Field(i).Interface().(int))
		case reflect.Float64:
			insertData[fName] = strconv.FormatFloat(insertStructValue.Field(i).Interface().(float64),
				'E', -1, 64)
		}
	}
	return insertData, nil
}

func (m *Model) separateKeysAndValuesForUpdate(sMap map[string]string) string {
	var updateValues string

	sMapLen := len(sMap)
	var i int
	for key, _ := range sMap {
		i++
		if i == sMapLen {
			updateValues += fmt.Sprintf("`%s` = '%s'", key, sMap[key])
		} else {
			updateValues += fmt.Sprintf("`%s` = '%s', ", key, sMap[key])
		}
	}
	return updateValues
}

// Divides the map with data to be inserted into keys (salts) and values.
// Returns already formatted strings.
func (m *Model) separateKeysAndValuesForInsert(sMap map[string]string) (string, string) {
	insertValues := "("
	insertKeys := "("

	sMapLen := len(sMap)
	var i int
	for key, _ := range sMap {
		i++
		if i == sMapLen {
			insertKeys += "`" + key + "`" + ")"
			insertValues += "'" + sMap[key] + "'" + ")"
		} else {
			insertKeys += "`" + key + "`" + ", "
			insertValues += "'" + sMap[key] + "'" + ", "
		}
	}
	return insertKeys, insertValues
}

// Initialization of destination data (structures) considering all data types.
// Only those fields that match in the map and structure are filled.
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

// GetTableRows Returns a slice from a map that contains data from a table rows.
// Count 0 == all.
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
