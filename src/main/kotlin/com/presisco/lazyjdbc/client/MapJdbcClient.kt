package com.presisco.lazyjdbc.client

import com.presisco.lazyjdbc.convertion.SimpleSql2JavaConvertion
import com.presisco.lazyjdbc.convertion.SqlTypedJava2SqlConvertion
import javax.sql.DataSource

class MapJdbcClient(
        dataSource: DataSource,
        queryTimeoutSecs: Int = 2,
        rollbackOnBatchFailure: Boolean = true
) : BaseJdbcClient(
        dataSource,
        queryTimeoutSecs,
        rollbackOnBatchFailure
) {
    private val sql2Java = SimpleSql2JavaConvertion()
    private val sqlTypeCache = HashMap<String, HashMap<String, Int>>()

    fun getColumnTypeMap(tableName: String): HashMap<String, Int>{
        if(sqlTypeCache.containsKey(tableName)){
            return sqlTypeCache[tableName]!!
        }

        val columnTypeMap = HashMap<String, Int>()
        sqlTypeCache[tableName] = columnTypeMap
        val connection = getConnection()
        val statement = connection.createStatement()
        statement.fetchSize = 1
        val resultSet = statement.executeQuery("select * from  $wrapper$tableName$wrapper")
        with(resultSet.metaData) {
            for (i in 1..columnCount) {
                columnTypeMap[getColumnName(i)] = getColumnType(i)
            }
        }
        statement.close()
        closeConnection(connection)
        return columnTypeMap
    }

    fun insert(tableName: String, mapList: List<Map<String, Any?>>){
        if(mapList.isEmpty())
            return
        val columnTypeMap = getColumnTypeMap(tableName)

        val columnList = mapList[0].keys.toList()
        val sqlTypeArray = Array(columnList.size, { 0 })
        columnList.forEachIndexed { index , column -> sqlTypeArray[index] = columnTypeMap[column]!! }

        val connection = getConnection()
        val statement = connection.prepareStatement(buildInsertSql(tableName, columnList))
        val buffer = Array<Any?>(columnList.size,{ null })
        val java2sql = SqlTypedJava2SqlConvertion(sqlTypeArray)

        mapList.forEach{
            map ->

            columnList.forEachIndexed{ index, column -> buffer[index] = map[column] }
            java2sql.bindArray(buffer, statement)

            statement.addBatch()
        }

        statement.executeBatch()
        connection.commit()
        statement.close()
        connection.close()
    }

    fun select(sql: String): List<Map<String, Any?>>{
        val connection = getConnection()
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(sql)
        val resultList = ArrayList<Map<String, Any?>>()
        val metadata = resultSet.metaData
        val columnNameArray = Array(metadata.columnCount, { "" })
        with(resultSet.metaData){
            for(i in 1..columnCount){
                columnNameArray[i - 1] = metadata.getColumnName(i)
            }
        }
        while(resultSet.next()){
            val map = HashMap<String, Any?>()
            val row = sql2Java.toArray(resultSet)
            columnNameArray.forEachIndexed { index, name -> map[name] = row[index] }

            resultList.add(map)
        }
        resultSet.close()
        statement.close()
        connection.close()
        return resultList
    }

}