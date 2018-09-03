package com.presisco.lazyjdbc.client

import com.presisco.lazyjdbc.convertion.SimpleSql2JavaConvertion
import com.presisco.lazyjdbc.convertion.SqlTypedJava2SqlConvertion
import java.sql.SQLException
import java.sql.Statement
import javax.sql.DataSource

class MapJdbcClient(
        dataSource: DataSource,
        queryTimeoutSecs: Int = 2,
        rollbackOnBatchFailure: Boolean = true
) : BaseJdbcClient<Map<String, Any?>>(
        dataSource,
        queryTimeoutSecs,
        rollbackOnBatchFailure
) {
    private val sql2Java = SimpleSql2JavaConvertion()
    private val sqlTypeCache = HashMap<String, HashMap<String, Int>>()

    fun getColumnTypeMap(tableName: String): HashMap<String, Int> {
        if (sqlTypeCache.containsKey(tableName)) {
            return sqlTypeCache[tableName]!!
        }

        val columnTypeMap = HashMap<String, Int>()
        sqlTypeCache[tableName] = columnTypeMap

        val connection = getConnection()
        val statement = connection.createStatement()
        try {
            statement.fetchSize = 1
            val resultSet = statement.executeQuery("select * from  $wrapper$tableName$wrapper")
            with(resultSet.metaData) {
                for (i in 1..columnCount) {
                    columnTypeMap[getColumnName(i)] = getColumnType(i)
                }
            }
        } catch (e: SQLException) {
            throw RuntimeException("failed to read column types", e)
        } finally {
            statement.close()
            closeConnection(connection)
        }
        return columnTypeMap
    }

    fun getColumnList(mapList: List<Map<String, Any?>>) = mapList[0].keys.toList()

    override fun select(sql: String): List<Map<String, Any?>> {
        val resultList = ArrayList<Map<String, Any?>>()
        val connection = getConnection()
        val statement = connection.createStatement()
        try {
            val resultSet = statement.executeQuery(sql)
            val metadata = resultSet.metaData
            val columnNameArray = Array(metadata.columnCount, { "" })
            with(resultSet.metaData) {
                for (i in 1..columnCount) {
                    columnNameArray[i - 1] = metadata.getColumnName(i)
                }
            }
            while (resultSet.next()) {
                val map = HashMap<String, Any?>()
                val row = sql2Java.toArray(resultSet)
                columnNameArray.forEachIndexed { index, name -> map[name] = row[index] }

                resultList.add(map)
            }
        } catch (e: SQLException) {
            throw RuntimeException("failed to execute sql: $sql", e)
        } finally {
            statement.close()
            closeConnection(connection)
        }
        return resultList
    }

    fun checkMissedColumns(columnList: List<String>, columnTypeMap: Map<String, Int>): Set<String> {
        val missedSet = HashSet<String>()
        columnList.forEach { column -> if (!columnTypeMap.containsKey(column)) missedSet.add(column) }
        return missedSet
    }

    fun executeBatch(tableName: String, sql: String, dataList: List<Map<String, Any?>>, columnList: List<String>, columnTypeMap: Map<String, Int>): Set<Int> {
        val failedSet = HashSet<Int>()

        val sqlTypeArray = Array(columnList.size, { 0 })

        val missedSet = checkMissedColumns(columnList, columnTypeMap)
        if (missedSet.isNotEmpty()) {
            throw IllegalStateException("column type map mismatch for $tableName, missed $missedSet")
        }

        columnList.forEachIndexed { index, column ->
            sqlTypeArray[index] = columnTypeMap[column]!!
        }

        val connection = getConnection()
        val statement = connection.prepareStatement(sql)
        val buffer = Array<Any?>(columnList.size, { null })
        val java2sql = SqlTypedJava2SqlConvertion(sqlTypeArray)

        try {
            dataList.forEach { map ->

                columnList.forEachIndexed { index, column -> buffer[index] = map[column] }
                java2sql.bindArray(buffer, statement)

                statement.addBatch()
            }

            val resultArray = statement.executeBatch()
            if (resultArray.contains(Statement.EXECUTE_FAILED)) {
                if (rollbackOnBatchFailure) {
                    connection.rollback()
                }
                resultArray.forEachIndexed { i, result -> if (result == Statement.EXECUTE_FAILED) failedSet.add(i) }
            }
            connection.commit()
        } catch (e: SQLException) {
            throw RuntimeException("failed to insert maps", e)
        } finally {
            statement.close()
            closeConnection(connection)
        }
        return failedSet
    }

    override fun insert(tableName: String, dataList: List<Map<String, Any?>>) = if (dataList.isEmpty()) {
        setOf()
    } else {
        val columnList = dataList[0].keys.toList()
        executeBatch(tableName, buildInsertSql(tableName, columnList), dataList, columnList, getColumnTypeMap(tableName))
    }

    override fun replace(tableName: String, dataList: List<Map<String, Any?>>) = if (dataList.isEmpty()) {
        setOf()
    } else {
        val columnList = dataList[0].keys.toList()
        executeBatch(tableName, buildReplaceSql(tableName, columnList), dataList, columnList, getColumnTypeMap(tableName))
    }

    override fun delete(sql: String) {
        executeSQL(sql)
    }

}