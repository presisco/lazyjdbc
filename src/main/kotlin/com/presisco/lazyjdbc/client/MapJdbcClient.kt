package com.presisco.lazyjdbc.client

import com.presisco.lazyjdbc.convertion.SimpleSql2JavaConvertion
import com.presisco.lazyjdbc.convertion.SqlTypedJava2SqlConversion
import java.sql.SQLException
import java.sql.Statement
import javax.sql.DataSource

open class MapJdbcClient(
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

    fun executeBatch(tableName: String, sql: String, dataList: List<Map<String, Any?>>, columnTypeMap: Map<String, Int>): Set<Int> {
        val failedSet = HashSet<Int>()

        val columnList = columnTypeMap.keys.toList()
        val sqlTypeList = columnTypeMap.values.toList()

        val connection = getConnection()
        val statement = connection.prepareStatement(sql)
        val sortedDataRow = arrayListOf<Any?>(columnList.size)
        val java2sql = SqlTypedJava2SqlConversion(sqlTypeList)

        try {
            dataList.forEach { map ->
                val columnMismatchSet = map.keys.minus(columnList)
                if (columnMismatchSet.isNotEmpty()) {
                    throw IllegalStateException("column type map mismatch for $tableName, missed $columnMismatchSet")
                }

                for (column in columnList) {
                    sortedDataRow.add(map[column])
                }

                java2sql.bindArray(sortedDataRow, statement)
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
            if (rollbackOnBatchFailure) {
                connection.rollback()
            }
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
        val typeMap = getColumnTypeMap(tableName)
        executeBatch(tableName, buildInsertSql(tableName, typeMap.keys), dataList, typeMap)
    }

    override fun replace(tableName: String, dataList: List<Map<String, Any?>>) = if (dataList.isEmpty()) {
        setOf()
    } else {
        val typeMap = getColumnTypeMap(tableName)
        executeBatch(tableName, buildReplaceSql(tableName, typeMap.keys), dataList, typeMap)
    }

    override fun delete(sql: String) {
        executeSQL(sql)
    }

}