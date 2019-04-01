package com.presisco.lazyjdbc.client

import Definition
import com.presisco.lazyjdbc.convertion.SimpleJava2SqlConversion
import com.presisco.lazyjdbc.convertion.SimpleSql2JavaConversion
import com.presisco.lazyjdbc.convertion.SqlTypedJava2SqlConversion
import sqlbuilder.DeleteBuilder
import sqlbuilder.InsertBuilder
import sqlbuilder.SelectBuilder
import sqlbuilder.UpdateBuilder
import java.sql.SQLException
import java.sql.Statement
import java.text.SimpleDateFormat
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
    private val sql2Java = SimpleSql2JavaConversion()
    private val sqlTypeCache = HashMap<String, HashMap<String, Int>>()
    var dateFormat = Definition.defaultDateFormat

    fun buildSelect(vararg columns: String) = SelectBuilder(this).select(*columns)

    fun withDateFormat(format: SimpleDateFormat): MapJdbcClient {
        dateFormat = format
        return this
    }

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

    override fun select(sql: String, vararg params: Any): List<Map<String, Any?>> {
        val resultList = ArrayList<Map<String, Any?>>()
        val connection = getConnection()
        val statement = connection.prepareStatement(sql)
        try {
            SimpleJava2SqlConversion().bindList(params.toList(), statement)
            val resultSet = statement.executeQuery()

            val metadata = resultSet.metaData
            val columnNameArray = Array(metadata.columnCount, { "" })
            with(resultSet.metaData) {
                for (i in 1..columnCount) {
                    columnNameArray[i - 1] = metadata.getColumnName(i)
                }
            }
            while (resultSet.next()) {
                val map = HashMap<String, Any?>()
                val row = sql2Java.toList(resultSet)
                columnNameArray.forEachIndexed { index, name -> map[name] = row[index] }

                resultList.add(map)
            }
        } catch (e: Exception) {
            throw e
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
        val sortedDataRow = ArrayList<Any?>(columnList.size)
        val java2sql = SqlTypedJava2SqlConversion(sqlTypeList, dateFormat)

        try {
            dataList.forEach { map ->
                val columnMismatchSet = map.keys.minus(columnList)
                if (columnMismatchSet.isNotEmpty()) {
                    throw IllegalStateException("column type map mismatch for $tableName, missed $columnMismatchSet")
                }

                for (column in columnList) {
                    sortedDataRow.add(map[column])
                }

                java2sql.bindList(sortedDataRow, statement)
                statement.addBatch()

                sortedDataRow.clear()
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
            throw SQLException("failed to insert maps on table: $tableName", e)
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

    fun insert(tableName: String, vararg fields: Pair<String, Any?>) = insert(tableName, listOf(mapOf(*fields))).first()

    fun insert(tableName: String, data: Map<String, Any?>) = insert(tableName, listOf(data)).first()

    override fun replace(tableName: String, dataList: List<Map<String, Any?>>) = if (dataList.isEmpty()) {
        setOf()
    } else {
        val typeMap = getColumnTypeMap(tableName)
        executeBatch(tableName, buildReplaceSql(tableName, typeMap.keys), dataList, typeMap)
    }

    override fun delete(sql: String, vararg params: Any) = executeSQL(sql, *params)

    override fun update(sql: String, vararg params: Any) = executeSQL(sql, *params)

    fun deleteFrom(table: String) = DeleteBuilder(table, this)

    fun update(table: String) = UpdateBuilder(table, this)

    fun insertInto(table: String) = InsertBuilder(table, this)

}