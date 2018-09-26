package com.presisco.lazyjdbc.client

import java.util.*
import javax.sql.DataSource

open class OracleMapJdbcClient(
        dataSource: DataSource,
        queryTimeoutSecs: Int = 2,
        rollbackOnBatchFailure: Boolean = true
) : MapJdbcClient(
        dataSource,
        queryTimeoutSecs,
        rollbackOnBatchFailure
) {
    fun querySequence(name: String, count: Int): List<Long> {
        val connection = getConnection()
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("select \"$name\".nextval from dual connect by level <= $count")
        val idList = ArrayList<Long>(count)
        while (resultSet.next()) {
            idList.add(resultSet.getLong(1))
        }
        resultSet.close()
        statement.close()
        closeConnection(connection)
        return idList
    }
}