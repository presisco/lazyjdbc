package com.presisco.lazyjdbc.sql

import org.junit.Test

class SqlHelperTest {

    @Test
    fun validate() {
        val tableName = "example"
        val colums = arrayOf("name", "class", "score")
        val rows = arrayListOf(
                arrayOf("james", "1", 100),
                arrayOf("bob", "2", 20),
                arrayOf("tom", null, 80)
        )
        println(SqlHelper.buildBatchInsertString(tableName, colums, rows))
    }

}