package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.LazyMySQLTestClient
import com.presisco.lazyjdbc.client.MapJdbcClient
import org.junit.Test
import java.util.*
import kotlin.test.expect

class InsertBuilderMySQLTest : LazyMySQLTestClient() {
    private val client = MapJdbcClient(getDataSource())

    @Test
    fun blindInsert() {
        val date = Date(System.currentTimeMillis())
        val builder = client.insertInto("names")
                .values("james", 18, date)
                .values("bob", 16, date)
        expect("insert into `names`\n" +
                "values (?, ?, ?), (?, ?, ?)") { builder.toSQL() }
        expect(listOf<Any?>("james", 18, date, "bob", 16, date)) { builder.params }
    }

    @Test
    fun fieldInsert() {
        val date = Date(System.currentTimeMillis())
        val builder = client.insertInto("names")
                .columns("name", "age", "time")
                .values("james", 18, date)
                .values("bob", 16, date)
        expect("insert into `names`\n" +
                "(`name`, `age`, `time`)\n" +
                "values (?, ?, ?), (?, ?, ?)") { builder.toSQL() }
        expect(listOf<Any?>("james", 18, date, "bob", 16, date)) { builder.params }
    }

    @Test
    fun insertSelect() {
        val builder = client.insertInto("names")
                .columns("name", "age", "time")
                .select(client.buildSelect("name", "age", "time")
                        .from("names")
                        .where("names", "=", "james"))
        expect("insert into `names`\n" +
                "(`name`, `age`, `time`)\n" +
                "select `name`, `age`, `time`\n" +
                "from `names`\n" +
                "where `names` = ?") { builder.toSQL() }
        expect(listOf<Any?>("james")) { builder.params }

    }
}