package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.LazyOracleTestClient
import com.presisco.lazyjdbc.client.MapJdbcClient
import org.junit.Test
import kotlin.test.expect

class DeleteBuilderTest : LazyOracleTestClient() {
    private val client = MapJdbcClient(getDataSource())

    @Test
    fun delete() {
        val builder = client.deleteFrom("names")
                .where("name", "=", "james")
        expect("delete from \"names\"\n" +
                "where \"name\" = ?") { builder.toSQL() }
        expect(listOf<Any?>("james")) { builder.params }
    }

}