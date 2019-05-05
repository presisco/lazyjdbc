package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.LazyOracleTestClient
import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazyjdbc.condition
import com.presisco.lazyjdbc.conditionNotNull
import com.presisco.lazyjdbc.table
import org.junit.Before
import org.junit.Test
import kotlin.test.expect

class SelectBuilderTest : LazyOracleTestClient() {
    private lateinit var client: MapJdbcClient

    @Before
    fun prepare() {
        client = MapJdbcClient(getDataSource())
    }

    @Test
    fun selectColumns() {
        expect("select \"name\", \"b\".\"c\" \"d\", \"e\"  \"f\"\n" +
                "from \"table\"") {
            client.buildSelect("name", "b.c d", "e  f")
                    .from(table("table"))
                    .toSQL()
        }
    }

    @Test
    fun fromTables() {
        expect("select *\n" +
                "from \"data_table\"\n" +
                ", \"map_table\"  \"map\"\n" +
                "inner join \"map_table\"  \"map\" on \"map\".\"sid\" = \"log\".\"sid\"") {
            client.buildSelect("*")
                    .from(
                            table("data_table")
                                    .table("map_table").rename("map")
                                    .innerJoin("map_table").rename("map").on("map.sid", "log.sid")
                    ).toSQL()
        }
    }

    @Test
    fun testConditionNotNull() {
        expect("select *\n" +
                "from \"data_table\"") {
            client.buildSelect("*")
                    .from(table("data_table"))
                    .where(conditionNotNull("age", ">", null))
                    .toSQL()
        }
    }

    @Test
    fun complicateMix() {
        val builder = client.buildSelect("a", "b", "c")
                .from(table("log_table").rename("log")
                        .innerJoin("map_table").rename("map").on("map.sid", "log.sid")
                        .fullJoin(
                                client.buildSelect("id")
                                        .from(table("alarm_table"))
                                        .where(condition("no", "like", "stop"))).rename("j").on("j.id", "log.sid")
                ).where(condition(
                        condition("id", "<", 14)
                                .and(
                                        condition("weight", ">", null)
                                                .and(listOf("gender", "age"), "in", client.buildSelect("gender", "age").from(table("run_table")))
                                )
                ).or(condition("gender", "in", listOf("male", "female")).andNotNull("height", ">", null))
                ).groupBy("id", "age", "gender")

        expect("select \"a\", \"b\", \"c\"\n" +
                "from \"log_table\"  \"log\"\n" +
                "inner join \"map_table\"  \"map\" on \"map\".\"sid\" = \"log\".\"sid\"\n" +
                "full join (select \"id\"\n" +
                "from \"alarm_table\"\n" +
                "where \"no\" like ?)  \"j\" on \"j\".\"id\" = \"log\".\"sid\"\n" +
                "where ((\"id\" < ?)\n" +
                " and ((\"weight\" > ?)\n" +
                " and ((\"gender\", \"age\") in (\n" +
                "select \"gender\", \"age\"\n" +
                "from \"run_table\"\n" +
                "))))\n" +
                " or (\"gender\" in (?, ?))\n" +
                "group by \"id\", \"age\", \"gender\"") { builder.toSQL() }

        expect(listOf("stop", 14, null, "male", "female")) { builder.params }
    }

}