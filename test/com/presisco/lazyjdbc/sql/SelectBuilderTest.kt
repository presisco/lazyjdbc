package sql

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazyjdbc.client.condition
import com.presisco.lazyjdbc.client.table
import org.junit.Before
import org.junit.Test
import kotlin.test.expect

class SelectBuilderTest : LazyJdbcClientTest(
        "dataSourceClassName" to "oracle.jdbc.pool.OracleDataSource",
        "dataSource.url" to "jdbc:oracle:thin:@//192.168.1.201:1521/XE",
        "dataSource.user" to "SAMPLE",
        "dataSource.password" to "sample",
        "maximumPoolSize" to "1"
) {
    private lateinit var client: MapJdbcClient

    @Before
    fun prepare() {
        client = MapJdbcClient(getDataSource())
    }

    @Test
    fun querySequence() {
        val builder = client.buildSelect("a", "b", "c")
                .from(table("log_table", "log")
                        .innerJoin("map_table", "map").on("map.sid", "log.sid")
                        .fullJoin(
                                client.buildSelect("id")
                                        .from(table("alarm_table"))
                                        .where(condition("no", "like", "stop")), "j").on("j.id", "log.sid")
                ).where(condition(
                        condition("id", "<", 14)
                                .and("age", "like", "old")
                                .or("weight", ">", null)
                                .and("gender", "in", client.buildSelect("*").from(table("run_table")))
                ).or("gender", "in", listOf("male", "female"))
                ).groupBy("id", "age", "gender")

        expect("select \"a\", \"b\", \"c\"\n" +
                "from \"log_table\" as \"log\"\n" +
                "full join (select \"id\"\n" +
                "from \"alarm_table\"\n" +
                "where \"no\" like ?) as \"j\" on \"j\".\"id\" = \"log\".\"sid\"\n" +
                "inner join \"map_table\" as \"map\" on \"map\".\"sid\" = \"log\".\"sid\"\n" +
                "where (((\"id\" < ?)\n" +
                " and (\"age\" like ?))\n" +
                " and (\"gender\" in (\n" +
                "select *\n" +
                "from \"run_table\"\n" +
                ")))\n" +
                " or (\"gender\" in (?, ?))\n" +
                "group by \"id\", \"age\", \"gender\"") { builder.toSQL() }

        println(builder.params)
    }

}