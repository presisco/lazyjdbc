package sql

import com.presisco.lazyjdbc.client.LazySelectJdbcClient
import com.presisco.lazyjdbc.client.condition
import com.presisco.lazyjdbc.client.table
import org.junit.Before
import org.junit.Test
import kotlin.test.expect

class LazySelectJdbcClientTest : LazyJdbcClientTest(
        "dataSourceClassName" to "oracle.jdbc.pool.OracleDataSource",
        "dataSource.url" to "jdbc:oracle:thin:@//192.168.1.201:1521/XE",
        "dataSource.user" to "SAMPLE",
        "dataSource.password" to "sample",
        "maximumPoolSize" to "1"
) {
    private lateinit var client: LazySelectJdbcClient

    @Before
    fun prepare() {
        client = LazySelectJdbcClient(getDataSource())
    }

    @Test
    fun querySequence() {
        expect("select \"a\", \"b\", \"c\"\n" +
                "from \"log_table\" as \"log\"\n" +
                "inner join \"alarm_table\" as \"alarm\" on \"alarm\".\"sid\" = \"log\".\"sid\"\n" +
                "inner join \"map_table\" as \"map\" on \"map\".\"sid\" = \"log\".\"sid\"\n" +
                "where ((\"id\" < 14) and (\"age\" like 'old')) or (\"gender\" in ( 'male', 'female' ))\n" +
                "group by \"id\", \"age\", \"gender\"") {
            client.select("a", "b", "c")
                    .from(table("log_table", "log")
                            .innerJoin("map_table", "map").on("map.sid", "log.sid")
                            .innerJoin("alarm_table", "alarm").on("alarm.sid", "log.sid")
                    ).where(condition(
                            condition("id", "<", 14)
                                    .and("age", "like", "old")
                                    .or("weight", ">", null)
                    ).or("gender", "in", listOf("male", "female"))
                    ).groupBy("id", "age", "gender")
                    .sql()
        }
    }

}