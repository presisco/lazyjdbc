package sql.sqlbuilder

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.junit.Test
import sql.LazyJdbcClientTest
import java.util.*
import kotlin.test.expect

class UpdateBuilderTest : LazyJdbcClientTest() {
    private val client = MapJdbcClient(getDataSource())

    @Test
    fun update() {
        val date = Date(System.currentTimeMillis())
        val builder = client.update("names")
                .set("age" to 18, "time" to date)
                .where("name", "=", "james")
        expect("update \"names\"\n" +
                "set \"age\" = ?,\n" +
                "\"time\" = ?\n" +
                "where \"name\" = ?") { builder.toSQL() }
        expect(listOf<Any?>(18, date, "james")) { builder.params }
    }

}