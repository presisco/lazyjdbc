package sql.sqlbuilder

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.junit.Test
import sql.LazyJdbcClientTest
import kotlin.test.expect

class DeleteBuilderTest : LazyJdbcClientTest() {
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