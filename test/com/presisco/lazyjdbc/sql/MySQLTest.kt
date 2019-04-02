package sql

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.junit.Before
import org.junit.Test
import java.util.*
import kotlin.test.expect

class MySQLTest : LazyMySQLTestClient() {
    private lateinit var client: MapJdbcClient

    @Before
    fun prepare() {
        client = MapJdbcClient(getDataSource())
        client.executeSQL("CREATE TABLE times  (\n" +
                "  time timestamp(3) NULL\n" +
                ");")
    }

    @Test
    fun querySequence() {
        val timeString = client.dateFormat.format(Date(System.currentTimeMillis()))
        println("created: $timeString")
        client.insert("times", mapOf("time" to timeString))
        val selected = client.buildSelect("time")
                .from("times")
                .execute()
                .map { it["time"] }
                .first()
        println("selected: $selected")
        expect(timeString) { client.dateFormat.format(selected) }
    }

    //@After
    fun cleanup() {
        client.executeSQL("drop table times")
    }

}