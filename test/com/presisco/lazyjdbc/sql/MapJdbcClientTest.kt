package sql

import Definition
import com.presisco.lazyjdbc.client.MapJdbcClient
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.*
import kotlin.test.expect

class MapJdbcClientTest : LazyJdbcClientTest() {
    private lateinit var client: MapJdbcClient

    @Before
    fun prepare() {
        client = MapJdbcClient(getDataSource(), 1, true)
        client.executeSQL("CREATE TABLE TEST (\n" +
                "  A NUMBER(8,3) ,\n" +
                "  B VARCHAR2(255) ,\n" +
                "  C TIMESTAMP \n" +
                ")")
    }

    @Test
    fun insertOracle() {
        println("current time: ${Definition.currentTimeString()}")
        val ms = System.currentTimeMillis()
        val date = Date(ms)
        val timeString = Definition.defaultDateFormat.format(date)
        client.insert("TEST", listOf(
                mapOf("A" to 888.888, "B" to "message", "C" to timeString),
                mapOf("A" to 888.888, "B" to "message", "C" to date),
                mapOf("A" to 888.888, "B" to "message", "C" to ms)
        ))
        expect(listOf(
                mapOf("A" to 888.888, "B" to "message", "C" to date),
                mapOf("A" to 888.888, "B" to "message", "C" to date),
                mapOf("A" to 888.888, "B" to "message", "C" to date)
        )) { client.select("SELECT * FROM TEST") }
    }

    @After
    fun cleanup() {
        client.executeSQL("drop table TEST")
    }

}