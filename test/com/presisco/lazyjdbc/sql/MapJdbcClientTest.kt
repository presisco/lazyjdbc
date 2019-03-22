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
        client = MapJdbcClient(getDataSource(), 5, true)
        client.executeSQL("CREATE TABLE TEST (\n" +
                "  A NUMBER(8,3) ,\n" +
                "  B VARCHAR2(255) ,\n" +
                "  C TIMESTAMP \n" +
                ")")
    }

    @Test
    fun batchInsertOracle() {
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
        )) { client.buildSelect("*").from("TEST").execute() }
    }

    @Test
    fun insertSelectUpdateDeleteOracle() {
        val ms = System.currentTimeMillis()
        val date = Date(ms)
        client.insertInto("TEST")
                .values(666, "a", date)
                .values(777, "e", date)
                .values(888, "g", date)
                .execute()
        expect(listOf(
                mapOf("A" to 666.0, "B" to "a")
        )) {
            client.buildSelect("A", "B")
                    .from("TEST")
                    .where("B", "=", "a")
                    .execute()
        }
        client.update("TEST")
                .set("B" to "b")
                .where("B", "=", "a")
                .execute()
        expect(listOf(
                mapOf("A" to 666.0, "B" to "b")
        )) {
            client.buildSelect("A", "B")
                    .from("TEST")
                    .where("B", "=", "b")
                    .execute()
        }
        client.deleteFrom("TEST")
                .where("B", "=", "b")
                .execute()
        expect(listOf(
                mapOf("A" to 777.0, "B" to "e"),
                mapOf("A" to 888.0, "B" to "g")
        )) {
            client.buildSelect("A", "B")
                    .from("TEST")
                    .execute()
        }
    }

    @After
    fun cleanup() {
        client.executeSQL("drop table TEST")
    }

}