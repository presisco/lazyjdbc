package sql

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.sql.Time
import java.sql.Timestamp
import kotlin.test.expect

class MapJdbcClientTest : LazyJdbcClientTest(
        "dataSourceClassName" to "oracle.jdbc.pool.OracleDataSource",
        "dataSource.url" to "jdbc:oracle:thin:@//192.168.1.201:1521/XE",
        "dataSource.user" to "SAMPLE",
        "dataSource.password" to "sample",
        "maximumPoolSize" to "1"
) {
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
        val timeString = "2019-03-01 23:45:06.789"
        val date = client.dateFormat.parse(timeString)
        val ms = date.time
        val timeStamp = Timestamp(ms)
        val time = Time(ms)
        val sqlDate = java.sql.Date(ms)
        client.insert("TEST", listOf(
                mapOf("A" to 888.888, "B" to "message", "C" to timeString),
                mapOf("A" to 888.888, "B" to "message", "C" to date),
                mapOf("A" to 888.888, "B" to "message", "C" to ms)
        ))
        expect(listOf(
                mapOf("A" to 888.888, "B" to "message", "C" to timeStamp),
                mapOf("A" to 888.888, "B" to "message", "C" to timeStamp),
                mapOf("A" to 888.888, "B" to "message", "C" to timeStamp)
        )) { client.select("SELECT * FROM TEST") }
    }

    @After
    fun cleanup() {
        client.executeSQL("drop table TEST")
    }

}