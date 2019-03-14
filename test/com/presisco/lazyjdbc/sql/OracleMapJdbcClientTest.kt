package sql

import com.presisco.lazyjdbc.client.OracleMapJdbcClient
import org.junit.Before
import org.junit.Test
import kotlin.test.expect

class OracleMapJdbcClientTest : LazyJdbcClientTest(
        "dataSourceClassName" to "oracle.jdbc.pool.OracleDataSource",
        "dataSource.url" to "jdbc:oracle:thin:@//192.168.1.201:1521/XE",
        "dataSource.user" to "SAMPLE",
        "dataSource.password" to "sample",
        "maximumPoolSize" to "1"
) {
    private lateinit var client: OracleMapJdbcClient

    @Before
    fun prepare() {
        client = OracleMapJdbcClient(getDataSource())
    }

    @Test
    fun querySequence() {
        client.executeSQL("CREATE SEQUENCE TEST MINVALUE 1 MAXVALUE 9 INCREMENT BY 1 START WITH 1 ORDER NOCACHE")
        expect(listOf(1L, 2L, 3L, 4L, 5L)) { client.querySequence("TEST", 5) }
        client.executeSQL("DROP SEQUENCE TEST")
    }

}