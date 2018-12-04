package sql

import com.presisco.lazyjdbc.client.OracleMapJdbcClient
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.Test
import java.util.*

class OracleMapJdbcClientTest {
    private val oracleDataSource = mapOf(
            "dataSourceClassName" to "oracle.jdbc.pool.OracleDataSource",
            "dataSource.url" to "jdbc:oracle:thin:@//192.168.1.201:1521/XE",
            "dataSource.user" to "SAMPLE",
            "dataSource.password" to "sample",
            "maximumPoolSize" to "2"
    )

    @Test
    fun querySequence() {
        val props = Properties()
        props.putAll(oracleDataSource)
        val hikari = HikariDataSource(HikariConfig(props))

        val client = OracleMapJdbcClient(hikari, 5, true)
        val ids = client.querySequence("run_id", 5)
        ids.forEach { println(it) }
    }
}