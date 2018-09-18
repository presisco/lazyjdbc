package sql

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.Ignore
import java.util.*

class BaseJdbcClientTest {
    val oracleDataSource = mapOf(
            "dataSourceClassName" to "oracle.jdbc.pool.OracleDataSource",
            "dataSource.url" to "jdbc:oracle:thin:@//192.168.1.201:1521/XE",
            "dataSource.user" to "SAMPLE",
            "dataSource.password" to "sample",
            "maximumPoolSize" to "2"
    )

    @Ignore
    fun databaseIdentify() {
        val props = Properties()
        props.putAll(oracleDataSource)
        val hikari = HikariDataSource(HikariConfig(props))

        val client = MapJdbcClient(hikari, 5, true)
    }
}