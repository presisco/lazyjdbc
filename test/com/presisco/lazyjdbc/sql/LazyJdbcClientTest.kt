package sql

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import javax.sql.DataSource

abstract class LazyJdbcClientTest(
        vararg config: Pair<String, String>
) {
    private val configMap = mapOf(*config)

    constructor() : this(
            "dataSourceClassName" to "oracle.jdbc.pool.OracleDataSource",
            "dataSource.url" to "jdbc:oracle:thin:@//192.168.1.201:1521/XE",
            "dataSource.user" to "SAMPLE",
            "dataSource.password" to "sample",
            "maximumPoolSize" to "1"
    )

    fun getDataSource(): DataSource {
        val props = Properties()
        props.putAll(configMap)
        return HikariDataSource(HikariConfig(props))
    }

}