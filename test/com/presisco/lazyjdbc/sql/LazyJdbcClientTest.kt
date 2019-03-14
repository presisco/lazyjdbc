package sql

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import javax.sql.DataSource

abstract class LazyJdbcClientTest(
        vararg config: Pair<String, String>
) {
    private val configMap = mapOf(*config)

    fun getDataSource(): DataSource {
        val props = Properties()
        props.putAll(configMap)
        return HikariDataSource(HikariConfig(props))
    }

}