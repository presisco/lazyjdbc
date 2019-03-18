package sqlbuilder

import com.presisco.lazyjdbc.client.MapJdbcClient
import java.util.*

abstract class Builder(
        val client: MapJdbcClient
) {
    val params = LinkedList<Any?>()

    fun Array<out String>.fieldJoin(separator: String = ", ") = this.joinToString(separator = separator, transform = client::wrap)

    abstract fun toSQL(): String

}