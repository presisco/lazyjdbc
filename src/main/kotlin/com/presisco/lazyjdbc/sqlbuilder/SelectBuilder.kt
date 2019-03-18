package sqlbuilder

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazyjdbc.client.addNotEmpty
import com.presisco.lazyjdbc.sqlbuilder.Condition
import com.presisco.lazyjdbc.sqlbuilder.Table
import java.util.*

class SelectBuilder(
        val client: MapJdbcClient
) {

    private lateinit var columnText: String
    private lateinit var from: String
    private var where = ""
    private var groupBy = ""
    private var having = ""
    private var orderBy = ""
    val params = LinkedList<Any?>()

    fun Array<out String>.fieldJoin(wrap: (String) -> String, separator: String = ", ") = this.joinToString(separator = separator, transform = wrap)


    fun select(vararg columns: String): SelectBuilder {
        columnText = if (columns.isEmpty() || columns[0] == "*") {
            "*"
        } else {
            columns.joinToString(separator = ", ", transform = { it ->
                with(client) {
                    val words = it.split("\\s".toRegex())
                    when (words.size) {
                        3 -> wrap(words[0]) + " ${words[1]} " + wrap(words[2])
                        2 -> wrap(words[0]) + " " + client.wrapper + words[1] + client.wrapper
                        1 -> wrap(it)
                        else -> throw IllegalStateException("illegal column definition: $it")
                    }
                }
            })
        }
        return this
    }

    fun from(table: Table): SelectBuilder {
        from = table.toSQL(client::wrap, params)
        return this
    }

    fun where(condition: Condition): SelectBuilder {
        where = condition.toSQL(client::wrap, client.dateFormat, params)
        return this
    }

    fun groupBy(vararg columns: String): SelectBuilder {
        groupBy = columns.fieldJoin(client::wrap)
        return this
    }

    fun having(condition: Condition): SelectBuilder {
        having = condition.toSQL(client::wrap, client.dateFormat, params)
        return this
    }

    fun orderBy(vararg orders: Pair<String, String>): SelectBuilder {
        orderBy = orders.map { "${client.wrap(it.first)} ${it.second}" }.joinToString(", ")
        return this
    }

    fun toSQL() = arrayListOf("select $columnText", "from $from")
            .addNotEmpty("where ", where)
            .addNotEmpty("group by ", groupBy)
            .addNotEmpty("having ", having)
            .addNotEmpty("order by ", orderBy)
            .joinToString("\n")

    fun execute() = client.select(toSQL(), params)

}