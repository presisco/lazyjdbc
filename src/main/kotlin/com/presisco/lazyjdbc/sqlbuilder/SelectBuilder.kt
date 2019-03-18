package sqlbuilder

import com.presisco.lazyjdbc.client.addNotEmpty
import com.presisco.lazyjdbc.client.fieldJoin
import com.presisco.lazyjdbc.sqlbuilder.Condition
import com.presisco.lazyjdbc.sqlbuilder.Table
import java.text.SimpleDateFormat
import java.util.*

class SelectBuilder(
        val wrap: (String) -> String,
        val dateFormat: SimpleDateFormat
) {

    private lateinit var columnText: String
    private lateinit var from: String
    private var where = ""
    private var groupBy = ""
    private var having = ""
    private var orderBy = ""
    val params = LinkedList<Any?>()

    fun select(vararg columns: String): SelectBuilder {
        columnText = if (columns.isEmpty() || columns[0] == "*") {
            "*"
        } else {
            columns.fieldJoin(wrap)
        }
        return this
    }

    fun from(table: Table): SelectBuilder {
        from = table.toSQL(wrap, params)
        return this
    }

    fun where(condition: Condition): SelectBuilder {
        where = condition.toSQL(wrap, dateFormat, params)
        return this
    }

    fun groupBy(vararg columns: String): SelectBuilder {
        groupBy = columns.fieldJoin(wrap)
        return this
    }

    fun having(condition: Condition): SelectBuilder {
        having = condition.toSQL(wrap, dateFormat, params)
        return this
    }

    fun orderBy(vararg orders: Pair<String, String>): SelectBuilder {
        orderBy = orders.map { "${wrap(it.first)} ${it.second}" }.joinToString(", ")
        return this
    }

    fun toSQL() = arrayListOf("select $columnText", "from $from")
            .addNotEmpty("where ", where)
            .addNotEmpty("group by ", groupBy)
            .addNotEmpty("having ", having)
            .addNotEmpty("order by ", orderBy)
            .joinToString("\n")

}