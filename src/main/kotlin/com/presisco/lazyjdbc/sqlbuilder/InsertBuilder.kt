package sqlbuilder

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazyjdbc.client.addNotEmpty
import com.presisco.lazyjdbc.client.placeHolders
import java.util.*

class InsertBuilder(
        val table: String,
        client: MapJdbcClient
) : Builder(client) {

    private var columnText = ""
    private var values = LinkedList<String>()
    private var select = ""

    fun columns(vararg columns: String): InsertBuilder {
        columnText = columns.fieldJoin()
        return this
    }

    fun values(vararg values: Any?): InsertBuilder {
        params.addAll(values)
        this.values.add("(${placeHolders(values.size)})")
        return this
    }

    fun select(builder: SelectBuilder): InsertBuilder {
        select = builder.toSQL()
        params.addAll(builder.params)
        return this
    }

    override fun toSQL(): String {
        val items = arrayListOf("insert into ${client.wrap(table)}")
                .addNotEmpty("(", columnText, ")")

        if (select.isNotEmpty()) {
            items.add(select)
        } else {
            items.add("values ${values.joinToString(separator = ", ")}")
        }

        return items.joinToString("\n")
    }

    fun execute() = client.executeSQL(toSQL(), params)

}