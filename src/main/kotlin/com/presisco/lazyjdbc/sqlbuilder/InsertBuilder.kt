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
        if (this.values.isEmpty()) {
            this.values.add("(${placeHolders(values.size)})")
        } else {
            when (client.databaseName) {
                "oracle" -> {
                    val builder = StringBuilder("into ${client.wrap(table)} ")
                    if (columnText.isNotEmpty()) {
                        builder.append(columnText)
                    }
                    builder.append(" values ")
                    builder.append("(${placeHolders(values.size)})")
                    this.values.add(builder.toString())
                }
                else -> this.values.add("(${placeHolders(values.size)})")
            }
        }

        return this
    }

    fun select(builder: SelectBuilder): InsertBuilder {
        select = builder.toSQL()
        params.addAll(builder.params)
        return this
    }

    override fun toSQL(): String {
        return when (client.databaseName) {
            "oracle" -> {
                if (values.size < 2) {
                    val items = arrayListOf("insert into ${client.wrap(table)}")
                            .addNotEmpty("(", columnText, ")")
                    if (select.isNotEmpty()) {
                        items.add(select)
                    } else {
                        items.add("values ${values.joinToString(separator = ", ")}")
                    }
                    items.joinToString("\n")
                } else {
                    val items = arrayListOf("insert all")

                    val builder = StringBuilder("into ${client.wrap(table)} ")
                    if (columnText.isNotEmpty()) {
                        builder.append(columnText)
                    }
                    builder.append(" values ")
                    builder.append(values[0])
                    values[0] = builder.toString()
                    items.addAll(values)
                    items.add("select * from dual")
                    items.joinToString("\n")
                }
            }
            else -> {
                val items = arrayListOf("insert into ${client.wrap(table)}")
                        .addNotEmpty("(", columnText, ")")
                if (select.isNotEmpty()) {
                    items.add(select)
                } else {
                    items.add("values ${values.joinToString(separator = ", ")}")
                }
                items.joinToString("\n")
            }
        }
    }

    fun execute() = client.executeSQL(toSQL(), *params.toArray())

}