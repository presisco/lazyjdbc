package sqlbuilder

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazyjdbc.client.addNotEmpty
import com.presisco.lazyjdbc.client.condition
import com.presisco.lazyjdbc.client.conditionNotNull
import com.presisco.lazyjdbc.sqlbuilder.Condition

class DeleteBuilder(
        val table: String,
        client: MapJdbcClient
) : Builder(client) {
    private var where = ""

    fun where(condition: Condition): DeleteBuilder {
        where = condition.toSQL(client::wrap, params)
        return this
    }

    fun where(left: Any, compare: String, right: Any?): DeleteBuilder {
        where(condition(left, compare, right))
        return this
    }

    fun whereNotNull(left: Any, compare: String, right: Any?): DeleteBuilder {
        where(conditionNotNull(left, compare, right))
        return this
    }

    override fun toSQL() = arrayListOf("delete from ${client.wrap(table)}")
            .addNotEmpty("where ", where)
            .joinToString("\n")

    fun execute() = client.delete(toSQL(), *params.toArray())

}