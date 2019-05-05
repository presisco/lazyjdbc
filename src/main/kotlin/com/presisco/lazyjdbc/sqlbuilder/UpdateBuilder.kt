package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.addNotEmpty
import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazyjdbc.condition
import com.presisco.lazyjdbc.conditionNotNull

class UpdateBuilder(
        val table: String,
        client: MapJdbcClient
) : Builder(client) {
    var set: String = ""
    var where: String = ""

    fun set(vararg fields: Pair<String, Any?>): UpdateBuilder {
        set = fields.joinToString(separator = ",\n", transform = { client.wrap(it.first) + " = ?" })
        params.addAll(fields.map { it.second })
        return this
    }

    fun where(condition: Condition): UpdateBuilder {
        where = condition.toSQL(client::wrap, params)
        return this
    }

    fun where(left: Any, compare: String, right: Any?): UpdateBuilder {
        where(condition(left, compare, right))
        return this
    }

    fun whereNotNull(left: Any, compare: String, right: Any?): UpdateBuilder {
        where(conditionNotNull(left, compare, right))
        return this
    }

    override fun toSQL() = arrayListOf("update ${client.wrap(table)}", "set $set")
            .addNotEmpty("where ", where)
            .joinToString("\n")

    fun execute() = client.update(toSQL(), *params.toArray())
}