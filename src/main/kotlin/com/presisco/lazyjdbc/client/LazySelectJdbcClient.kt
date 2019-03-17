package com.presisco.lazyjdbc.client

import com.presisco.lazyjdbc.sqlbuilder.*
import javax.sql.DataSource

class LazySelectJdbcClient(
        dataSource: DataSource,
        queryTimeoutSecs: Int = 2,
        rollbackOnBatchFailure: Boolean = true
) : MapJdbcClient(
        dataSource,
        queryTimeoutSecs,
        rollbackOnBatchFailure
), From, Where, GroupBy, Having, OrderBy, Execute {

    private lateinit var columnText: String
    private lateinit var from: String
    private var where = ""
    private var groupBy = ""
    private var having = ""
    private var orderBy = ""

    fun select(vararg columns: String): From {
        columnText = if (columns.isEmpty() || columns[0] == "*") {
            "*"
        } else {
            columns.fieldsJoin(wrapper)
        }
        return this
    }

    override fun from(table: Table): Where {
        from = table.toSQL(this::wrap)
        return this
    }

    override fun where(condition: Condition): GroupBy {
        where = condition.toSQL(this::wrap, dateFormat)
        return this
    }

    override fun groupBy(vararg columns: String): Having {
        groupBy = columns.fieldsJoin(wrapper)
        return this
    }

    override fun having(condition: Condition): OrderBy {
        having = condition.toSQL(this::wrap, dateFormat)
        return this
    }

    override fun orderBy(vararg orders: Pair<String, String>): Execute {
        orderBy = orders.map { "${wrap(it.first)} ${it.second}" }.joinToString(", ")
        return this
    }

    override fun sql() = arrayListOf("select $columnText", "from $from")
            .addNotEmpty("where ", where)
            .addNotEmpty("group by ", groupBy)
            .addNotEmpty("having ", having)
            .addNotEmpty("order by ", orderBy)
            .joinToString("\n")

    override fun execute(): List<Map<String, Any?>> {
        return select(sql(), 0)
    }
}