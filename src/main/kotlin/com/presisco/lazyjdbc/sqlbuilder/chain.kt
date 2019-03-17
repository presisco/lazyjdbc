package com.presisco.lazyjdbc.sqlbuilder

interface From {
    fun from(table: Table): Where
}

interface Where : Execute {
    fun where(condition: Condition): GroupBy
}

interface GroupBy : Execute {
    fun groupBy(vararg columns: String): Having
}

interface Having : Execute {
    fun having(condition: Condition): OrderBy
}

interface OrderBy {
    fun orderBy(vararg orders: Pair<String, String>): Execute
}

interface Execute {
    fun execute(): List<Map<String, Any?>>

    fun sql(): String
}