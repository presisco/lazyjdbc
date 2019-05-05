package com.presisco.lazyjdbc.sqlbuilder

object EmptyCondition : Condition("", "", null) {

    override fun toSQL(wrap: (String) -> String, params: MutableList<Any?>) = ""
}