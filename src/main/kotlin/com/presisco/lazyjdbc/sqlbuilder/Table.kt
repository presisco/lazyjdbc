package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.client.addNotEmpty
import com.presisco.lazyjdbc.client.addWith
import sqlbuilder.SelectBuilder

class Table(
        val original: Any,
        val rename: String = "",
        var next: Table? = null
) {
    var join: String = ""
    var leftKey: String = ""
    var rightKey: String = ""

    fun table(original: Any, rename: String = ""): Table {
        val newTable = Table(original, rename, next)
        next = newTable
        return this
    }

    fun join(join: String, original: Any, rename: String = ""): Table {
        val newTable = Table(original, rename, next)
        newTable.join = join
        next = newTable
        return this
    }

    fun innerJoin(original: Any, rename: String = "") = join("inner join", original, rename)

    fun leftJoin(original: Any, rename: String = "") = join("left join", original, rename)

    fun rightJoin(original: Any, rename: String = "") = join("right join", original, rename)

    fun fullJoin(original: Any, rename: String = "") = join("full join", original, rename)

    fun on(left: String, right: String): Table {
        next!!.leftKey = left
        next!!.rightKey = right
        return this
    }

    fun toSQL(wrap: (String) -> String, params: MutableList<Any?>, first: Boolean = true): String {
        val items = arrayListOf<String>()
        if (first) {

        } else if (join.isNotEmpty()) {
            items.add(join)
        } else {
            items.add(",")
        }
        items.addWith(if (original is SelectBuilder) {
            val sql = "(${original.toSQL()})"
            params.addAll(original.params)
            sql
        } else {
            wrap(original.toString())
        })
                .addNotEmpty("as ", rename, wrap)
                .addNotEmpty("on ", leftKey, wrap)
                .addNotEmpty("= ", rightKey, wrap)

        val sql = items.joinToString(" ")
        return if (next != null) {
            if (first) {
                sql + "\n" + next!!.toSQL(wrap, params, false)
            } else {
                next!!.toSQL(wrap, params, false) + "\n" + sql
            }
        } else {
            sql
        }
    }
}