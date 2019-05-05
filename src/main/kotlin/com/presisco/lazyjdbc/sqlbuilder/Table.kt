package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.addNotEmpty
import com.presisco.lazyjdbc.addWith

class Table(
        val original: Any,
        var next: Table? = null
) {
    var rename: String = ""
    var join: String = ""
    var leftKey: String = ""
    var rightKey: String = ""

    fun table(original: Any): Table {
        val newTable = Table(original, next)
        next = newTable
        return this
    }

    fun join(join: String, original: Any): Table {
        val newTable = Table(original, next)
        newTable.join = join
        next = newTable
        return this
    }

    fun innerJoin(original: Any) = join("inner join", original)

    fun leftJoin(original: Any) = join("left join", original)

    fun rightJoin(original: Any) = join("right join", original)

    fun fullJoin(original: Any) = join("full join", original)

    fun rename(rename: String): Table {
        if (next == null) {
            this.rename = rename
        } else {
            next!!.rename = rename
        }
        return this
    }

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
                .addNotEmpty(" ", rename, wrap = wrap)
                .addNotEmpty("on ", leftKey, wrap = wrap)
                .addNotEmpty("= ", rightKey, wrap = wrap)

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