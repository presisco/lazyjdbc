package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.client.addNotEmpty
import com.presisco.lazyjdbc.client.addWith

class Table(
        val original: String,
        val rename: String = "",
        var next: Table? = null
) {
    var join: String = ""
    var leftKey: String = ""
    var rightKey: String = ""

    fun join(join: String, original: String, rename: String = ""): Table {
        val newTable = Table(original, rename, next)
        newTable.join = join
        next = newTable
        return this
    }

    fun innerJoin(original: String, rename: String = "") = join("inner join", original, rename)

    fun on(left: String, right: String): Table {
        next!!.leftKey = left
        next!!.rightKey = right
        return this
    }

    fun toSQL(wrap: (String) -> String): String {
        val items = arrayListOf<String>()
                .addNotEmpty(text = join)
                .addWith(wrap(original))
                .addNotEmpty("as ", rename, wrap)
                .addNotEmpty("on ", leftKey, wrap)
                .addNotEmpty("= ", rightKey, wrap)

        val sql = items.joinToString(" ")
        return if (next != null) {
            sql + "\n" + next!!.toSQL(wrap)
        } else {
            sql
        }
    }

}