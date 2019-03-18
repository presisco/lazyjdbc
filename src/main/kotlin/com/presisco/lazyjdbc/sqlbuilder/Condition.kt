package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.client.placeHolders
import sqlbuilder.SelectBuilder
import java.text.SimpleDateFormat

class Condition(
        private val left: Any,
        private val compare: String,
        private val right: Any?
) {

    constructor(condition: Condition) : this(condition.left, condition.compare, condition.right)

    fun and(left: Any, compare: String, right: Any?) = Condition(this, "and", Condition(left, compare, right))

    fun or(left: Any, compare: String, right: Any?) = Condition(this, "or", Condition(left, compare, right))

    fun toSQL(wrap: (String) -> String, dateFormat: SimpleDateFormat, params: MutableList<Any?>): String {
        if (right == null) {
            return ""
        }

        val leftRaw = if (left is Condition) {
            left.toSQL(wrap, dateFormat, params)
        } else {
            left as String
        }

        val rightRaw = when (right) {
            is Condition -> right.toSQL(wrap, dateFormat, params)
            is SelectBuilder -> {
                val sql = "(\n${right.toSQL()}\n)"
                params.addAll(right.params)
                sql
            }
            is List<*> -> {
                params.addAll(right)
                "(${placeHolders(right.size)})"
            }
            else -> {
                params.add(right)
                "?"
            }
        }

        val leftEmpty = leftRaw.isEmpty()
        val rightEmpty = rightRaw.isEmpty()

        val builder = StringBuilder()
        if (!leftEmpty) {
            when (left) {
                is Condition -> builder.append(if (!rightEmpty) "($leftRaw)" else leftRaw).append("\n")
                is String -> builder.append(wrap(leftRaw))
            }
        }
        if (!leftEmpty && !rightEmpty) {
            builder.append(" $compare ")
        }
        if (!rightEmpty) {
            when (right) {
                is Condition -> builder.append(if (!leftEmpty) "($rightRaw)" else rightRaw).append("\n")
                else -> builder.append(rightRaw)
            }
        }
        return builder.toString().trimEnd()
    }
}