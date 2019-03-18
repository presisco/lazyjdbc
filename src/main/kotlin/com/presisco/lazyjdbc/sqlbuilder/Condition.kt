package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.client.placeHolders
import sqlbuilder.SelectBuilder

open class Condition(
        private val left: Any,
        private val compare: String,
        private val right: Any?
) {

    constructor(condition: Condition) : this(condition.left, condition.compare, condition.right)

    fun and(left: Any, compare: String, right: Any?) = Condition(this, "and", Condition(left, compare, right))

    fun and(condition: Condition) = and(condition.left, condition.compare, condition.right)

    fun andNotNull(left: Any, compare: String, right: Any?) = if (right == null) this else and(left, compare, right)

    fun andNotNull(condition: Condition) = andNotNull(condition.left, condition.compare, condition.right)

    fun or(left: Any, compare: String, right: Any?) = Condition(this, "or", Condition(left, compare, right))

    fun or(condition: Condition) = or(condition.left, condition.compare, condition.right)

    fun orNotNull(left: Any, compare: String, right: Any?) = if (right == null) this else or(left, compare, right)

    fun orNotNull(condition: Condition) = orNotNull(condition.left, condition.compare, condition.right)

    open fun toSQL(wrap: (String) -> String, params: MutableList<Any?>): String {

        val leftRaw = if (left is Condition) {
            left.toSQL(wrap, params)
        } else if (left is Collection<*>) {
            (left as Collection<String>).joinToString(separator = ", ", transform = wrap)
        } else {
            left.toString()
        }

        val rightRaw = when (right) {
            is Condition -> right.toSQL(wrap, params)
            is SelectBuilder -> {
                val sql = "(\n${right.toSQL()}\n)"
                params.addAll(right.params)
                sql
            }
            is Collection<*> -> {
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
                is Collection<*> -> builder.append("($leftRaw)")
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