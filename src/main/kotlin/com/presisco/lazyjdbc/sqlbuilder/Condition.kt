package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.client.joinSQLValue
import com.presisco.lazyjdbc.client.toSQLValue
import java.text.SimpleDateFormat
import java.util.*

class Condition(
        private val left: Any,
        private val compare: String,
        private val right: Any?
) {

    constructor(condition: Condition) : this(condition.left, condition.compare, condition.right)

    fun and(left: Any, compare: String, right: Any?) = Condition(this, "and", Condition(left, compare, right))

    fun or(left: Any, compare: String, right: Any?) = Condition(this, "or", Condition(left, compare, right))

    fun toSQL(wrap: (String) -> String, dateFormat: SimpleDateFormat): String {
        if (right == null) {
            return ""
        }

        val leftRaw = if (left is Condition) left.toSQL(wrap, dateFormat) else left as String
        val rightRaw = when (right) {
            is Condition -> right.toSQL(wrap, dateFormat)
            is List<*> -> right.joinSQLValue()
            is Number -> right.toString()
            is Date -> dateFormat.format(right).toSQLValue()
            else -> right.toSQLValue()
        }

        val leftEmpty = leftRaw.isEmpty()
        val rightEmpty = rightRaw.isEmpty()

        val builder = StringBuilder()
        if (!leftEmpty) {
            when (left) {
                is Condition -> builder.append(if (!rightEmpty) "($leftRaw)" else leftRaw)
                is String -> builder.append(wrap(leftRaw))
            }
        }
        if (!leftEmpty && !rightEmpty) {
            builder.append(" $compare ")
        }
        if (!rightEmpty) {
            when (right) {
                is Condition -> builder.append(if (!leftEmpty) "($rightRaw)" else rightRaw)
                else -> builder.append(rightRaw)
            }
        }

        return builder.toString()
    }
}