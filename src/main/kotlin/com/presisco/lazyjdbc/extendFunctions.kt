package com.presisco.lazyjdbc.client

import com.presisco.lazyjdbc.sqlbuilder.Condition
import com.presisco.lazyjdbc.sqlbuilder.Table

fun StringBuilder.appendNotEmpty(before: String = "", text: String, after: String = ""): StringBuilder {
    if (text.isNotEmpty()) {
        append(before)
        append(text)
        append(after)
    }
    return this
}

fun Array<out String>.fieldsJoin(wrapper: String) = this.map { "$wrapper$it$wrapper" }.joinToString(", ")

fun String.toSQLValue() = "'${this.replace("'", "''")}'"

fun Any.toSQLValue(): String = when (this) {
    is Collection<*> -> joinSQLValue()
    is Number -> toString()
    is String -> toSQLValue()
    else -> toString().toSQLValue()
}

fun Collection<*>.joinSQLValue(): String = this.map {
    if (it == null)
        return@map "null"
    return@map "'$it'"
}.joinToString(", ", "( ", " )")

fun ArrayList<String>.addWith(text: String): ArrayList<String> {
    this.add(text)
    return this
}

fun ArrayList<String>.addNotEmpty(prefix: String = "", text: String, wrap: (String) -> String = { it }): ArrayList<String> {
    if (text.isNotEmpty()) {
        this.add(prefix + wrap(text))
    }
    return this
}

fun table(original: String, rename: String = "") = Table(original, rename)

fun condition(left: Any, compare: String, right: Any?) = Condition(left, compare, right)

fun condition(condition: Condition) = Condition(condition)