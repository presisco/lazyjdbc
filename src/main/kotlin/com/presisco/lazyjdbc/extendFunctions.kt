package com.presisco.lazyjdbc.client

import com.presisco.lazyjdbc.sqlbuilder.Condition
import com.presisco.lazyjdbc.sqlbuilder.Table
import sqlbuilder.EmptyCondition

fun StringBuilder.appendNotEmpty(before: String = "", text: String, after: String = ""): StringBuilder {
    if (text.isNotEmpty()) {
        append(before)
        append(text)
        append(after)
    }
    return this
}

fun ArrayList<String>.addWith(text: String): ArrayList<String> {
    this.add(text)
    return this
}

fun ArrayList<String>.addAllWith(text: Collection<String>): ArrayList<String> {
    this.addAll(text)
    return this
}

fun ArrayList<String>.addNotEmpty(prefix: String = "", text: String, postfix: String = "", wrap: (String) -> String = { it }): ArrayList<String> {
    if (text.isNotEmpty()) {
        this.add(prefix + wrap(text) + postfix)
    }
    return this
}

fun Collection<String>.fieldJoin(wrap: (String) -> String, separator: String = ", ") = this.joinToString(separator = separator, transform = wrap)

fun Array<out String>.fieldJoin(wrap: (String) -> String, separator: String = ", ") = this.joinToString(separator = separator, transform = wrap)

fun table(original: String) = Table(original)

fun condition(left: Any, compare: String, right: Any?) = Condition(left, compare, right)

fun conditionNotNull(left: Any, compare: String, right: Any?) = if (right == null) EmptyCondition else condition(left, compare, right)

fun condition(condition: Condition) = Condition(condition)

fun placeHolders(count: Int, separator: String = ", "): String {
    if (count < 1) {
        return ""
    }
    val builder = StringBuilder("?")
    var index = 1
    while (index++ < count) {
        builder.append(separator).append("?")
    }
    return builder.toString()
}