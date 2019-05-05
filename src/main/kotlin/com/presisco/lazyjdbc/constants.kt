package com.presisco.lazyjdbc

import com.presisco.lazyjdbc.sqlbuilder.Condition
import com.presisco.lazyjdbc.sqlbuilder.EmptyCondition
import com.presisco.lazyjdbc.sqlbuilder.Table
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

const val DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"

val systemZoneId = ZoneId.systemDefault()!!

val defaultTimeStampFormat = DateTimeFormatter.ofPattern(DEFAULT_TIME_FORMAT)!!

fun nowTimeString() = defaultTimeStampFormat.format(LocalDateTime.now())!!

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