package com.presisco.lazyjdbc

import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

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

fun String.toSystemMs(format: DateTimeFormatter = defaultTimeStampFormat) = LocalDateTime.parse(this, format).toSystemMs()

fun LocalDateTime.toSystemMs() = this.toInstant(OffsetDateTime.now().offset).toEpochMilli()

fun Long.toLocalDateTime() = LocalDateTime.ofInstant(Instant.ofEpochMilli(this), systemZoneId)