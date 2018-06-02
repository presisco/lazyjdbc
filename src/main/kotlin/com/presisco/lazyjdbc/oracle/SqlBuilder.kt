package com.presisco.lazyjdbc.oracle

import com.presisco.toolbox.StringToolbox

object SqlBuilder {
    const val INSERT_PATTERN = "insert into \"TABLE\"(COLUMNS) values(PLACEHOLDERS)"
    const val SELECT_PATTERN = "select COLUMNS from TABLE where ARGS"

    fun buildInsert(tableName: String, columns: Array<String>) = INSERT_PATTERN
            .replace("TABLE", tableName)
            .replace("COLUMNS", concatOrclColumns(columns))
            .replace("PLACEHOLDERS", StringToolbox.concat("?", columns.size, ","))

    fun concatOrclColumns(data: Array<String>): String {
        if (data.isEmpty())
            return ""
        val sb = StringBuilder()

        val iterator = data.iterator()
        sb.append('\"')
        sb.append(iterator.next())
        sb.append('\"')

        while (iterator.hasNext()) {
            sb.append(",\"")
            sb.append(iterator.next())
            sb.append('\"')
        }

        return sb.toString()
    }

    fun concatOrclColumns(data: Collection<String>): String {
        if (data.isEmpty())
            return ""
        val sb = StringBuilder()

        val iterator = data.iterator()
        sb.append('\"')
        sb.append(iterator.next())
        sb.append('\"')

        while (iterator.hasNext()) {
            sb.append(",\"")
            sb.append(iterator.next())
            sb.append('\"')
        }

        return sb.toString()
    }
}