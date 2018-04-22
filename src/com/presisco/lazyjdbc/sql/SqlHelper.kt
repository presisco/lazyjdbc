package com.presisco.lazyjdbc.sql

import com.presisco.toolbox.StringToolbox

object SqlHelper {
    const val INSERT_BATCH = "insert into TABLE(COLUMNS) values"

    fun buildValue(data: Any?): String {
        if (data == null)
            return "null"
        return when (data) {
            is Number -> data.toString()
            is String -> "'${data.replace("'", "''")}'"
            else -> throw RuntimeException("unsupported type! value: $data")
        }
    }

    fun buildValues(row: Array<*>): String {
        if (row.isEmpty())
            return "()"
        val sb = StringBuilder("(")
        val iterator = row.iterator()
        sb.append(buildValue(iterator.next()))

        while (iterator.hasNext()) {
            sb.append(',')
            sb.append(buildValue(iterator.next()))
        }

        return sb.append(')').toString()
    }

    fun buildBatchInsertString(tableName: String, columns: Array<String>, rows: List<Array<*>>): String {
        if (rows.isEmpty())
            return "()"

        val sb = StringBuilder(INSERT_BATCH
                .replace("TABLE", tableName)
                .replace("COLUMNS", StringToolbox.concat(columns, ","))
        )
        val iterator = rows.iterator()
        sb.append(buildValues(iterator.next()))

        while (iterator.hasNext()) {
            sb.append(',')
            sb.append(buildValues(iterator.next()))
        }

        return sb.toString()
    }

}