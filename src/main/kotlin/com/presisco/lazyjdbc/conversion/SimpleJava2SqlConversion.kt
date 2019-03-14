package com.presisco.lazyjdbc.convertion

import java.sql.Date
import java.sql.PreparedStatement
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types.VARCHAR

class SimpleJava2SqlConversion : Java2Sql {
    override fun bindList(data: List<*>, preparedStatement: PreparedStatement) {
        data.mapIndexed { i, value ->
            val index = i + 1
            with(preparedStatement) {
                if (value == null) {
                    setNull(index, VARCHAR)
                    return
                }
                when (value) {
                    is String -> setString(index, value)

                    is Short -> setShort(index, value)
                    is Int -> setInt(index, value)
                    is Long -> setLong(index, value)

                    is Float -> setFloat(index, value)
                    is Double -> setDouble(index, value)

                    is Boolean -> setBoolean(index, value)
                    is Date -> setDate(index, value)
                    is Time -> setTime(index, value)
                    is Timestamp -> setTimestamp(index, value)
                    else -> throw RuntimeException("Unknown type of value: " + value + ", type: " + value::class.java.name)
                }
            }
        }
    }
}