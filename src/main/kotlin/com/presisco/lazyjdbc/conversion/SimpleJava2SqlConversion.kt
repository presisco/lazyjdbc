package com.presisco.lazyjdbc.conversion

import com.presisco.lazyjdbc.toSystemMs
import java.sql.PreparedStatement
import java.sql.Types.VARCHAR
import java.time.Instant
import java.time.LocalDateTime
import java.util.*

class SimpleJava2SqlConversion : Java2Sql {
    override fun bindList(data: List<*>, preparedStatement: PreparedStatement) {
        data.mapIndexed { i, value ->
            val index = i + 1
            with(preparedStatement) {
                if (value == null) {
                    setNull(index, VARCHAR)
                } else {
                    when (value) {
                        is String -> setString(index, value)

                        is Short -> setShort(index, value)
                        is Int -> setInt(index, value)
                        is Long -> setLong(index, value)

                        is Float -> setFloat(index, value)
                        is Double -> setDouble(index, value)

                        is Boolean -> setBoolean(index, value)
                        is Date -> setTimestamp(index, java.sql.Timestamp(value.time))
                        is Instant -> setTimestamp(index, java.sql.Timestamp(value.toEpochMilli()))
                        is LocalDateTime -> setTimestamp(index, java.sql.Timestamp(value.toSystemMs()))
                        else -> throw ConversionException(index, value, java.sql.Types.OTHER, "Unknown java type")
                    }
                }
            }
        }
    }
}