package com.presisco.lazyjdbc.convertion

import conversion.ConversionException
import java.sql.PreparedStatement
import java.sql.Types.VARCHAR
import java.util.*

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
                    is Date -> setTimestamp(index, java.sql.Timestamp(value.time))
                    else -> throw ConversionException(index, value, java.sql.Types.OTHER, "Unknown java type")
                }
            }
        }
    }
}