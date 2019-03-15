package com.presisco.lazyjdbc.convertion

import conversion.ConversionException
import java.sql.PreparedStatement
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types.*
import java.text.SimpleDateFormat
import java.util.*

class SqlTypedJava2SqlConversion(
        private val columnSqlTypeArray: List<Int>,
        private val dateFormat: SimpleDateFormat = SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
) : Java2Sql {

    override fun bindList(data: List<*>, preparedStatement: PreparedStatement) {
        data.mapIndexed { i, value ->
            val index = i + 1
            val sqlType = columnSqlTypeArray[i]
            with(preparedStatement) {
                if (value == null) {
                    setNull(index, sqlType)
                } else {

                    fun getMs(value: Any) = when (value) {
                        is String -> dateFormat.parse(value).time
                        is Number -> value.toLong()
                        is Date -> value.time
                        else -> throw ConversionException(index, value, sqlType, "Unsupported time millisecond extraction")
                    }

                    when (sqlType) {
                        TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> setTimestamp(index, Timestamp(getMs(value)))
                        DATE -> setDate(index, java.sql.Date(getMs(value)))
                        TIME, TIME_WITH_TIMEZONE -> setTime(index, Time(getMs(value)))
                        else -> when (value) {
                            is String -> setString(index, value)

                            is Short -> setShort(index, value)
                            is Int -> setInt(index, value)
                            is Long -> setLong(index, value)

                            is Float -> setFloat(index, value)
                            is Double -> setDouble(index, value)

                            is Boolean -> setBoolean(index, value)
                            else -> throw ConversionException(index, value, sqlType, "Unknown type of value")
                        }
                    }
                }
            }
        }
    }
}