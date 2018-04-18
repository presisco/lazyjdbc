package com.presisco.lazyjdbc.convertion

import java.sql.Date
import java.sql.PreparedStatement
import java.sql.Time
import java.sql.Timestamp

class SqlTypedJava2SqlConvertion(
        private val columnSqlTypeArray: Array<Int>
) : Java2Sql {

    override fun bindArray(data: Array<*>, preparedStatement: PreparedStatement) {
        data.mapIndexed { i, value ->
            val index = i + 1
            val sqlType = columnSqlTypeArray[i]
            with(preparedStatement) {
                if (value == null) {
                    setNull(index, sqlType)
                } else {
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

}