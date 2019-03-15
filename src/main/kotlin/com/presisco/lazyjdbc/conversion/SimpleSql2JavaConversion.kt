package com.presisco.lazyjdbc.convertion

import conversion.ConversionException
import java.sql.ResultSet
import java.sql.Types
import java.util.*

class SimpleSql2JavaConversion : Sql2Java {

    override fun toList(resultSet: ResultSet): List<*> {
        val metaData = resultSet.metaData
        val columnCount = metaData.columnCount
        val data = Array<Any?>(columnCount, { null })

        for (i in data.indices) {
            val index = i + 1
            val columnSqlType = metaData.getColumnType(index)

            with(resultSet) {
                data[i] = when (columnSqlType) {
                    Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR -> getString(index)

                    Types.TINYINT -> getByte(index)
                    Types.SMALLINT -> getShort(index)
                    Types.INTEGER, Types.DECIMAL -> getInt(index)
                    Types.BIGINT -> getLong(index)

                    Types.FLOAT, Types.REAL -> getFloat(index)
                    Types.DOUBLE, Types.NUMERIC, 2 -> getDouble(index)

                    Types.BOOLEAN -> getBoolean(index)

                    Types.BIT -> getByte(index)
                    Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> getBytes(index)

                    Types.DATE -> Date(getDate(index).time)
                    Types.TIME -> Date(getTime(index).time)
                    Types.TIMESTAMP -> Date(getTimestamp(index).time)
                    else -> throw ConversionException(index, null, columnSqlType, "type for column ${metaData.getColumnName(index)} not supported.")
                }
            }
        }

        return data.toList()
    }

}