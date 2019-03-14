package com.presisco.lazyjdbc.convertion

import java.sql.ResultSet
import java.sql.Types

class SimpleSql2JavaConversion : Sql2Java {

    override fun toList(resultSet: ResultSet): Array<*> {
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

                    Types.DATE -> getDate(index)
                    Types.TIME -> getTime(index)
                    Types.TIMESTAMP -> getTimestamp(index)
                    else -> throw RuntimeException("type = $columnSqlType for column ${metaData.getColumnName(index)} not supported.")
                }
            }
        }

        return data
    }

}