package com.presisco.lazyjdbc.convertion

import java.sql.ResultSet

interface Sql2Java {
    fun toArray(resultSet: ResultSet): Array<*>
}