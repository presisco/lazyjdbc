package com.presisco.lazyjdbc.conversion

import java.sql.ResultSet

interface Sql2Java {
    fun toList(resultSet: ResultSet): List<*>
}