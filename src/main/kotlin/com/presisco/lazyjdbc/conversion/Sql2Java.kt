package com.presisco.lazyjdbc.convertion

import java.sql.ResultSet

interface Sql2Java {
    fun toList(resultSet: ResultSet): Array<*>
}