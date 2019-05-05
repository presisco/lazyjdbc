package com.presisco.lazyjdbc.conversion

import java.sql.PreparedStatement

interface Java2Sql {
    fun bindList(data: List<*>, preparedStatement: PreparedStatement)
}