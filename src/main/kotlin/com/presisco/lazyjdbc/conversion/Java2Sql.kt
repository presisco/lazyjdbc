package com.presisco.lazyjdbc.convertion

import java.sql.PreparedStatement

interface Java2Sql {
    fun bindArray(data: List<*>, preparedStatement: PreparedStatement)
}