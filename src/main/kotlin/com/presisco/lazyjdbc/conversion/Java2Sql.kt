package com.presisco.lazyjdbc.convertion

import java.sql.PreparedStatement

interface Java2Sql {
    fun bindList(data: List<*>, preparedStatement: PreparedStatement)
}