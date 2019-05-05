package com.presisco.lazyjdbc.conversion

class ConversionException(
        val index: Int,
        val value: Any?,
        val sqlType: Int,
        message: String
) : Exception(
        "$message, index: $index, value: $value, type of value: ${if (value == null) null else value::class.java.name}, sql type: $sqlType"
)