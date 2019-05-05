package com.presisco.lazyjdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.time.LocalDateTime
import kotlin.test.expect

class MySQLTest : LazyMySQLTestClient() {
    private lateinit var client: MapJdbcClient

    @Before
    fun prepare() {
        client = MapJdbcClient(getDataSource())
        client.executeSQL("CREATE TABLE times  (\n" +
                "  time timestamp(3) NULL\n" +
                ");")
    }

    @Test
    fun timestampStringConversion() {
        val localDateTime = LocalDateTime.now()
        val timeString = client.dateFormat.format(localDateTime)
        println("created: $timeString")
        client.insert("times", mapOf("time" to timeString))
        val selected = client.buildSelect("time")
                .from("times")
                .execute()
                .map { it["time"] }
                .first()
        println("selected: $selected")
        expect(localDateTime) {
            selected
        }
    }

    @Test
    fun timestampLocalDateTimeConversion() {
        val localDateTime = LocalDateTime.now()
        println("created: $localDateTime")
        client.insert("times", mapOf("time" to localDateTime))
        val selected = client.buildSelect("time")
                .from("times")
                .execute()
                .map { it["time"] }
                .first()
        println("selected: $selected")
        expect(localDateTime) {
            selected
        }
    }

    @After
    fun cleanup() {
        client.executeSQL("drop table times")
    }

}