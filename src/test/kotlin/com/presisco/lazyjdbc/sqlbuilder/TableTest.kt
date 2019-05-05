package com.presisco.lazyjdbc.sqlbuilder

import com.presisco.lazyjdbc.LazyMySQLTestClient
import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazyjdbc.table
import org.junit.Test
import java.util.*
import kotlin.test.expect

class TableTest : LazyMySQLTestClient() {
    private val wrap = { text: String -> text.split(".").joinToString(separator = ".", transform = { "`$it`" }) }
    private val client = MapJdbcClient(getDataSource())

    @Test
    fun flatTables() {
        val table = table("one").rename("o").table("another").rename("a").table("third")
        expect("`one`  `o`\n" +
                ", `another`  `a`\n" +
                ", `third`") { table.toSQL(wrap, LinkedList()) }
    }

    @Test
    fun joinTables() {
        val table = table("a")
                .leftJoin("b").rename("bat").on("bat.id", "a.sid")
                .rightJoin("c").on("a.cp", "c.dd")
                .fullJoin("d").rename("dancer").on("d.ss", "a.sid")
                .innerJoin("e").rename("echo").on("e.jj", "d.ss")
        expect("`a`\n" +
                "left join `b`  `bat` on `bat`.`id` = `a`.`sid`\n" +
                "right join `c` on `a`.`cp` = `c`.`dd`\n" +
                "full join `d`  `dancer` on `d`.`ss` = `a`.`sid`\n" +
                "inner join `e`  `echo` on `e`.`jj` = `d`.`ss`") { table.toSQL(wrap, LinkedList()) }
    }

    @Test
    fun nestedSelect() {
        val table = table("a")
                .leftJoin(client.buildSelect("*")
                        .from("names", "birth")
                        .where("name", "=", "james")).rename("bat").on("bat.id", "a.sid")
        expect("`a`\n" +
                "left join (select *\n" +
                "from `names`, `birth`\n" +
                "where `name` = ?)  `bat` on `bat`.`id` = `a`.`sid`") { table.toSQL(wrap, LinkedList()) }

    }

}