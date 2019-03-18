package sql.sqlbuilder

import com.presisco.lazyjdbc.client.condition
import com.presisco.lazyjdbc.client.conditionNotNull
import org.junit.Test
import sqlbuilder.EmptyCondition
import java.util.*
import kotlin.test.expect

class ConditionTest {
    private val wrap = { text: String -> text.split(".").joinToString(separator = ".", transform = { "`$it`" }) }

    @Test
    fun emptyCondition() {
        expect(EmptyCondition) { conditionNotNull("name", "=", null) }
        val params = LinkedList<Any?>()
        var condition = condition("age", ">", 18)
                .andNotNull("gender", "in", null)
        expect("`age` > ?") { condition.toSQL(wrap, params) }
        expect(listOf<Any?>(18)) { params }
    }

    @Test
    fun flatConditionMix() {
        val condition = condition("name", "=", "james")
                .and("age", ">", 18)
                .or("gender", "=", null)
        val params = LinkedList<Any?>()
        expect("((`name` = ?)\n" +
                " and (`age` > ?))\n" +
                " or (`gender` = ?)") { condition.toSQL(wrap, params) }
        expect(listOf("james", 18, null)) { params }
    }

    @Test
    fun nestedConditionMix() {
        val date = Date(System.currentTimeMillis())
        val condition = condition(condition("name", "=", "james").or("time", "<", date))
                .and(condition("age", ">", 18).and("gender", "in", listOf("male", "female")))
        val params = LinkedList<Any?>()
        expect("((`name` = ?)\n" +
                " or (`time` < ?))\n" +
                " and ((`age` > ?)\n" +
                " and (`gender` in (?, ?)))") { condition.toSQL(wrap, params) }
        expect(listOf<Any?>("james", date, 18, "male", "female")) { params }
    }

    @Test
    fun nestedNullConditionMix() {
        val date = Date(System.currentTimeMillis())
        val condition = condition(condition("name", "=", "james").or("time", "<", date))
                .and(conditionNotNull("age", ">", null).and("gender", "in", listOf("male", "female")))
        val params = LinkedList<Any?>()
        expect("((`name` = ?)\n" +
                " or (`time` < ?))\n" +
                " and (`gender` in (?, ?))") { condition.toSQL(wrap, params) }
        expect(listOf<Any?>("james", date, "male", "female")) { params }
    }
}