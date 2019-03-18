package sqlbuilder

import com.presisco.lazyjdbc.sqlbuilder.Condition

object EmptyCondition : Condition("", "", null) {

    override fun toSQL(wrap: (String) -> String, params: MutableList<Any?>) = ""
}