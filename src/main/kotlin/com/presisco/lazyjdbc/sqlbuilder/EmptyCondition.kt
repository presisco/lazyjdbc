package sqlbuilder

import com.presisco.lazyjdbc.sqlbuilder.Condition
import java.text.SimpleDateFormat

object EmptyCondition : Condition("", "", null) {

    override fun toSQL(wrap: (String) -> String, dateFormat: SimpleDateFormat, params: MutableList<Any?>) = ""
}