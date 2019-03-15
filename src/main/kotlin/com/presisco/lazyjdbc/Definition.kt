import java.text.SimpleDateFormat

object Definition {
    const val DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"
    val defaultDateFormat = SimpleDateFormat(DEFAULT_TIME_FORMAT)
    fun currentTimeString() = defaultDateFormat.format(System.currentTimeMillis())
}