package robot

import java.text.SimpleDateFormat
import java.util.Date

object DateDemo {
  def main(args: Array[String]): Unit = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.format(now)
    println(date)
  }

}
