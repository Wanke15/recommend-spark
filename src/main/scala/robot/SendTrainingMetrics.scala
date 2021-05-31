package robot

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject
import scalaj.http.{Http, HttpOptions}

import scala.collection.mutable


object SendTrainingMetrics {
  def main(args: Array[String]): Unit = {

    val web_hook = ""

    val contentJson = new JSONObject()
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    val dataNum = 3127803
    val posVsNeg = "0.67 : 0.33"
    val trainAuc = 0.7012
    val testAuc = 0.6734
    val metricInfo =
      s"""${date}
         |<font color=\"green\">数据量：</font>${dataNum}
         |<font color=\"purple\">正负样本比例：</font>${posVsNeg}
         |<font color=\"blue\">训练集AUC：</font>${trainAuc}
         |<font color=\"red\">测试集AUC：</font>${testAuc}
         |@王轲
         |""".stripMargin
    contentJson.put("content", metricInfo)
    val message = mutable.HashMap("msgtype" -> "markdown", "markdown" -> contentJson)
    val messageJson = new JSONObject()
    message.foreach(x => {
      messageJson.put(x._1,x._2)
    })
    val messageStr = messageJson.toJSONString

    Http(web_hook).postData(messageStr)
    .header("Content-Type", "application/json")
    .header("Charset", "UTF-8")
    .option(HttpOptions.readTimeout(10000))

  }

}
