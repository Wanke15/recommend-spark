package feature.profile

import org.apache.spark.sql.{SaveMode, SparkSession}

object FactLabel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("als").master("local[2]").getOrCreate()

    val data = spark.sparkContext.textFile("src/main/resources/data/events.csv")
    /**
     * timestamp,visitorid,event,itemid,transactionid
     * 1433221332117,257597,view,355908,
     */

    val basicTags = data
      .map(x => x.split(","))
      .filter(x => x(0) != "timestamp")
        .map(x => (x(1), x(2)))
        .map(x => (x, 1))
        .reduceByKey(_+_)
        .map{
          case ((userId, behavior), cnt) => (userId, (behavior, cnt))
        }
        .groupByKey().map{
      case (userId, tags) =>
        val tg = tags.toMap
        val view = tg.getOrElse("view", 0)
        val cart = tg.getOrElse("addtocart", 0)
        val transact = tg.getOrElse("transaction", 0)

        val cartRate = if (view > 0) {
          cart.toDouble / view
        } else {0}

        val trasctRate = if (view > 0) {
          transact.toDouble / view
        } else {0}

        (userId, view, cart, transact, cartRate, trasctRate)
    }

//    basicTags.take(10).foreach(println)
//    basicTags.repartition(1).saveAsTextFile("src/main/resources/data/factLabel")

    import spark.implicits._
    basicTags.toDF().coalesce(1).write.format("csv").mode(SaveMode.Overwrite).save("src/main/resources/data/factLabelDf")

  }

}
