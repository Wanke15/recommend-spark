package feature.label

import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author wangke
 * @date 2021/5/14 5:38 下午
 * @version 1.0
 */
object TrainingData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local[*]")
      .appName("SearchAlgorithm")
      .getOrCreate()

    val log = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/data/my_rec_log.csv")
    log.show(false)
    //    log.printSchema()

    log.createOrReplaceTempView("query_ana_filter_combine")

    val days = 7

    val dd = spark.sql(
      f"""
         |select who, bv_tr_id, collect_list(concat_ws('\0', product_name, if(add_cart = 1, 1, if(is_clickin = 1, 1, 0)))) as exposure
         |    from query_ana_filter_combine
         |    where cur_time >= concat_ws('', split(date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), '${days}'), '-')) and product_id is not null and log_type in (4, 5)
         |group by who, bv_tr_id
         |""".stripMargin)

    dd.show(false)

    import spark.implicits._

    val newData = dd.map(row => {
      val products = row.getSeq [String](2)
      val prodSplits = products
        .map(x => x.split("\0"))
        .map(x => (x(0), x(1).toInt))

      val posProds = prodSplits.filter(x => x._2 == 1)

      val posProdPids = posProds.map(x => x._1)

      val negProds = prodSplits.filter(x => x._2 == 0).filter(x => !posProdPids.contains(x._1))
      (row.getString(0), row.getString(1), posProds ++ negProds)
    })

    newData.show(false)
  }
}