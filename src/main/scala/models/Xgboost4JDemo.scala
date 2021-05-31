package models

import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassifier, XGBoostClassificationModel}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


object Xgboost4JDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val schema = new StructType(Array(
      StructField("sepal length", DoubleType, true),
      StructField("sepal width", DoubleType, true),
      StructField("petal length", DoubleType, true),
      StructField("petal width", DoubleType, true),
      StructField("class", StringType, true)
    ))
    val rawInput = spark.read.schema(schema).option("header", true).csv("src/main/resources/data/iris.data")
    rawInput.show(false)

    import org.apache.spark.ml.feature.StringIndexer
    val stringIndexer = new StringIndexer().
      setInputCol("class").
      setOutputCol("label").
      fit(rawInput)
    val labelTransformed = stringIndexer.transform(rawInput).drop("class")

    import org.apache.spark.ml.feature.VectorAssembler
    val vectorAssembler = new VectorAssembler().
      setInputCols(Array("sepal length", "sepal width", "petal length", "petal width")).
      setOutputCol("features")
    val xgbInput = vectorAssembler.transform(labelTransformed).select("features", "label")

    //    val Array(train, test) = xgbInput.randomSplit(Array(0.8, 0.2))

    import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
    val xgbParam = Map("eta" -> 0.1f,
      "missing" -> -999,
      "objective" -> "multi:softprob",
      "num_class" -> 3,
      "num_round" -> 100,
      "num_workers" -> 2)
    val xgbClassifier = new XGBoostClassifier(xgbParam).
      setFeaturesCol("features").
      setLabelCol("label")
    xgbClassifier.setMaxDepth(2)
    xgbClassifier.setTreeMethod("approx")

    val xgbClassificationModel = xgbClassifier.fit(xgbInput)
    val results = xgbClassificationModel.transform(xgbInput)
    results.show(false)

    val xgbClassificationModelPath = "src/main/resources/models/xgbClassificationModel"
    xgbClassificationModel.write.overwrite().save(xgbClassificationModelPath)


  }

}
