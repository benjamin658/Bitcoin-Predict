package predictModel

import com.google.gson.Gson
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import scala.collection.JavaConversions._

/**
  * Created by benjamin658 on 2015/12/24.
  */

case class BarId(
  bar: Double
)

case class Bar(
  _id: BarId,
  volBTC: Double,
  volUSD: Double,
  maxPrice: Double,
  minPrice: Double,
  numberTrades: Double,
  open: Double,
  close: Double
)

object RandomForestModel {
  /**
    * entry point
    */
  def main(args: Array[String]): Unit = {
    // 將spark執行時的INFO log關掉，方便debug
    val rootLogger = Logger.getRootLogger().setLevel(Level.WARN)
    // spark context
    val sc = SparkInit.getSparkContext()

    val mongoClient = new MongoClient()
    val db = mongoClient.getDatabase("bitcoin")
    val gson = new Gson()

    println("開始從DAL取得資料.....")
    val barData = db.getCollection("BarData")
      .find(Filters.eq("_id.source", "btceUSD"))
      .iterator()
      .map(barObj => {
        val bar = gson.fromJson(barObj.toJson, classOf[Bar])
        Array(bar.volBTC, bar.volUSD, bar.maxPrice, bar.minPrice, bar.numberTrades, bar.open, bar.close)
      }).toArray
    println("取得資料完成.....")

    val barRDD = sc.parallelize(barData, 4).randomSplit(Array(0.6, 0.4))
    val initTrainData = barRDD(0).collect()
    val initTestData = barRDD(1).collect()

    // 決定要往前看幾個bar
    val historyBarShift = 5

    // 將當前的bar與之前的historyBarShift個做merge
    println("開始merge history bar，共往前merge" + historyBarShift + "個bar")
    val mergedTrainData = mergeBarData(initTrainData, initTrainData.length - 1, historyBarShift)
    val mergedTestData = mergeBarData(initTestData, initTestData.length - 1, historyBarShift)
    println("Merge Bar完成")

    println("開始計算label與features....")
    val finalTrainData = sc.parallelize(mergedTrainData.takeRight(mergedTrainData.length - historyBarShift), 4)
      .map((trainDataSet) => {
        val features = trainDataSet.takeRight(trainDataSet.length - 7)
        val label = signLabel(trainDataSet)
        LabeledPoint(label, Vectors.dense(features))
      })

    val finalTestData = sc.parallelize(mergedTestData.takeRight(mergedTestData.length - historyBarShift), 4)
      .map((testDataSet) => {
        val features = testDataSet.takeRight(testDataSet.length - 7)
        val label = signLabel(testDataSet)
        LabeledPoint(label, Vectors.dense(features))
      })
    println("label與features計算完成....")

    println("開始訓練模型.....")
    val maxTreeDepth = 5
    val maxBins = 32
    val numClasses = 3
    val numTrees = 64
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val featureSubsetStrategy = "auto"

    val dtModel = RandomForest.trainClassifier(finalTrainData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxTreeDepth, maxBins)
    println("模型訓練完成.....")

    println("計算精準度.....")
    val labelAndPreds = finalTestData.map { point =>
      val prediction = dtModel.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / finalTestData.count()
    val accur = 1 - testErr

    println("精準度 = " + accur)

  }

  def mergeBarData(barData: Array[Array[Double]], index: Int, shift: Int): Array[Array[Double]] = {
    def currentBarData = barData
    if (index - shift < 0) currentBarData
    else {
      for (i <- 1 to shift) {
        currentBarData(index) = currentBarData(index) ++ currentBarData(index - i)
      }
      mergeBarData(currentBarData, index - 1, shift)
    }
  }

  def signLabel(features: Array[Double]): Double = {
    if (features(2) >= features(13) * 1.004) 2 //* 1.002
    else if (features(3) <= features(13) * 0.996) 0 // * 0.998
    else 1
  }

}
