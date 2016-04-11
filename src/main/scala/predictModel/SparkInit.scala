package predictModel

/**
  * Created by benjamin658 on 2015/12/24.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SparkInit {
  val appName = "Bitcoin-predict"
  val master = "local[4]"
  val conf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
    .set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
    .set("spark.executor.memory", "2g")
    .set("spark.driver.memory", "2g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  def getSparkContext(): SparkContext = {
    sc
  }

  def getSqlContext(): SQLContext = {
    sqlContext
  }
}
