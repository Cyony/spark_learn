package poc.donghang

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator of com.ai.batch.tools on 2017/4/27.
  */
object Spark2s {
  val LOGGER: Logger = Logger.getLogger(getClass)

  //获取变量类型
  def getType[T: Manifest](t: T): Manifest[T] = manifest[T]

  //初始化
  def init(appName: String, parallelism: String): SparkContext = {
    val sparkConf = new SparkConf().setAppName(appName)
    sparkConf.set("spark.default.parallelism", parallelism).set("spark.testing.memory", "4147480000").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    /*.set("spark.sql.warehouse.dir", "E:/Spark/SparkProgram/home/GuangDongNongXin/spark-warehouse")*/
    //初始化sparkContext
    val sc = new SparkContext(sparkConf)
    sc
  }

  def consolePrintln[T](data: RDD[T]): Unit = {
    data.collect().foreach(println)
  }

  def readData(path: String, sc: SparkContext): RDD[String] = {
    sc.textFile(path)
  }

  def dataDescribe(): Unit = {

  }

  def functionDescribe(functionName: String, info: String, windowLength: Int): Unit = {
    val functionNameLength = functionName.length
    val infoLength = info.length
    println("+" + "-" * windowLength + "+")
    println("| FunctionName: " + functionName + " " * (windowLength - 15 - functionNameLength) + "|")
    println("| Author: chenlei13@asiainfo-sec.com" + " " * (windowLength - 36) + " |")
    if (infoLength < windowLength - 10) {
      println("| Details: " + info + " " * (windowLength - 10 - infoLength) + "|")
    } else {
      println("| Details: " + info.substring(0, windowLength - 11) + " |")
      val it = (infoLength - windowLength - 11) / (windowLength - 11) + 1
      for (i <- 1 until it) println("|" + " " * 10 + info.substring(i * (windowLength - 11), (i + 1) * (windowLength - 11)) + " |")
      println("|" + " " * 10 + info.substring(it * (windowLength - 11)) + " " * ((windowLength - 10) - info.substring(it * (windowLength - 11)).length) + "|")
    }
    println("+" + "-" * windowLength + "+")
  }

  def logPrintln[T](data: T): Unit = {
    println(data)
  }

  def init_streaming(parametersMap: Map[String, String]): StreamingContext = {
    val sparkConf: SparkConf = new SparkConf().setAppName(parametersMap("appName"))
      .set("spark.default.parallelism", parametersMap("parallelism"))
      .set("log4j.logger.org.apache.spark.rpc.akka.ErrorMonitor", "FATAL")
      .set("spark.dynamicAllocation.enabled", "false")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", parametersMap("maxRatePerPartition"))
      .set("spark.testing.memory", "2147480000").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(parametersMap("windowSize").toInt))
    ssc
  }

}
