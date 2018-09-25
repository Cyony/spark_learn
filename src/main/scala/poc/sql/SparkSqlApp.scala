package poc.sql

import java.io.{File, FileInputStream, FileReader, InputStreamReader}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by shiyang on 2018/3/23.
  */
object SparkSqlApp {


  def createkafkaProducer(bootstrap: String): KafkaProducer[String, String] = {
    val prop = new Properties()
    prop.put("bootstrap.servers", bootstrap)
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](prop)
  }

  def saveToKafka(df: DataFrame, bootstrap: Broadcast[String]): Unit = {
    var producer: KafkaProducer[String, String] = null
    df.toJSON.rdd.foreach(msg => {
      if (producer == null) {
        producer = createkafkaProducer(bootstrap.value)
      }
      producer.send(new ProducerRecord[String, String]("sparksql", msg.hashCode.toString, msg))
    })
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("aus-sql")
      .master("local[*]")
      .getOrCreate()
    val prop = new Properties()
    prop.load(new FileInputStream("./conf/spark-sql.conf"))
    val sql = prop.getProperty("spark.sql")
    val dataSource = prop.getProperty("spark.source")
    val kafka = spark.sparkContext.broadcast(prop.getProperty("spark.kafka"))

    val df = spark.read.parquet(dataSource)
    df.createOrReplaceTempView("ausview1")


    val data = spark.sql(sql)
    data.show(10)
    saveToKafka(data, kafka)
  }

}
