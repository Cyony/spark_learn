package poc.steaming

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategy, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Created by shiyang on 2018/8/22.
  */
object KafkaZkStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val topic: String = "test"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "xxx.xxx.xxx.xxx:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-06",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    val zkClient = new ZkClient("XXX.XXX.XXX.XXX")
    var fromOffsets: Map[TopicPartition, Long] = Map()
    val children = zkClient.countChildren("offsetDir")
    if (children > 0) {
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String]("offsetDir" + "/" + i)
        val tp = new TopicPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)
        kafkaStream = KafkaUtils.createDirectStream[String, String](
          ssc, PreferConsistent, Subscribe[String, String](Set(topic), kafkaParams, fromOffsets)
        )
      }
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](Set(topic), kafkaParams)
      )
    }



  }

}
