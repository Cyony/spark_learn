package poc.steaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{rdd, _}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StateSpec

import scala.util.parsing.json.JSON
import org.apache.spark.streaming.State

/**
  * Created by shiyang on 2018/3/22.
  */
object KafkaStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("hdfs://10.21.17.209:9000/spark/streaming/cyony")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.21.17.209:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-06",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](Set("cyony"), kafkaParams))

    val mappingFun = (sex: Int, score: Option[Int], state: State[Int]) => {
      val sum = score.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(sum)
      (sex, sum)
    }

    val updateFun = (currentValue: Seq[Int], preValue: Option[Int]) => {
      Some(currentValue.sum + preValue.getOrElse(0))
    }

    //    messages.foreachRDD(rdd => {
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    //    }
    //    )
    messages.map(_.value()).map(JSON.parseFull(_).get.asInstanceOf[Map[String, String]])
      .map(map => (map.get("sex").get.toInt, map.get("score").get.toInt))
      .combineByKey(
        score => score,
        (c1: Int, newScore) => c1 + newScore,
        (c1: Int, c2: Int) => c1 + c2,
        new HashPartitioner(2)
      )
      .reduceByKey(_ + _)
      .mapWithState(StateSpec.function(mappingFun))
      .updateStateByKey(updateFun)
      .print()


    ssc.start()
    ssc.awaitTermination()

  }

}
