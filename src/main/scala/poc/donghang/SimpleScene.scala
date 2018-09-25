package poc.donghang

import poc.donghang.Spark2s._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

import scala.util.parsing.json.JSON
import org.apache.log4j.Logger

/**
  * Created by miss on 2018-04-12.
  */
object SimpleScene {

  val LOGGER: Logger = Logger.getLogger(getClass)

  def receiveWindowData_(ssc: StreamingContext, topics: String, brokers: String, consume_location: String): DStream[String] = {
    val topicsSet = topics.split(",").toSet
    //val topicsSet = Array(topics)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "hit_lib",
      "auto.offset.reset" -> consume_location,
      "enable.auto.commit" -> (false: java.lang.Boolean) //false
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    messages.map(_.value())
  }

  def formatFunc_(data: String): String = {
    JSON.parseFull(data).get.asInstanceOf[Map[String, String]].getOrElse("payload", "")
  }

  /*def formatFunc(data: String): String = {
    val json: JSONObject = Json2s.getJson(data)
    Json2s.getString(json, "payload")
  }*/

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      LOGGER.warn("请检查输入配置文件是否正确并重新提交！")
      System.exit(-1)
    }
    LOGGER.warn("传参================================")
    //参数传入
    val propertyFilePath = args(0)
    LOGGER.warn("读取配置文件================================")
    //读取配置文件
    val props = FileTool.loadPropertiesFromPath(propertyFilePath)
    LOGGER.warn("设置参数================================")
    //设置参数
    val parametersMap = Settings.setProperty(props)
    LOGGER.warn("流会话初始化================================")
    val ssc = init_streaming(parametersMap)
    LOGGER.warn("接收数据================================")
    val o_data = receiveWindowData_(ssc, parametersMap("topic"), parametersMap("brokers"), parametersMap("consume_location"))
    LOGGER.warn("格式化数据================================")
    val format_data = o_data.map(formatFunc_).filter(_ != "").map(l => {
      val dst_ip = l.split("--")(0)
      val event_time = l.split("\\[")(1).split("\\]")(0)
      val request_type = l.split("\"")(1).split(" ")(0)
      val info = l.split(request_type)(1).trim
      val request_return_code = info.split("\" ")(1).split(" ")(0)
      var src_ip = ""

      if (info.contains("ceair.com ")) {
        val src_ip_arr = info.substring(info.indexOf("ceair.com ")).split("\" ")(1)
        if (src_ip_arr.contains(", ")) {
          if (src_ip_arr.split(", ").length == 2) src_ip = src_ip_arr.split(", ")(0)
          else if (src_ip_arr.split(", ").length == 3) src_ip = src_ip_arr.split(", ")(1)
        }
      }
      var event_type = ""
      if (info.contains("shopping")) {
        //booking
        event_type = "订票"
      } else if (info.contains("doPsdpSearchFlightStatus")) {
        event_type = "航班查询"
      } else {
        event_type = "其他"
      }
      (dst_ip, src_ip, event_time, request_type, request_return_code, event_type) //, info
    }).filter(!_._2.trim.equals(""))
    val malware_request = format_data.map(l => {
      val src_ip = l._2
      val event_time = l._3
      val date_key = event_time.split(":")(0) + ":" + event_time.split(":")(1) + ":" + event_time.split(":")(2)
      ((src_ip, date_key), 1)
    }).reduceByKey(_ + _).filter(_._2 > parametersMap("threshold_request").toInt)

    val malware_booking = format_data.map(l => {
      val src_ip = l._2
      val event_time = l._3
      val date_key = event_time.split(":")(0) + ":" + event_time.split(":")(1) + ":" + event_time.split(":")(2)
      ((src_ip, date_key, l._6), 1)
    }).reduceByKey(_ + _).filter(l => l._1._3 == "订票" && l._2 > parametersMap("threshold_login").toInt)

    malware_request.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).saveAsTextFile(parametersMap("outPutPath") + "/malware_request/" + System.currentTimeMillis())
      }
    })
    malware_booking.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).saveAsTextFile(parametersMap("outPutPath") + "/malware_booking/" + System.currentTimeMillis())
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
