package poc.donghang

import java.util.Properties

/**
  * Created by Administrator of tools on 2016/12/28.
  */
object Settings {

  def setProperty(props: Properties): Map[String, String] = {
    var result = Map[String, String]()
    result += ("parallelism" -> props.getProperty("spark.parallelism"))
    result += ("appName" -> props.getProperty("spark.app.name"))
    result += ("windowSize" -> props.getProperty("spark.windowsize"))
    result += ("maxRatePerPartition" -> props.getProperty("spark.maxRatePerPartition"))
    result += ("modelPath" -> props.getProperty("spark.modelpath"))
    result += ("separator" -> props.getProperty("spark.separator"))
    result += ("ipDBPath" -> props.getProperty("ipDBPath"))
    result += ("searcherAlgorithm" -> props.getProperty("searcherAlgorithm"))
    result += ("table_high_risk_account" -> props.getProperty("mysql.hit.account"))
    result += ("table_high_risk_ip" -> props.getProperty("mysql.hit.ip"))
    result += ("table_shield" -> props.getProperty("mysql.hit.shield"))
    result += ("url" -> props.getProperty("mysql.url"))
    result += ("username" -> props.getProperty("mysql.user.name"))
    result += ("password" -> props.getProperty("mysql.pass.word"))
    result += ("db" -> props.getProperty("mysql.db"))
    result += ("threshold_request" -> props.getProperty("threshold_request"))
    result += ("threshold_login" -> props.getProperty("threshold_login"))
    result += ("threshold_force" -> props.getProperty("threshold_force"))
    result += ("threshold_port" -> props.getProperty("threshold_port"))
    result += ("threshold_dstIp" -> props.getProperty("threshold_dstIp"))
    result += ("threshold_account" -> props.getProperty("threshold_account"))
    result += ("topic" -> props.getProperty("kafka.topic"))
    result += ("brokers" -> props.getProperty("kafka.brokers"))
    result += ("consume_location" -> props.getProperty("kafka.consume_location"))
    result += ("outPutPath" -> props.getProperty("file.output"))

    result
  }
}
