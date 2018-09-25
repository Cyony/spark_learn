package poc.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors


/**
  * Created by shiyang on 2018/5/15.
  */
object Kmeans {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kmeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/home/cyony/zhusy/city.dat", 1)

    val parseData = data.map(_.split(" ").drop(1).map(_.toDouble)).map(Vectors.dense(_))
    val model = KMeans.train(parseData, 3, 20)
    val cost = model.computeCost(parseData)
    println("深圳 is belongs to clusters:" + model.predict(Vectors.dense("1191 0.2 19492 16.37 9.75".split(' ').map(_.toDouble))))
    println("重庆 is belongs to clusters:" + model.predict(Vectors.dense("3016 8.24 17558 5.82 0.21".split(' ').map(_.toDouble))))
    println("宁波 is belongs to clusters:" + model.predict(Vectors.dense("782 0.98 8541 10.92 0.87".split(' ').map(_.toDouble))))
    val result = data.map {
      line =>
        val linevectore = Vectors.dense(line.split(" ").drop(1).map(_.toDouble))
        val prediction = model.predict(linevectore)
        line + "---------城市类别:" + prediction
    }.take(1000).foreach(println(_))
  }

}
