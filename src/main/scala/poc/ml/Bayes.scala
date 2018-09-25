package poc.ml

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by shiyang on 2018/5/25.
  */
object Bayes {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kmeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/home/cyony/zhusy/city.dat", 1)

    val parseData = data.map(_.split(" ").drop(1).map(_.toDouble)).map(line => {
      LabeledPoint(line(0), Vectors.dense(line.drop(1)))
    })
    val splits = parseData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    val model = NaiveBayes.train(training, lambda = 1.0)
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("NaiveBayes精度----->" + accuracy)
    model.predict(Vectors.dense("782 0.98 8541 10.92 0.87".split(' ').map(_.toDouble)))

  }

}
