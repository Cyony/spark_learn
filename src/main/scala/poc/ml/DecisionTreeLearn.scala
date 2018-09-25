package poc.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shiyang on 2018/5/25.
  */
object DecisionTreeLearn {

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

    val categoricalFeaturesInfo = Map[Int, Int]()
    //设定输入数据的格式
    val model = DecisionTree.trainClassifier(training, 4, categoricalFeaturesInfo,
      "gini", 5, 5)

    val labelAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / test.count()
  }

}
