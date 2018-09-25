package poc.ml

import org.apache.spark.mllib.clustering._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by shiyang on 2018/5/15.
  */
object Gmm {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kmeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/home/cyony/zhusy/city.dat", 1)

    val parseData = data.map(_.split(" ").drop(1).map(_.toDouble)).map(Vectors.dense(_))
    val gmm=new GaussianMixture().setK(3).setMaxIterations(100).setSeed(1L)
    val model1=gmm.run(parseData)
    println("重庆 is belongs to clusters:" + model1.predict(Vectors.dense("3016 8.24 17558 5.82 0.21".split(' ').map(_.toDouble))))

    val ldaModel  =new LDA().setK(3).run(parseData.zipWithIndex.map(_.swap).cache())
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print(s"Topic $topic :")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(s"${topics(word, topic)}")
      }
      println()
    }

    val bkm = new BisectingKMeans().setK(3)



  }

}
