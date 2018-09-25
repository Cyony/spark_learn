package poc

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by shiyang on 2018/2/16.
  */
object BaseData {

  //尾递归
  def fab(num: Int): Int = {
    if (num <= 0) 1
    else fab(num - 1) + fab(num - 2)
  }

  //默认参数
  def sayHellow(fisrtName: String, midName: String = "a", lastName: String = "b"): String = {
    fisrtName + midName + lastName
  }

  //变长函数
  def sum(nums: Int*): Int = {
    if (nums.length == 0) 0
    else {
      nums.head + sum(nums.tail: _*)
    }
  }

  //ArrayBuffer
  def arrayLearn {
    val buffer = new ArrayBuffer[Int]()
    buffer += 1
    buffer += (8, 9)
    buffer ++= Array(2, 3, 4, 5, 6, 7)
    for (num <- buffer) print(num)
    println()
    val after = buffer.filter(_ % 2 == 0).map(_ * 2)
    println(after.mkString(","))
    after -= 16
    println(after.mkString(","))
  }

  //list
  def listLearn: Unit = {
    val listBuffer = new ListBuffer[Any]
    listBuffer += 1
    listBuffer += (2, "a")
    listBuffer ++= List("b", 3, 4)
    listBuffer -= 1
    println(listBuffer.mkString(","))
  }

  //map
  def mapLearn = {
    val myMap = new mutable.HashMap[String, String]()
    myMap += ("jack" -> "30", "tom" -> "20", "jhon" -> "25")
    println(myMap.getOrElse("jack", 0))
    for ((key, value) <- myMap) println(key + ": " + value)
  }

  //tuple
  def tupleLearn = {
    val array1 = Array("jack", "tom", "leo")
    val array2 = Array(10, 20, 30)
    val tuples = array1.zip(array2)
    println(tuples.apply(1))
    for (t <- tuples) println(t._1 + ": " + t._2)
  }

  def string2Array(line: String): ArrayBuffer[String] = {
    val array = new ArrayBuffer[String]()
    for (a <- line) array += a.toString
    array
  }


  def main(args: Array[String]): Unit = {
    val s = Student("Cyony", 25)
    println(s.toString)
    // listLearn
    //tupleLearn
    // mapLearn
    //println(fab(10))
    //println(sayHellow("cyony", lastName = "s"))
    // println(sum(1 to 10: _*))
    // arrayLearn
    //    val test = Array(("a", 1), ("b", 2))
    //    val test1 = test.map((x: Tuple2[String, Int]) => (x._2 + 1, x._1))
    //    println(test1.toList)
    //
    //    val sc = new SparkContext()
    //    val rdd = sc.textFile("")
    //    sc.textFile("").flatMap(_.toArray.map(_.toString)).map((_, 1))
    //      .reduceByKey(_ + _).map(_.swap).sortByKey()
    //      .filter((x: Tuple2[Int, String]) => x._1 > 100 && !x._2.equals(""))
    //      .mapPartitions(_.toList.map(_.swap).toIterator)
    //    rdd.union(rdd)
    //    sc.textFile("/home/cyony/cluster/hadoop-2.6.0/etc/hadoop/yarn-env.sh").flatMap(_.toArray.map(_.toString)).map((_, 1)).reduceByKey(_ + _).mapPartitions(_.toList.map(_.swap).toIterator).sortByKey(false).filter((x: Tuple2[Int, String]) => x._1 > 100 && !x._2.equals(" ")).collect
    //
    //
    //    val spark = new SparkSession(sc)
    //    val source = spark.read.parquet("")
    //    source.write.mode(SaveMode.Append).save("")
    //    source.coalesce(1).write.parquet("")
  }

}
