package poc

import scala.collection.mutable

/**
  * Created by shiyang on 2018/2/16.
  */
object Match {

  val map = Map(
    "jack" -> "A",
    "tom" -> "B",
    "katty" -> "C"
  )

  def jundgeGrade(name: String) = {
    val grade = map.get(name)
    grade match {
      case Some(a) => println("grade is :" + a)
      case None => println("cannot get")
    }


  }

  def main(args: Array[String]): Unit = {
    jundgeGrade("jack")
  }

}
