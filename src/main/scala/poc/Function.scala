package poc

import org.apache.spark.SparkContext

/**
  * Created by shiyang on 2018/2/16.
  */
object Function {

  //匿名函数
  val func1 = (name: String) => println("I am:" + name)

  val sayHello = (name: String) => println("my name is:" + name)

  //无返回高阶函数
  def func2(fun: (String) => Unit, name: String) {
    fun(name)
  }


  //有返回高阶函数
  def func3(msg: String) = (name: String) => println("msg is:" + msg + ",name is:" + name)


  //==currying函数
  def introduce(age: Int)(name: String) = println("my name is:" + name + ",my age is:" + age)

  //类型推断
  def triple(fun: (Int) => Int, num: Int) = {
    fun(num)
  }

  def sayHello(age: Int)(implicit name: String) = println("my name is:" + name + ",my age is:" + age)

  implicit def int2Range(num: Int): Range = 1 to num

  def spreadNum(range: Range): String = range.mkString(",")


  class SuperMan(val name: String) {
    def fly = println("you can fly")
  }

  //implicit def man2SuperMan(man: Man): SuperMan = new SuperMan(man.name)


  def main(args: Array[String]): Unit = {

   // spreadNum(5)

    //    implicit val name = "cyony"
    //    implicit val name1 = "cyony1"
    //    sayHello(25)

  }

}
