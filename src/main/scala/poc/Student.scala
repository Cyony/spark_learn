package poc

/**
  * Created by shiyang on 2018/3/20.
  */
class Student private(var name: String) {

  var age: Int = _
  var clazz: String = Student.clazz

  private def this(name: String, age: Int) {
    this(name)
    this.age = age
  }

  override def toString: String = "My name is " + name + " and age is " + age + ". I am from " + clazz

}

object Student {
  var instance: Student = null
  var clazz = "class1"


  def apply(name: String, age: Int) = {
    if (instance == null) instance = new Student(name, age)
    instance
  }

}




