package poc

import akka.actor._
import akka.actor.Actor._

/**
  * Created by shiyang on 2018/3/20.
  */
class MyActor extends Actor {
  override def receive: Receive = {
    case "hello" =>
      println("您好！")
    case _ => println("您是?")
  }
}

object MyActor {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("actor")
    val actor = system.actorOf(Props[MyActor], name = "actor")
    actor ! "hello"
  }
}