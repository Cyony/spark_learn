package poc.actor

/**
  * Created by shiyang on 2018/3/20.
  */

import java.util.UUID
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorSelection
import scala.concurrent.duration._ //如果不引入这个包 在 context.system.scheduler.schedule 中的mills 地方会报错

//使用Akka框架来实现RCP不同进程之间的方法调用   导入Actor是导入的akka包下的
//在Worker类上定义一个主构造器
class Worker(val masterHost: String, val masterPort: Int, val memory: Int, val cores: Int) extends Actor {
  var master: ActorSelection = _
  //记录Worker从节点的id
  val workerId = UUID.randomUUID().toString()
  //记录Worker从节点的发送心跳的时间间隔
  val HEART_INTERVAL = 10000

  //preStart()方法在构造器之后,在receive之前执行  actorOf()方法执行 就会执行生命周期方法
  override def preStart(): Unit = {
    //跟Master建立连接
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
    //向Master发送注册消息
    master ! RegisterWorker(workerId, memory, cores)
  }

  override def receive: Receive = {
    //Master对注册的Worker存储注册之后需要向Worker发送消息 来说明该Woker注册成功
    //此处是Worker节点接收到Master节点的反馈消息
    case RegisteredWorker(masterUrl) => {
      println(masterUrl)
      //启动定时器发送心跳
      import context.dispatcher
      //多长时间后执行 单位 ,多长时间执行一次 单位  ,消息的接受者(直接给master发不好,先给自己发送消息,以后可以做下判断,什么情况下再发送消息), 消息
      context.system.scheduler.schedule(0 millis, HEART_INTERVAL millis, self, SendHeartbeat)
    }
    //Worker先向自己self发送一下心跳,这个地方可以做一下判断,根据实际情况,定义什么时候向Master发送消息
    case SendHeartbeat => {
      println("send heartbeat to Master")
      master ! Heartbeat(workerId)
    }
  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    //从传入的参数列表中取得
    val host = args(0)
    val port = args(1).toInt
    val masterHost = args(2)
    val masterPort = args(3).toInt
    val memory = args(4).toInt
    val cores = args(5).toInt
    //为了通过ActorSystem老大来获得Actor  准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
        """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem是所有Actor的老大,辅助创建和监控下面的Actor, 该老大是单例的.
    //得到该老大的对象需要一个名字和配置对象config
    val actorSystem = ActorSystem("WorkerSystem", config)
    //actorOf方法执行,就会执行生命周期方法
    actorSystem.actorOf(Props(new Worker(masterHost, masterPort, memory, cores)), "Worker")
    actorSystem.terminate()
  }
}
