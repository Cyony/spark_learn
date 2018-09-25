package poc.actor

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import scala.collection.mutable
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

/**
  * Created by shiyang on 2018/3/20.
  */
class Master(val host: String, val port: Int) extends Actor {

  //定义一个HashMap用来存储注册Worker的信息
  val idToWorkerHashMap = new mutable.HashMap[String, WorkerInfo]()
  //定义一个HashSet用于存储 注册的Worker对象 WorkerInfo
  val workersSet = new mutable.HashSet[WorkerInfo]() //使用set删除快, 也可用linkList
  //超时检查的间隔  这个时间间隔一定要大于Worker向Master发送心跳的时间间隔.
  val CHECK_INTERVAL = 15000

  override def preStart(): Unit = {
    println("preStart invoked")
    //导入隐式转换
    //使用timer太low了,可以使用akka的定时器,需要导入这个包
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, CheckTimeOutWorker)
  }

  //接收Worker发送来的消息  可以有注册RegisterWorker 心跳Heartbeat
  override def receive: Receive = {
    case RegisterWorker(id, memory, cores) => {
      //判断该Worker是否已经注册过
      if (!idToWorkerHashMap.contains(id)) {
        //把Worker的信息封装起来保存到内存中
        val workerInfo = new WorkerInfo(id, memory, cores)
        //以id为键  worker对象为值 添加到该idToWorker的HashMap对象中
        idToWorkerHashMap(id) = workerInfo
        //把workerInfo对象放到存储Woker的HashSet中
        workersSet += workerInfo
        //对注册的Worker存储注册之后需要向Worker发送消息 来说明该Woker注册成功
        sender ! RegisteredWorker(s"akka.tcp://MasterSystem@$host:$port/user/Master") //通知worker注册
      }
    }
    //接收已经在Master注册过的Worker定时发送来的心跳
    case Heartbeat(id) => {
      if (idToWorkerHashMap.contains(id)) {
        val workerInfo = idToWorkerHashMap(id)
        //报活  把该Worker给Master心跳的时间记录下来
        val currentTime = System.currentTimeMillis()
        workerInfo.lastHeartbeatTime = currentTime
      }
    }
    //Master节点主动检测在Master自己这里注册Worker节点的存活状态
    case CheckTimeOutWorker => {
      val currentTime = System.currentTimeMillis();
      //如果当前时间和最近一次心跳时间的差值大于检测的时间间隔 就要干掉这个Worker
      val toRemove = workersSet.filter(x => currentTime - x.lastHeartbeatTime > CHECK_INTERVAL)
      for (w <- toRemove) {
        workersSet -= w
        idToWorkerHashMap -= w.id
      }
      println(workersSet.size)
    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    //准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
        """.stripMargin
    //ConfigFactory 既有parseString 也有parseFile
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem是所有Actor的老大,辅助创建和监控下面的所有Actor,它是单例的
    val actorSystem = ActorSystem("MasterSystem", config)
    val master = actorSystem.actorOf(Props(new Master(host, port)), "Master")
    actorSystem.terminate()
  }
}
