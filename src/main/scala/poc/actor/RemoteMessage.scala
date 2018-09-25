package poc.actor

/**
  * Created by shiyang on 2018/3/20.
  */
//对象要序列化才能通过网络传输   这个地方没有大括号....这有这个extends声明
trait RemoteMessage extends Serializable

//Worker ->(发送给) Master  Worker给Master节点发送注册消息
case class RegisterWorker(id: String, memory: Int, scores: Int) extends RemoteMessage

//接收已经在Master注册过的Worker定时发送来的心跳  Woker->Master
case class Heartbeat(id: String) extends RemoteMessage

//Master -> Worker   Master反馈给Worker已经注册的Worker有哪些
case class RegisteredWorker(masterUrl: String) extends RemoteMessage

//Worker -> self  //Worker先向自己发送一下心跳,这个地方可以做一下判断,根据实际情况,定义什么时候向Master发送消息
//在这个样例类中发送给Master的心跳  case class Heartbeat
case object SendHeartbeat

//Master -> self  Master自检查看Worker的状态
case object CheckTimeOutWorker