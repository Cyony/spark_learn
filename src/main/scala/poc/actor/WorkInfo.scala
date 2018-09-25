package poc.actor

/**
  * Created by shiyang on 2018/3/20.
  */
class WorkerInfo(val id: String, val memory: Int, val scores: Int) {
  //记录上一次心跳
  var lastHeartbeatTime: Long = _
}
