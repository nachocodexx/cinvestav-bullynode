package mx.cinvestav.domain

object Payloads {

  case class RemoveNode(nodeId:String)
  case class Election(nodeId:String,shadowNodeId:String)
  case class Ok(id:Int,nodeId:String)
  case class Coordinator(nodeId:String,shadowNodeId:String)

}
