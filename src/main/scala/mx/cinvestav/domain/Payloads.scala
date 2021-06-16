package mx.cinvestav.domain

import io.circe.Json
import mx.cinvestav.commons.commands.CommandData

object Payloads {

  case class RemoveNode(nodeId:String)
  case class Election(nodeId:String,shadowNodeId:String)
  case class Ok(nodeId:String)
  case class Coordinator(nodeId:String,shadowNodeId:String)
  case class RunAll(commands:List[CommandData[Json]])
  case class Run(command:CommandData[Json])

}
