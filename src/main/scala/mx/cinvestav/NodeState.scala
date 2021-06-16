package mx.cinvestav
import cats.effect.IO
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.BullyNode

case class NodeState(
                      priority:Int,
                      bullyNodes:List[BullyNode],
                      heartBeatCounter:Int,
                      retries:Int,
                      isLeader:Boolean,
                      status:Status,
                      leader:String,
                      shadowLeader:String,
                      okMessages:List[String]= Nil,
                      //                    Experimental
                      electionSignal:SignallingRef[IO,Boolean],
                      leaderSignal:SignallingRef[IO,Boolean]
                    )
