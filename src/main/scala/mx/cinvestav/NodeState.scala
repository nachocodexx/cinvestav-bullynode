package mx.cinvestav
import cats.effect.IO
import cats.effect.std.Queue
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.BullyNode

case class NodeState(
                      priority:Int,
                      node:String,
                      bullyNodes:List[BullyNode],
                      heartBeatCounter:Int,
                      retries:Int,
                      isLeader:Boolean,
                      status:Status,
                      leader:String,
                      shadowLeader:String,
                      okMessages:List[String]= Nil,
                      nodes:List[String],
                      //                    Experimental
                      electionSignal:SignallingRef[IO,Boolean],
//                    Monitoring leader
                      leaderSignal:SignallingRef[IO,Boolean],
//                    Monitoring health check
                      healthCheckSignal:SignallingRef[IO,Boolean],
//
                      maxRetriesQueue:Queue[IO,Option[Int]],
//                    Coordinator signal to skip unnecessary election
                      coordinatorSignal:SignallingRef[IO,Boolean]
                    )
