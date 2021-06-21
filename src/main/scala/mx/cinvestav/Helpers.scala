package mx.cinvestav

import cats.implicits._
import cats.effect.{IO, Ref}
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.circe.Json
import mx.cinvestav.Main.config
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.utils.RabbitMQUtils
import io.circe.syntax._
import io.circe.generic.auto._
import jdk.nashorn.internal.lookup.Lookup
import mx.cinvestav.config.{BullyNode, DefaultConfig}
//import mx.cinvestav.domain.Payloads
import mx.cinvestav.commons.{payloads=>Payloads}
import mx.cinvestav.commons.status
//import mx.cinvestav.commons.payloads
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import mx.cinvestav.commons.commands.Identifiers

import java.util.UUID

class Helpers()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]){

  def startLeaderHeartbeat(): IO[Unit] =
    for {
      pub <- utils.createPublisher(config.poolId,s"${config.poolId}.${config.node}.default")
      id <- UUID.randomUUID().toString.pure[IO]
      _ <- Logger[IO].debug(Identifiers.START_HEARTBEAT+s" $id ${config.node} ${config.nodeId}")
      payload <- Payloads.StartHeartbeat(id,config.node,config.nodeId).asJson.pure[IO]
      cmd <- CommandData[Json](Identifiers.START_HEARTBEAT,payload = payload).asJson.noSpaces.pure[IO]
      _   <- pub(cmd)
    } yield ()
}



object Helpers {
  type PublishFn = String => IO[Unit]
  case class PublisherData(nodeId:String,publish:PublishFn,metadata:Map[String,String]=Map.empty[String,String])

  def apply()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]): Helpers = {
    new Helpers()
  }



  def monitoringLeaderNodeS(queueName:String,state:Ref[IO,NodeState],leaderSignal:SignallingRef[IO,Boolean])(implicit
                                                                                                             utils:RabbitMQUtils[IO],logger:Logger[IO]): Stream[IO, Unit]
  = {
    utils.consumeJson(queueName = queueName)
      .evalMap{ command=>  command.commandId match {
        case  Identifiers.HEARTBEAT=> CommandHanlders.heartbeat(command,state)
      }
      }
      .interruptWhen(leaderSignal)
  }
//

  def healthCheck(state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config:DefaultConfig,logger: Logger[IO]): Stream[IO, (Int, FiniteDuration)] =
    Stream.awakeEvery[IO](config.healthCheckTime milliseconds)
      .evalMapAccumulate(0){
        case (lastHeartbeats,x) =>
          for {
//            get current state
            currentState <- state.get
            retries      <- currentState.retries.pure[IO]
/*          (1)  If coordinator does not respond to it within a time interval T, then it is assumed that coordinator
                has failed and execute Helpers.maxRetriesExceeded.
 */
            _            <- if(config.maxRetries == retries) Helpers.maxRetriesExceeded(state) else IO.unit
            hearbeats    <- currentState.heartBeatCounter.pure[IO]
            _            <-  if(hearbeats===lastHeartbeats) state.update(s=>s.copy(retries=s.retries+1)) else IO.unit
            _            <- Logger[IO].trace(s"HEALTH_CHECK ${currentState.shadowLeader} ${currentState.leader}")
          } yield (hearbeats,x)
      }


//  Send commands
  def sendCommand(publisherData: List[PublisherData], commandData: CommandData[Json])(implicit
                                                                                      utils: RabbitMQUtils[IO],
                                                                                      config: DefaultConfig,
                                                                                      logger: Logger[IO]): IO[Unit] =
    for {
//    _    <- Logger[IO].debug(s"SEND_COMMANDS,${publisherData.map(_.nodeId).mkString(",")}")
    pubs <- publisherData.map(_.publish).pure[IO]
      _  <- pubs.traverse(_(commandData.asJson.noSpaces))
  }  yield ()


  def sendStopHeartbeat(nodeId:String,fromNodeId:String)(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,
                                              logger: Logger[IO]): IO[Unit] = for {
    previousLeaderPub <-  utils.createPublisher(config.poolId,s"${config.poolId}.$nodeId.default")
    stopId            <- UUID.randomUUID().pure[IO].map(_.toString)
    _                 <- Logger[IO].debug(s"${Identifiers.STOP_HEARTBEAT} $stopId $nodeId $fromNodeId")
    stopCmd           <- CommandData[Json](Identifiers.STOP_HEARTBEAT,Payloads.StopHeartbeat(stopId,nodeId,fromNodeId).asJson).asJson.noSpaces.pure[IO]
    _                 <- previousLeaderPub(stopCmd)
  } yield ()

  def sendStartHeartbeat(nodeId:String,fromNodeId:String)(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,
                                              logger: Logger[IO]): IO[Unit] = for {
    currentLeaderPub  <-  utils.createPublisher(config.poolId,s"${config.poolId}.$nodeId.default")
    startId           <- UUID.randomUUID().toString.pure[IO]
    payload           <- Payloads.StartHeartbeat(startId,nodeId,fromNodeId).asJson.pure[IO]
    _                 <- Logger[IO].debug(s"${Identifiers.START_HEARTBEAT} $startId $nodeId $fromNodeId")
    startCmd          <- CommandData[Json](Identifiers.START_HEARTBEAT,payload).asJson.noSpaces.pure[IO]
    _                 <- currentLeaderPub(startCmd)
  } yield ( )

//

//  def sendCoordinatorCmd(lowest:List[(String, String=>IO[Unit])], state:Ref[IO,NodeState], electionSignal:SignallingRef[IO,Boolean])(implicit
  def sendCoordinatorCmd(lowest:List[PublisherData], state:Ref[IO,NodeState], electionSignal:SignallingRef[IO,Boolean])(implicit
                                                                                                                                     utils: RabbitMQUtils[IO],
                                                                                                                        config: DefaultConfig,
                                                                                                                                     logger: Logger[IO]): IO[Unit]
  = {
    for {
      _             <- Logger[IO].debug(s"NEW_COORDINATOR ${config.nodeId} ${config.node}")
      previousState <- state.getAndUpdate(s=>s.copy(
          isLeader = true,
          status = status.Up,
          leader = config.node,
          shadowLeader = config.nodeId,
          retries = 0,
          nodes = s.nodes.filter(_!=s.leader)
        )
      )
      //        Send stop heart command
      _             <- Helpers.sendStopHeartbeat(previousState.leader,config.nodeId)
      // Start heart in new leader node
      _             <- Helpers.sendStartHeartbeat(config.node,config.nodeId)
      nodes          <- previousState.nodes.filter(_!=previousState.leader).pure[IO]
      pubs           <- Helpers.getPublisherData_(nodes)
      newCoordinatorPayload <- Payloads.NewCoordinator(previousState.leader,config.node).asJson.pure[IO]
      newCoordinatorCmd <- CommandData[Json](Identifiers.NEW_COORDINATOR,newCoordinatorPayload).pure[IO]
      _                <- Helpers.sendCommand(pubs,newCoordinatorCmd)
//      _              <- pubs.traverse(_.publish(newCoordinatorCmd.asJson.noSpaces))
      // Stop the watcher of the election process
      _              <- electionSignal.set(true)
//    Enable monitoring
      _             <- previousState.healthCheckSignal.set(false)
//    Send coordinator command to all the lowest nodes
      coordinatorCmdPayload <- Payloads.Coordinator(config.node,config.nodeId).asJson.pure[IO]
      coordinatorCmd        <- CommandData[Json](Identifiers.COORDINATOR,coordinatorCmdPayload).pure[IO]
      _                     <- Helpers.sendCommand(lowest,coordinatorCmd)
      _              <- Logger[IO].debug(s"COORDINATOR_COMMAND_SENT ${lowest.length} ${lowest.map(_.nodeId).mkString(" ")}")

    } yield ()
  }
  def electionBackgroundTask(
                              state:Ref[IO,NodeState],
                              currentState:NodeState,
                              highest:List[PublisherData],
                              lowest:List[PublisherData]
                            )(implicit utils:RabbitMQUtils[IO],config: DefaultConfig,
                                                                       logger: Logger[IO]): IO[Unit]
  = {


    for {
      electionSignal <- currentState.electionSignal.pure[IO]
      _      <- electionSignal.set(false)
      _  <- Stream.awakeEvery[IO](1 seconds)
        .evalMapAccumulate(0){ (x,fd)=>
          for {
            currentState <- state.get
//            _signal      <- currentState.electionSignal.pure[IO]
            okMessages   <- currentState.okMessages.pure[IO]
            _            <- if(x >= config.maxRetriesOkMessages && okMessages.length === highest.length)
                                Helpers.sendCoordinatorCmd(lowest,state,electionSignal)
                            else IO.unit
            _            <- Logger[IO].debug(s"MONITOR_OK_MESSAGES $x ${okMessages.length}")
//            _            <- if(okMessages.isEmpty)  signal.set(true) else IO.unit
          } yield (x+1,fd)
        }
        .interruptWhen(electionSignal)
        .onComplete(Stream.emit("").evalMap(x=>Logger[IO].debug("ELECTION_FINISHED")))
        .compile.drain
  } yield ()
  }

  def createPubToDefault(nodeId:String)(implicit utils:RabbitMQUtils[IO]): IO[String => IO[Unit]] =
    utils.createPublisher(config.poolId,s"${config.poolId}.$nodeId.default")

  def _sendCmd(publishers:List[PublisherData],commandData: CommandData[Json]): IO[Unit] = for {
    _ <- publishers.map(_.publish).traverse(p=>p(commandData.asJson.noSpaces))
  } yield ()

  def bullyNodeToPubData(bullyNode: BullyNode,metadata:Map[String,String]=Map.empty[String,String])(implicit
                                                                                        utils: RabbitMQUtils[IO]):IO[PublisherData] = for {
   pub <- Helpers.createPubToDefault(bullyNode.nodeId)
    pubData <- PublisherData(bullyNode.nodeId,pub,metadata).pure[IO]
  } yield pubData
  def nodeToPubData(nodeId: String, metadata:Map[String,String]=Map.empty[String,String])(implicit
                                                                                          utils: RabbitMQUtils[IO]):IO[PublisherData] = for {
    pub <- Helpers.createPubToDefault(nodeId)
    pubData <- PublisherData(nodeId,pub,metadata).pure[IO]
  } yield pubData
  def getPublisherData_(nodes:List[String])(implicit utils: RabbitMQUtils[IO]): IO[List[PublisherData]] =
    for {
//      metadatas <- bullyNodes.map(metadataFn).pure[IO]
//      bullyMeta <- bullyNodes.zip(metadatas).pure[IO]
//      pubsData  <- bullyMeta.traverse(x=>bullyNodeToPubData(x._1,x._2))
      pubsData <- nodes.traverse(nodeToPubData(_))
    } yield pubsData

  def getPublisherData(bullyNodes:List[BullyNode],
                       metadataFn:BullyNode=>Map[String,String]=_=>Map.empty[String,String],
    predicate:BullyNode=>Boolean= _=>true)
  (implicit utils: RabbitMQUtils[IO]): IO[List[PublisherData]] =
    for {
      metadatas <- bullyNodes.map(metadataFn).pure[IO]
      bullyMeta <- bullyNodes.zip(metadatas).pure[IO]
      pubsData  <- bullyMeta.traverse(x=>bullyNodeToPubData(x._1,x._2))
  } yield pubsData

  def byPriority(pubData: PublisherData, predicate:Int=>Boolean ): Boolean = {
    val priority = pubData.metadata.getOrElse("priority","0").toInt
    predicate(priority)
  }

  def election(state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config:DefaultConfig,
  logger: Logger[IO])
  : IO[Unit] = {
      def doBully(currentState:NodeState, highestPubData:List[PublisherData], lowestPubData:List[PublisherData]) =
        for {

      electionCmd  <- CommandData[Json](Identifiers.ELECTIONS,Payloads.Election(config.node,config.nodeId).asJson).pure[IO]
      /*/
         Now process P sends election message to every process with high priority number.
         if high priority nodes list is empty
       */
      _  <- electionBackgroundTask(state,currentState,highestPubData,lowest = lowestPubData).start
//      _    <- Logger[IO].debug(s"ELECTION_COMMAND,${highestPubData.map(_.nodeId).mkString(",")}")
      _ <- Helpers.sendCommand(highestPubData,electionCmd)
      _    <- Logger[IO].debug(s"ELECTION_COMMAND_SENT ${highestPubData.length} ${highestPubData.map(_.nodeId).mkString(" ")}")
    } yield ()

    for {
//      _            <- Logger[IO].debug(s"INIT_ELECTION")
      currentState <- state.updateAndGet(_.copy(status= statusx.BullyStatus.Election))
      //   Bully nodes without the previous leader
      bullyNodes   <- currentState.bullyNodes.filter(_.nodeId!=currentState.shadowLeader).pure[IO]
      pubsData     <- getPublisherData(bullyNodes,bully=>Map("priority"->bully.priority.toString))
      higherNodes  <-  pubsData.filter(x=>byPriority(x,_>config.priority)).pure[IO]
      //      Lower
      lowerNodes   <- pubsData.filter(x=>byPriority(x,_<config.priority)).pure[IO]
      //
      electionSignal <- currentState.electionSignal.pure[IO]
      _             <- if(higherNodes.isEmpty)
                       Helpers.sendCoordinatorCmd(lowerNodes,state,electionSignal)
                    else
                      doBully(currentState,higherNodes,lowerNodes)
    } yield ()
  }


  def maxRetriesExceeded(state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO],config:DefaultConfig,
                                                  logger: Logger[IO]): IO[Unit]
  =for {
    _            <- Logger[IO].debug("MAX_RETRIES_REACHED")
//    RESET RETRIES
      currentState <- state.updateAndGet(_.copy(retries = 0))
     _             <- currentState.healthCheckSignal.set(true)
      _            <- if(currentState.isLeader)
                        Logger[IO].debug(
                          s"NO_ELECTION_CURRENT_LEADER ${currentState.shadowLeader} ${currentState.leader}"
                        )
                      else Helpers.election(state)
  } yield ()

}
