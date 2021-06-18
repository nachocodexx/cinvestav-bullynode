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
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import mx.cinvestav.commons.commands.Identifiers

import java.util.UUID

class Helpers()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]){

  def startLeaderHeartbeat(): IO[Unit] =
    for {
      //    _   <- IO.println(s"START HEARTBEAT[${config.node}]")
      _ <- Logger[IO].debug(Identifiers.START_HEARTBEAT)
      pub <- utils.createPublisher(config.poolId,s"${config.poolId}.${config.node}.default")
      id <- UUID.randomUUID().toString.pure[IO]
      cmd <- CommandData[Json](Identifiers.START_HEARTBEAT,payload = Payloads.StartHeartbeat(id,config.node).asJson)
        .asJson
        .noSpaces
        .pure[IO]
      _   <- pub(cmd)
    } yield ()
}

object Helpers {
  type PublishFn = String => IO[Unit]
  case class PublisherData(nodeId:String,publish:PublishFn,metadata:Map[String,String]=Map.empty[String,String])

  def apply()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]) = {
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
//            _            <- Logger[IO].debug(currentState.toString)
            _            <- Logger[IO].debug(s"HEALTH_CHECK,${currentState.shadowLeader},${currentState.leader}")
//            _            <- IO.println(s"<3[LAST]: $lastHeartbeats - <3[CURRENT]: $hearbeats - Retries: $retries")
          } yield (hearbeats,x)
      }


//  Send commands
  def sendCommad(publisherData: List[PublisherData],commandData: CommandData[Json])(implicit
                                                                                    utils: RabbitMQUtils[IO],
                                                                                    config: DefaultConfig,
                                                                                    logger: Logger[IO]): IO[Unit] =
    for {
    _    <- Logger[IO].debug(s"SEND_COMMANDS,${publisherData.map(_.nodeId).mkString(",")}")
    pubs <- publisherData.map(_.publish).pure[IO]
      _  <- pubs.traverse(_(commandData.asJson.noSpaces))
  }  yield ()
  def sendStopHeartbeat(nodeId:String)(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]): IO[Unit] = for {
    previousLeaderPub <-  utils.createPublisher(config.poolId,s"${config.poolId}.$nodeId.default")
    stopId            <- UUID.randomUUID().pure[IO].map(_.toString)
    _                 <- Logger[IO].debug(s"${Identifiers.STOP_HEARTBEAT},$stopId")
    stopCmd           <- CommandData[Json](Identifiers.STOP_HEARTBEAT,Payloads.StopHeartbeat(stopId,nodeId).asJson).asJson.noSpaces.pure[IO]
    _                 <- previousLeaderPub(stopCmd)
  } yield ()

  def sendStartHeartbeat(nodeId:String)(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]): IO[Unit] = for {
    currentLeaderPub  <-  utils.createPublisher(config.poolId,s"${config.poolId}.$nodeId.default")
    startId           <- UUID.randomUUID().toString.pure[IO]
    payload           <- Payloads.StartHeartbeat(startId,nodeId).asJson.pure[IO]
    _                 <- Logger[IO].debug(s"${Identifiers.START_HEARTBEAT},$startId")
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
      _             <- Logger[IO].debug(s"NEW_COORDINATOR,${config.shadowLeader},${config.node}")
      previousState <- state.getAndUpdate(_.copy(isLeader = true,status = status.Up,leader = config.node, shadowLeader = config.nodeId))
      //        Send stop heart command
      _             <- Helpers.sendStopHeartbeat(previousState.leader)
      // Start heart in new leader node
      _             <- Helpers.sendStartHeartbeat(config.node)
      // Stop the watcher of the election process
      _              <- electionSignal.set(true)
//    Send coordinator command to all the lowest nodes
      coordinatorCmdPayload <- Payloads.Coordinator(config.node,config.nodeId).asJson.pure[IO]
      coordinatorCmd        <- CommandData[Json](Identifiers.COORDINATOR,coordinatorCmdPayload).pure[IO]
      _                     <- Helpers.sendCommad(lowest,coordinatorCmd)
      _              <- Logger[IO].debug(s"COORDINATOR_COMMAND_SENT,${lowest.map(_.nodeId).mkString(",")}")

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
    signal <- currentState.electionSignal.pure[IO]
    _      <- signal.set(false)
      _  <- Stream.awakeEvery[IO](1 seconds)
        .evalMapAccumulate(0){ (x,fd)=>
          for {
            currentState <- state.get
//            _signal      <- currentState.electionSignal.pure[IO]
            okMessages   <- currentState.okMessages.pure[IO]
            _            <- if(x == config.maxRetriesOkMessages && okMessages.length === highest.length)
                                sendCoordinatorCmd(lowest,state,signal)
                            else IO.unit
            _            <- Logger[IO].debug(s"MONITOR_OK_MESSAGES,$x,${okMessages.length}")
            _            <- if(okMessages.isEmpty)  signal.set(true) else IO.unit
          } yield (x+1,fd)
        }
        .interruptWhen(signal)
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


  def getPublisherData(bullyNodes:List[BullyNode],
                       metadataFn:BullyNode=>Map[String,String],
    predicate:BullyNode=>Boolean= _=>true)
  (implicit utils: RabbitMQUtils[IO]): IO[List[PublisherData]] =
    for {
      metadatas <- bullyNodes.map(metadataFn).pure[IO]
      bullyMeta <- bullyNodes.zip(metadatas).pure[IO]
      pubsData  <- bullyMeta.traverse(x=>bullyNodeToPubData(x._1,x._2))
  } yield pubsData

  def byPriority(pubData: PublisherData, predicate:Int=>Boolean ) = {
    val priority = pubData.metadata.getOrElse("priority","0").toInt
    predicate(priority)
  }

  def election(state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config:DefaultConfig,logger: Logger[IO]): IO[Unit] = {
//    def sendCmd(publishers:List[String=>IO[Unit]],commandData: CommandData[Json]) = for {
//      res          <- publishers.traverse(p=>p(commandData.asJson.noSpaces))
//    } yield ()
//    def doBully(currentState:NodeState, highestIdPubs:List[(String,String=>IO[Unit])], lowestIdPubs:List[(String, String=>IO[Unit])]) = for {
      def doBully(currentState:NodeState, highestPubData:List[PublisherData], lowestPubData:List[PublisherData]) =
        for {

      electionCmd  <- CommandData[Json](Identifiers.ELECTIONS,Payloads.Election(config.node,config.nodeId).asJson).pure[IO]
      //      SAVE OK MESSAGES
      //      _           <- state.update(s=>s.copy(okMessages = highestNodes.indices.toList))
      highestNodes <- highestPubData.map(_.nodeId).pure[IO]
//      highestPubs <- highestPubData.map(_.publish).pure[IO]
      _           <- state.update(s=>s.copy(okMessages = highestNodes))
      //    Lowest nodes
      /*/
         Now process P sends election message to every process with high priority number.
         if high priority nodes list is empty
       */
      _  <- electionBackgroundTask(state,currentState,highestPubData,lowest = lowestPubData).start
          _ <- Helpers.sendCommad(highestPubData,electionCmd)
//      _  <- sendCmd(highestPubs,electionCmd)
      //      _            <- if(highestNodes.isEmpty) IO.println("SEND COORDINATOR") else sendCmd(highestPubs,electionCmd)
    } yield ()

    for {
      _            <- Logger[IO].debug(s"INIT_ELECTION")
      currentState <- state.updateAndGet(_.copy(status= statusx.BullyStatus.Election))
      //   Highest nodes
      bullyNodes   <- currentState.bullyNodes.pure[IO]
      pubsData     <- getPublisherData(bullyNodes,bully=>Map("priority"->bully.priority.toString))
      higherNodes  <-  pubsData.filter(byPriority(_,_>config.priority)).pure[IO]
      //      Lower
      lowerNodes   <- pubsData.filter(byPriority(_,_<config.priority)).pure[IO]
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
    _          <- Logger[IO].debug("MAX_RETRIES_REACHED")
//    RESET RETRIES
    _          <- state.update(_.copy(retries = 0))
    _          <- Helpers.election(state)
//    isLeader   <- state.get.map(_.isLeader)
//    _           <- if(isLeader) Helpers.election(state) else IO.println("REMOVE NODE")
  } yield ()

}
