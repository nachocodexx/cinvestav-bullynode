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
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Payloads
import mx.cinvestav.commons.status
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import mx.cinvestav.commons.commands.Identifiers

object Helpers {
  def startLeaderHeartbeat()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig): IO[Unit] = for {
    _   <- IO.println(s"START HEARTBEAT[${config.node}]")
    pub <- utils.createPublisher(config.poolId,s"${config.poolId}.${config.node}.default")
    cmd <- CommandData[Json](Identifiers.START_HEARTBEAT,Json.Null).asJson.noSpaces.pure[IO]
    _   <- pub(cmd)
  } yield ()

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
  def createBullyNode(nodeId:String)(implicit utils:RabbitMQUtils[IO]): IO[String => IO[Unit]] =
    utils.createPublisher(config.poolId,s"${config.poolId}.$nodeId.default")

  def healthCheck(state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config:DefaultConfig,logger: Logger[IO]): Stream[IO, (Int, FiniteDuration)] =
    Stream.awakeEvery[IO](5 seconds)
      .evalMapAccumulate(0){
        case (lastHeartbeats,x) =>
          for {
//            get current state
            currentState <- state.get
            retries      <- currentState.retries.pure[IO]
/*          (1)  If coordinator does not respond to it within a time interval T, then it is assumed that coordinator
                has failed and execute Helpers.maxRetriesExceeded.
 */
            _            <- if(config.maxRetries ==retries) Helpers.maxRetriesExceeded(state) else IO.unit
            hearbeats    <- currentState.heartBeatCounter.pure[IO]
            _            <-  if(hearbeats===lastHeartbeats) state.update(s=>s.copy(retries=s.retries+1)) else IO.unit
//            _            <- Logger[IO].debug(currentState.toString)
            _            <- Logger[IO].debug(s"LEADER => ${currentState.shadowLeader} - ${currentState.leader}")
//            _            <- IO.println(s"<3[LAST]: $lastHeartbeats - <3[CURRENT]: $hearbeats - Retries: $retries")
          } yield (hearbeats,x)
      }


  def sendCoordinatorCmd(lowest:List[(String, String=>IO[Unit])],state:Ref[IO,NodeState],_signal:SignallingRef[IO,Boolean])(implicit
                                                                                       utils: RabbitMQUtils[IO],
                                                                                      logger: Logger[IO])
  = {
    for {
      _              <- Logger[IO].debug(s"NOW IM THE F*CK LEADER!  =>  ${config.shadowLeader} - ${config.node}")
      previousState  <- state.getAndUpdate(_.copy(isLeader = true,status = status.Up,leader = config.node, shadowLeader = config.nodeId))
      //        Send stop heart command
      previousLeaderPub <-  utils.createPublisher(config.poolId,s"${config.poolId}.${previousState.leader}.default")
      stopCmd           <- CommandData[Json](Identifiers.STOP_HEARTBEAT,Json.Null).asJson.noSpaces.pure[IO]
      _                 <- previousLeaderPub(stopCmd)
      // Start heart in new leader node
      currentLeaderPub <-  utils.createPublisher(config.poolId,s"${config.poolId}.${config.node}.default")
      startCmd          <- CommandData[Json](Identifiers.START_HEARTBEAT,Json.Null).asJson.noSpaces.pure[IO]
      _                 <- currentLeaderPub(startCmd)
      //
      _              <- _signal.set(true)
      pubs           <- lowest.map(_._2).pure[IO]
      coordinatorCmd <- CommandData[Json](Identifiers.COORDINATOR,Payloads.Coordinator(config.node,config.nodeId).asJson)
        .asJson.noSpaces.pure[IO]
      _              <- pubs.traverse(p=>p(coordinatorCmd))
      _              <- Logger[IO].debug("SEND COORDINATOR COMMAND")

    } yield ()
  }
  def electionBackgroundTask(
                              state:Ref[IO,NodeState],
                              currentState:NodeState,
                              highest:List[(String,String=>IO[Unit])],
                              lowest:List[(String, String=>IO[Unit])]
                            )(implicit utils:RabbitMQUtils[IO],
                                                                       logger: Logger[IO]): IO[Unit]
  = {


    for {
    signal <- currentState.electionSignal.pure[IO]
    _      <- signal.set(false)
      _  <- Stream.awakeEvery[IO](1 seconds)
        .evalMapAccumulate(0){ (x,fd)=>
          for {
            currentState <- state.get
            _signal      <- currentState.electionSignal.pure[IO]
            okMessages   <- currentState.okMessages.pure[IO]
            _            <- if(x >= 15 && okMessages.nonEmpty) sendCoordinatorCmd(lowest,state,_signal) else IO.unit
            _            <- Logger[IO].debug(s"OK MESSAGES[Retries = $x, okMessages = ${okMessages.length}]! - ${highest.length}")
//            _            <- IO.println(okMessages)
            _            <- if(okMessages.isEmpty)  _signal.set(true) else IO.unit
          } yield (x+1,fd)
        }
        .interruptWhen(signal).compile.drain
  } yield ()
  }

  def election(state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config:DefaultConfig,logger: Logger[IO]): IO[Unit] = {
    def sendCmd(publishers:List[String=>IO[Unit]],commandData: CommandData[Json]) = for {
      res          <- publishers.traverse(p=>p(commandData.asJson.noSpaces))
    } yield ()
    def doBully(currentState:NodeState, highestIdPubs:List[(String,String=>IO[Unit])], lowestIdPubs:List[(String, String=>IO[Unit])]) = for {

      electionCmd  <- CommandData[Json](Identifiers.ELECTIONS,Payloads.Election(config.node,config.nodeId).asJson).pure[IO]
      //      SAVE OK MESSAGES
      //      _           <- state.update(s=>s.copy(okMessages = highestNodes.indices.toList))
      highestNodes <- highestIdPubs.map(_._1).pure[IO]
      highestPubs <- highestIdPubs.map(_._2).pure[IO]
      _           <- state.update(s=>s.copy(okMessages = highestNodes))
      //    Lowest nodes
      /*/
         Now process P sends election message to every process with high priority number.
         if high priority nodes list is empty
       */
      _  <- electionBackgroundTask(state,currentState,highestIdPubs,lowestIdPubs).start
      _  <- sendCmd(highestPubs,electionCmd)
      //      _            <- if(highestNodes.isEmpty) IO.println("SEND COORDINATOR") else sendCmd(highestPubs,electionCmd)
    } yield ()

    for {
      _            <- Logger[IO].debug("INIT ELECTIONS")
      currentState <- state.updateAndGet(_.copy(status= statusx.BullyStatus.Election))
      //   Highest nodes
      highestNodes <- currentState.bullyNodes.filter(_.priority>config.priority).map(_.nodeId).pure[IO]
      highestPubs  <- highestNodes.traverse(Helpers.createBullyNode)
      lowestNodes   <- currentState.bullyNodes
        .filter(_.priority < config.priority)
        .map(_.nodeId).pure[IO].map(_:+currentState.shadowLeader).map(_.toSet.toList)
      lowestPubs    <- lowestNodes.traverse(Helpers.createBullyNode)
      _            <- Logger[IO].debug(s"HIGHEST: $highestNodes")
      _            <- Logger[IO].debug(s"LOWEST: $lowestNodes")
      highestIdPubs <- highestNodes.zip(highestPubs).pure[IO]
      lowestIdPubs <- lowestNodes.zip(lowestPubs).pure[IO]
      _signal      <- state.get.map(_.electionSignal)

      _            <- if(highestNodes.isEmpty)
                      Helpers.sendCoordinatorCmd(lowestIdPubs,state,_signal)
                    else
                      doBully(currentState,highestIdPubs,lowestIdPubs)
    } yield ()
  }


  def maxRetriesExceeded(state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO],config:DefaultConfig,
                                                  logger: Logger[IO]): IO[Unit]
  =for {
    _          <- Logger[IO].debug("MAX RETRIES REACHED")
//    RESET RETRIES
    _          <- state.update(_.copy(retries = 0))
    _          <- Helpers.election(state)
//    isLeader   <- state.get.map(_.isLeader)
//    _           <- if(isLeader) Helpers.election(state) else IO.println("REMOVE NODE")
  } yield ()

}
