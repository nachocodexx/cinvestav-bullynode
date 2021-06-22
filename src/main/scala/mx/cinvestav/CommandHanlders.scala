package mx.cinvestav

import cats.implicits._
import cats.effect.{IO, Ref}
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
import mx.cinvestav.Helpers.PublisherData
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads
import mx.cinvestav.config.{BullyNode, DefaultConfig}
import mx.cinvestav.domain.Payloads
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import mx.cinvestav.commons.status
import mx.cinvestav.statusx.BullyStatus
import mx.cinvestav.commons.commands.Identifiers
import org.typelevel.log4cats.Logger
import scala.concurrent.duration._
import scala.language.postfixOps

object CommandHanlders {


  implicit val heartbeatDecoder: Decoder[payloads.HeartbeatPayload] = deriveDecoder[payloads.HeartbeatPayload]
  implicit val removeNodeDecoder:Decoder[Payloads.RemoveNode] = deriveDecoder
  implicit val electionsDecoder:Decoder[Payloads.Election] = deriveDecoder

  def run(command: Command[Json],state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO])
  : IO[Unit] =
    command.payload.as[Payloads.Run] match {
      case Left(e) =>
        Logger[IO].error(e.getMessage())
      case Right(payload) => for {
        currentState <- state.get
        _            <- Logger[IO].debug(s"RUN ${payload.command.commandId} ${currentState.leader}")
//        bullyNode    <- currentState.bullyNodes.filter(_.nodeId == config.nodeId).head.pure[IO]
        bullyNode    <- BullyNode(currentState.leader,-1).pure[IO]
        pubData      <- Helpers.getPublisherData(bullyNode::Nil).map(_.head)
        _            <- pubData.publish(payload.command.asJson.noSpaces)
        _            <- Logger[IO].debug(s"${payload.command.commandId}_COMMAND_SENT ${bullyNode.nodeId}")
      } yield ()
//        IO.println(payload)
    }

  def coordinator(command: Command[Json],state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO],
                                                                  config:DefaultConfig,logger: Logger[IO]):IO[Unit] = command.payload.as[Payloads.Coordinator] match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        for {
          _           <- Logger[IO].debug(s"COORDINATOR ${payload.shadowNodeId} ${payload.nodeId}")
          currentState <- state.updateAndGet(s=>s.copy(
            status = status.Up,
            leader = payload.nodeId,
            shadowLeader = payload.shadowNodeId,
            isLeader = false,
            bullyNodes = s.bullyNodes.filter(_.nodeId!=s.shadowLeader),
            nodes = s.nodes.filter(_!=s.leader),
            okMessages = Nil,
            retries = 0
          )
          )
          _ <- currentState.coordinatorSignal.set(true)
          _ <- currentState.electionSignal.set(false)
          _ <- currentState.healthCheckSignal.set(true)
          _ <- Helpers.enableMonitoring(state)
          coordWin = Logger[IO].info("COORDINATOR_WINDOW_TIME_START")
          coordWinEnd = Logger[IO].info("COORDINATOR_WINDOW_TIME_FINISH")
          _ <- (coordWin*>IO.sleep(config.coordinatorWindow milliseconds) *> currentState.coordinatorSignal.set
          (false) *> coordWinEnd).start
//            _ <- currentState.healthCheckSignal.set(false)
        } yield ()
    }

//  OK

  def ok(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config:DefaultConfig,
                                                         logger: Logger[IO])
  : IO[Unit] =
    command.payload.as[payloads.Ok] match {
      case Left(e) =>
        Logger[IO].error(e.getMessage())
      case Right(payload) => for {
        _            <- Logger[IO].debug(Identifiers.OK+s" ${payload.nodeId}")
        currentState <- state.updateAndGet(s=>s.copy(okMessages = s.okMessages.filter(_ != payload.nodeId)))
        _            <- currentState.electionSignal.set(true)
//        _            <- currentState.
      } yield ()
    }

// ELECTIONS
  def elections(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],
                                                                config:DefaultConfig,logger: Logger[IO]): IO[Unit] = {

    def sendOk(currentPeer:List[PublisherData]) = for {
//      def sendOk(pubsData:List[PublisherData],payload:Payloads.Election) = for {
//      currentPeer <- pubsData.filter(_.nodeId == payload.shadowNodeId).head.pure[IO]
      okCommand   <- CommandData[Json](Identifiers.OK,Payloads.Ok(config.nodeId).asJson).pure[IO]
      _           <- Helpers.sendCommand(currentPeer,okCommand)
//      _           <- Helpers.sendCommand(currentPeer::Nil,okCommand)
    } yield ()
    command.payload.as[payloads.Election] match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        for {
//          _             <
          currentState     <- state.get
          isCoordWinActive <- currentState.coordinatorSignal.get
//          bullyNodes    <- currentState.bullyNodes.pure[IO]
//          pubsData      <- Helpers.getPublisherData(bullyNodes,bully => Map("priority"->bully.priority.toString))
          pubsData <- Helpers.getPublisherData_(payload.shadowNodeId::Nil)
//          No check if exists
          _ <- sendOk(pubsData)
          _ <- if(currentState.status == BullyStatus.Election || isCoordWinActive)
                    Logger[IO].debug(s"SKIP_ELECTION ${payload.shadowNodeId} ${payload.nodeId}")
               else for {
                   _ <-Logger[IO].debug(s"ELECTION ${payload.shadowNodeId} ${payload.nodeId}")
                   //          STOP HEALTH CHECK & ELECTION PROCESS
                   _ <- currentState.healthCheckSignal.set(true)
                   _  <- currentState.electionSignal.set(true)
                   _ <-Helpers.election(state)
                } yield ()
        } yield( )
    }
  }


  def heartbeat(command:Command[Json],state:Ref[IO,NodeState])(implicit logger: Logger[IO]):IO[Unit] = {
    val payloadMaybe = command.payload.as[payloads.HeartbeatPayload]
    payloadMaybe match {
      case Left(value) =>
        IO.println(value.getMessage())
      case Right(payload) =>
        state.update(s=>s.copy(heartBeatCounter = s.heartBeatCounter+1,retries = 0))
          .flatMap(_=>Logger[IO].info(s"HEARTBEAT ${payload.value} ${payload.nodeId}"))
    }
  }

}
