package mx.cinvestav

import cats.implicits._
import cats.effect.{IO, Ref}
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Payloads
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import mx.cinvestav.commons.status
import org.typelevel.log4cats.Logger

object CommandHanlders {


  implicit val heartbeatDecoder: Decoder[payloads.HeartbeatPayload] = deriveDecoder[payloads.HeartbeatPayload]
  implicit val removeNodeDecoder:Decoder[Payloads.RemoveNode] = deriveDecoder
  implicit val electionsDecoder:Decoder[Payloads.Election] = deriveDecoder

  def coordinator(command: Command[Json],state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO],
                                                                  config:DefaultConfig,logger: Logger[IO]):IO[Unit] = command.payload.as[Payloads.Coordinator] match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        for {
          _            <- IO.println(s"NEW COORDINATOR -> ${payload.shadowNodeId} - ${payload.nodeId}")
          _ <- state.update(s=>s.copy(status = status.Up,leader = payload.nodeId,shadowLeader = payload.shadowNodeId,isLeader = false))
//          _  <- currentState.leaderSignal.set(true) *> currentState.leaderSignal.set(false)
//          _ <- Helpers
//            .monitoringLeaderNodeS(s"${config.poolId}-${payload.shadowNodeId}.default",state,currentState.leaderSignal)
//            .compile
//            .drain.start
        } yield ()
    }


  def ok(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config:DefaultConfig): IO[Unit] =
    command.payload.as[Payloads.Ok] match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        state.update(s=>s.copy(okMessages = s.okMessages.filter(_ != payload.id)))
    }
  def elections(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],
                                                                config:DefaultConfig,logger: Logger[IO]): IO[Unit] =
    command.payload.as[Payloads.Election] match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        for {
          _           <- IO.println(s"NEW ELECTIONS -> ${payload.shadowNodeId} - ${payload.nodeId}")
          okPublisher <- utils.createPublisher(config.poolId,s"${config.poolId}.${payload.shadowNodeId}.default")
          heartbeat   <- state.get.map(_.heartBeatCounter)
          okCommand   <- CommandData[Json](CommandsId.OK,Payloads.Ok(heartbeat,config.nodeId).asJson).pure[IO]
          _           <- okPublisher(okCommand.asJson.noSpaces)
          _           <- Helpers.election(state)
        } yield( )
    }

  def resetState(command: Command[Json],state:Ref[IO,NodeState])(implicit config:DefaultConfig): IO[Unit] =
    state.update(s=>
      NodeState(
        priority = config.priority,
        bullyNodes=config.bullyNodes,
        heartBeatCounter = 0,
        retries =  0,
        isLeader = config.isLeader,
        status = status.Up,
        leader =  config.leaderNode,
        electionSignal = s.electionSignal,
        leaderSignal =  s.leaderSignal,
        shadowLeader = config.leaderNode
      )
    )

  def removeNode(command:Command[Json],
                 state:Ref[IO,NodeState]):IO[Unit] = command.payload.as[Payloads.RemoveNode] match {
    case Left(e) =>
      IO.println(e.getMessage())
    case Right(payload) =>
      IO.println("REMOVED!")
  }



  def heartbeat(command:Command[Json],state:Ref[IO,NodeState])(implicit logger: Logger[IO]):IO[Unit] = {
    val payloadMaybe = command.payload.as[payloads.HeartbeatPayload]
    payloadMaybe match {
      case Left(value) =>
        IO.println(value.getMessage())
      case Right(_) =>
        state.update(s=>s.copy(heartBeatCounter = s.heartBeatCounter+1))
//          .flatMap(_=>IO.println("<3"))
          .flatMap(_=>Logger[IO].debug("<3"))
    }
  }

}
