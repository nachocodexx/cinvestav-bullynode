package mx.cinvestav

//
import mx.cinvestav.commons.commands.Identifiers
// Cats
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, MonadCancel, Ref}
import dev.profunktor.fs2rabbit.config.declaration.{Durable, NonAutoDelete, NonExclusive}
import fs2.concurrent.{Signal, SignallingRef}
import io.circe.Json
import mx.cinvestav.commons.commands.CommandData
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
// Fs2
import fs2.Stream
// RabbitMQ
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.ExchangeType
// PureConfig
import pureconfig._
import pureconfig.generic.auto._
// Circe
// Scala
import scala.concurrent.duration._
import scala.language.postfixOps
// Owns
import mx.cinvestav.commons.status
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.RabbitMQUtils

object Main extends IOApp{
  implicit val config: DefaultConfig           = ConfigSource.default.loadOrThrow[DefaultConfig]
  implicit val rabbitMqConfig: Fs2RabbitConfig = RabbitMQUtils.dynamicRabbitMQConfig(config.rabbitmq)
  implicit val unsafeLogger = Slf4jLogger.getLogger[IO]


  def monitoringNode(queueName:String,state:Ref[IO,NodeState],leaderSignal:SignallingRef[IO,Boolean])(implicit utils:RabbitMQUtils[IO]): IO[Unit]
  = {
    utils.consumeJson(queueName = queueName)
      .evalMap{ command=>  command.commandId match {
        case  Identifiers.HEARTBEAT=> CommandHanlders.heartbeat(command,state)
        }
      }
      .interruptWhen(leaderSignal)
      .compile.drain
  }


  def  program(queueName:String,state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO]) =
    utils.consumeJson(queueName)
    .evalMap{ command=> command.commandId  match {
      case Identifiers.REMOVE_NODE =>
        CommandHanlders.removeNode(command,state)
      case Identifiers.RESET_STATE =>
        CommandHanlders.resetState(command,state)
      case Identifiers.ELECTIONS =>
        CommandHanlders.elections(command,state)
      case Identifiers.OK =>
        CommandHanlders.ok(command,state)
      case Identifiers.COORDINATOR =>
        CommandHanlders.coordinator(command,state)
      case Identifiers.RUN =>
        CommandHanlders.run(command,state)
      case _ =>  state.get.map(_.status).flatMap{  s=>
        Logger[IO].debug("UKNOWN COMMAND")
      }
    }

    }

  def initState(signal:SignallingRef[IO,Boolean],leaderSignal:SignallingRef[IO,Boolean])(implicit config:DefaultConfig)
  : NodeState =
    NodeState(priority = config.priority,
      bullyNodes=config.bullyNodes,
      heartBeatCounter = 0,
      retries =  0,
      isLeader = config.isLeader,
      status = status.Up,
      leader =  config.leaderNode,
      electionSignal = signal,
      leaderSignal = leaderSignal,
      shadowLeader = config.shadowLeader
    )


  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.init[IO](rabbitMqConfig){ implicit utils =>
      for {
//        _    <- IOa.println(config)
        _  <- if(config.isLeader) Helpers.startLeaderHeartbeat() else IO.unit
        _  <- Logger[IO].debug(config.toString)
//        Init signals
        electionStatusSignal <- SignallingRef[IO,Boolean](false)
        leaderSignal         <- SignallingRef[IO,Boolean](false)
        _  <- Logger[IO].debug("Init signals [COMPLETED]")
//        Init state
        state <- IO.ref[NodeState](initState(signal = electionStatusSignal,leaderSignal = leaderSignal))
        _  <- Logger[IO].debug("Init state [COMPLETED]")
//     MAIN PROGRAM
        queueName <- s"${config.poolId}-${config.nodeId}".pure[IO]
        _     <- utils.createQueue(
          queueName =  queueName,
          exchangeName = config.poolId,
          exchangeType = ExchangeType.Topic,
          routingKey =  s"${config.poolId}.${config.nodeId}.default"
        )
//        _  <- utils.bindQueue(queueName,config.poolId,"#.config")
        _  <- utils.bindQueue(queueName,config.poolId,"shadow.#.config")
        _  <- program(queueName,state).compile.drain.start
        _  <- Logger[IO].debug("Main program is running successfully")
//      HEALTH CHECK
        _  <- Logger[IO].debug("Monitoring is running successfully")
        heartbeatQueue <- s"${config.poolId}-heartbeat".pure[IO]
        _   <- utils.declareQueue(heartbeatQueue,durable = Durable,exclusive = NonExclusive,autoDelete = NonAutoDelete)
        _   <- monitoringNode(heartbeatQueue,state,leaderSignal).start
        _   <- Helpers.healthCheck(state).compile.drain
//
      } yield ()
    }.as(ExitCode.Success)
  }
}
