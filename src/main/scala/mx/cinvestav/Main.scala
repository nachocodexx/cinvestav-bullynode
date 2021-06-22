package mx.cinvestav

//
import cats.effect.std.Queue
import dev.profunktor.fs2rabbit.config.declaration.{AutoDelete, NonDurable}
import mx.cinvestav.commons.commands.Identifiers
import org.typelevel.log4cats.SelfAwareStructuredLogger
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
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]


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


  def  program(queueName:String,state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO]): Stream[IO, Unit] =
    utils.consumeJson(queueName)
    .evalMap{ command=> command.commandId  match {
      case Identifiers.ELECTIONS =>
        CommandHanlders.elections(command,state)
      case Identifiers.OK =>
        CommandHanlders.ok(command,state)
      case Identifiers.COORDINATOR =>
        CommandHanlders.coordinator(command,state)
      case Identifiers.RUN =>
        CommandHanlders.run(command,state)
      case _ =>  state.get.flatMap{  s=>
        Logger[IO].debug("UNKNOWN COMMAND") *> Logger[IO].debug(s.toString) *> s.healthCheckSignal.set(true)
      }
    }

    }


  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.init[IO](rabbitMqConfig){ implicit utils =>
      for {
        _       <- utils.createExchange(config.poolId,ExchangeType.Topic,NonDurable,AutoDelete)
        helpers <- Helpers().pure[IO]
        queue   <- Queue.unbounded[IO,Option[Int]]
//
        _       <- if(config.isLeader) helpers.startLeaderHeartbeat() else IO.unit

        _       <- Logger[IO].trace(config.toString)
//        Init signals
        electionStatusSignal <- SignallingRef[IO,Boolean](false)
        healthCheckSignal    <- SignallingRef[IO,Boolean](false)
        leaderSignal         <- SignallingRef[IO,Boolean](false)
        coordinatorSignal   <-SignallingRef[IO,Boolean](false)
        _  <- Logger[IO].debug("INIT_SIGNALS")
//        Init state
        state <- IO.ref[NodeState](
          NodeState(priority = config.priority,
            bullyNodes=config.bullyNodes,
            heartBeatCounter = 0,
            retries =  0,
            isLeader = config.isLeader,
            status = status.Up,
            leader =  config.leaderNode,
            electionSignal = electionStatusSignal,
            leaderSignal = leaderSignal,
            shadowLeader = config.shadowLeader,
            node =  config.node,
            healthCheckSignal = healthCheckSignal,
            nodes = config.nodes,
            maxRetriesQueue = queue,
            coordinatorSignal = coordinatorSignal
        )
        )
        _  <- Logger[IO].debug("INIT_STATE")
//     MAIN PROGRAM
        queueName <- s"${config.poolId}-${config.nodeId}".pure[IO]
        _     <- utils.createQueue(
          queueName =  queueName,
          exchangeName = config.poolId,
          exchangeType = ExchangeType.Topic,
          routingKey =  s"${config.poolId}.${config.nodeId}.default"
        )
//        _  <- utils.bindQueue(queueName,config.poolId,"#.config")
//        _  <- utils.bindQueue(queueName,config.poolId,"shadow.#.config")
//      HEALTH CHECK
//        _  <- Logger[IO].debug("Monitoring is running successfully")
        heartbeatQueue <- s"${config.poolId}-heartbeat".pure[IO]
        _              <- utils.declareQueue(
          heartbeatQueue,
          durable = Durable,
          exclusive = NonExclusive,
          autoDelete = NonAutoDelete
        )
        _  <- Logger[IO].trace("MONITOR_NODE_START")
        _   <- monitoringNode(heartbeatQueue,state,leaderSignal).start
        _  <- Logger[IO].trace("HEALTH_CHECKER_START")
//        _   <- Helpers.healthCheck(state).pauseWhen(healthCheckSignal).compile.drain

        _     <- Stream.fromQueueNoneTerminated(queue).evalMap{ _=>
          for {
//            currentState <- state.get
//            isCoordWin   <- currentState.coordinatorSignal.get
//            _            <- if(isCoordWin) Logger[IO].info("ELECTION_IN_PROGRESS")
//            else
            _<-Helpers.maxRetriesExceeded(state)
          } yield ()
        }
          .compile.drain.start
        _   <- Helpers.healthCheck(state).interruptWhen(healthCheckSignal).compile.drain.start
//        _   <- Helpers.healthCheck(state).pauseWhen(healthCheckSignal).compile.drain.start
        _  <- Logger[IO].debug("START_NODE")
        _  <- program(queueName,state).compile.drain
//
      } yield ()
    }.as(ExitCode.Success)
  }
}
