import mx.cinvestav.statusx.BullyStatus
import mx.cinvestav.commons.status
import cats.implicits._
import cats.effect._
import fs2.Stream
import fs2.concurrent.SignallingRef
import scala.concurrent.duration._
import scala.language.postfixOps
class BasicSpec extends munit.CatsEffectSuite {

  test("Basics"){
    for {
      s <- SignallingRef[IO,Boolean](false)
      ss = Stream.iterate(0)(_+1)
        .covary[IO]
        .metered(.5 second)
        .map(x=>s"GOLA[$x]")
        .debug()
        .pauseWhen(s)
      _ <- ss.compile.drain.start
      _ <- IO.sleep(2 seconds)
      _ <- s.set(true)
      _ <- IO.println("PAUSED")
      _ <- IO.sleep(5 seconds)
      _ <- s.set(false)
      _ <- IO.sleep(100 seconds)
    } yield ()
//      .pauseWhen()
//    assert(status.Up == status.Up)
  }

}
