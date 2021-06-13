package mx.cinvestav.statusx
import mx.cinvestav.commons.status

object BullyStatus {
  case object Election extends status.Status {
    override def value: Int = 4
    override def toString = "ELECTION"
  }
  case object Ok extends status.Status {
    override def value: Int = 5
    override def toString: String = "OK"
  }
  case object Coordinator extends status.Status {
    override def value: Int = 6
    override def toString: String = "COORDINATOR"
  }
}
