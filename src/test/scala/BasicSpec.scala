import mx.cinvestav.statusx.BullyStatus
import mx.cinvestav.commons.status
class BasicSpec extends munit.CatsEffectSuite {

  test("Basics"){
    assert(status.Up == status.Up)
  }

}
