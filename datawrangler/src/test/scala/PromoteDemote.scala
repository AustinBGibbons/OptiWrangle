import main.scala._
import org.scalatest._

class PromoteDemote extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"promotedemote_test.csv", "\n", ",")

  test ("Hello World is promoted") {
    dw.promote(1)
    assert(dw.header(0) == "Hello")
    assert(dw.header(1) == "World")
  }

  test ("Header is demoted") {
    dw.demote(1)
    assert(dw.header == null)
    assert(dw.table(0)(1) == "Hello")
    assert(dw.table(1)(1) == "World")
  }

  test ("Header is promoted demoted and promoted") {
    dw.promote(row=0)
    assert(dw.header(0) == "Goodbye")
    assert(dw.header(1) == "Universe")
  }
}
