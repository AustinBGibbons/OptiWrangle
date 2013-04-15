import main.scala._
import org.scalatest._

class Translate extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"2x3_test.csv", "\n", ",")

  val trans = Array(Array("", "", "1","2"), Array("", "", "3", "4"), Array("", "", "5", "6"))

  test ("Table is translated") {
    dw.translate(distance=2)
    dw.table.zip(trans).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  }

  // left right up down columns etc.
}
