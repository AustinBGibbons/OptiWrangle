import main.scala._
import org.scalatest._

class Fill extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"fill_test.csv", "\n", ",")

  val filled = Array(Array("1", "1", "4", "5", "5"), Array("2", "3", "3", "6", "6"))

  test ("Table is filled") {
    dw.fill()
    dw.table.zip(filled).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  }
}
