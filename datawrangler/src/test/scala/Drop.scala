import main.scala._
import org.scalatest._

class Drop extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"2x3_test.csv", "\n", ",")

  val dropped = Array(Array("1", "2"), Array("5", "6"))

  test ("Column 1 is deleted") {
    dw.drop(column=1)
    dw.table.zip(dropped).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  }
}
