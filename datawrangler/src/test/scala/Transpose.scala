import main.scala._
import org.scalatest._

class Transpose extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"2x3_test.csv", "\n", ",")

  val trans = Array(Array("1","2"),Array("3","4"), Array("5","6"))
  val orig = Array(Array("1","2","3"), Array("4","5","6"))

  test ("Table is transposed") {
    dw.transpose
    dw.table.zip(trans).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  }

  test ("Table is back to original") {
    dw.transpose
    dw.table.zip(trans).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  }
}
