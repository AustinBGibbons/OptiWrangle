import main.scala._
import org.scalatest._

class Merge extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"2x3_test.csv", "\n", ",")

  val merged = Array(Array("1+3", "2+4"), Array("5", "6"))

  test ("Table is merged") {
    dw.merge(column= (0, 1), glue="+")
    dw.table.zip(merged).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  }
}
