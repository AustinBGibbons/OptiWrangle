import main.scala._
import org.scalatest._

class Delete extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"2x3_test.csv", "\n", ",")

  val deleted = Array(Array("1"), Array("3") , Array("5"))

  test ("Rows with '.2'.r are deleted") {
    dw.delete(column=1, where=".2".r)
    dw.table.zip(deleted).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  }
}
