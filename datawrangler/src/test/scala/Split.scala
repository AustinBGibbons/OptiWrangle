import main.scala._
import org.scalatest._

class Split extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"easy_split_test.csv", "\n", ",")

  val split = Array(Array("1", "5"), Array("2", "6"), Array("3", "7"), Array("4", "8"))

  test ("Table is split") { 
    dw.split(after="1", before="2")
    dw.split(on=List("i", "GH"), max="all")
    dw.merge(column=(1,2))
    dw.table.zip(split).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  } 
}
