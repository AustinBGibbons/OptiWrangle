import main.scala._
import org.scalatest._

class Wrap extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"wrap_test.csv", "\n", ",")

  val wrapped = Array(Array("1", "2", "2"), Array("", "3", "4"), Array("", "", "5"))

  test ("Table is wraped") {
    dw.wrap(where="2")
    dw.table.zip(wrapped).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  } 

  // todo: directions...
}
