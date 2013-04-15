import main.scala._
import org.scalatest._

class Edit extends FunSuite {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"2x3_test.csv", "\n", ",")

  val edited = Array(Array("1", "2"), Array("4", "4"), Array("4", "4"))

  test ("Table is edited") {
    dw.edit(column=List(1,2), value = "4")
    dw.table.zip(edited).foreach{x => x._1.zip(x._2).foreach(y => assert(y._1 == y._2))}
  } 

  // todo: directions...
}
