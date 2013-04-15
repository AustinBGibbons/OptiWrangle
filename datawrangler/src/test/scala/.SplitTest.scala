import main.scala._
import org.scalatest._

class SplitTest extends FlatSpec {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"split_test.csv", "\n", ",")

  dw.promote(0)
  
  dw.split(on=4)
  dw.split(on="3")

  dw.split(on="67?8?")
  dw.split(between=List(("y".r, "o")), max="all")

  println(dw)

  "split" should "remove a wide array of values" in {
    assert(dw.table.map(col => col.map(y => y == "okay" || y == "").reduce(_ && _)).reduce(_ && _))
  }
}
