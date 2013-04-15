import main.scala._
import org.scalatest._

class Cut extends FlatSpec {
  val root = "/afs/cs.stanford.edu/u/gibbons4/optiwrangle/datawrangler/src/test/data/"
  var dw : DataWrangler = new DataWrangler(root+"cut_test.csv", "\n", ",")

  dw.promote(0)
  dw.cut(before="o")
  dw.cut(on="123")
  dw.cut(after="y")

  dw.cut(column="INT", row=1, before=3)
  dw.cut(column="INT", row=1, on=List(2,3,4))
  dw.cut(column=0, after=4)


  dw.cut(column="STRING", row=1, before="O")
  dw.cut(column="STRING", row=1, on=List("B","M","N"))
  dw.cut(column=1, after="Y")

  dw.cut(column="REGEX", row=1, before="O".r)
  dw.cut(column="REGEX", row=1, on=List("NM".r, "B".r))
  dw.cut(column=2, after="Y".r)

  dw.cut(column="TUPLE_ON", row=1, on=(0,2))
  dw.cut(column="TUPLE_ON", row=1, on=(2, "NM".r))
  dw.cut(column=3, on=(".8".r, "9"), max="all")

  dw.cut(column="TUPLE_BETWEEN", row=1, after= -1, before=3)
  dw.cut(column="TUPLE_BETWEEN", row=1, between=(1, "A78"))
  dw.cut(column=4, between=("A".r, "Y"), max="all")

  "cut" should "remove a wide array of values" in {
    assert(dw.table.map(col => col.map(y => y == "okay" || y == "OKAY").reduce(_ && _)).reduce(_ && _))
  }
}
