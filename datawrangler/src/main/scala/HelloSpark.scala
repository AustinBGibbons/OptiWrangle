package main.scala

//import SparkWrangler
import SparkWrangler._

object SparkTest {
  def main(args: Array[String]) {
    val location = 4
    val testFile = "/afs/cs.stanford.edu/u/gibbons4/data/test.data"
    var dw : SparkWrangler = SparkWrangler(testFile, "\n", ",")
    //dw = dw.promote(Array("ID","LATITUDE","LONGITUDE","USER_ID","DATE","COMMENT"))
    dw = dw.promote(0)
    dw = dw.cut(0, List("LATITUDE", "LONGITUDE")) // cut the first string
    // dw = dw.cutRight(0) // cut the last string?
    dw = dw.cut("\"")
    dw("LATITUDE") = dw("LATITUDE").cut("1") // by value
    dw("LONGITUDE") = dw("LONGITUDE").cut(1) // by position
    dw("ID") = dw("ID").cut(_.substring(1,2)) // by function
/*
    dw.writeToFile()
*/
    println(dw.toString())
  } 
}
