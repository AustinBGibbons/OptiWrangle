package main.scala

//import SparkWrangler
import SparkWrangler._

object SparkTest {
  def main(args: Array[String]) {
    val location = 4
    val testFile = "/afs/cs.stanford.edu/u/gibbons4/data/test.data"
    var dw : SparkWrangler = SparkWrangler(testFile, "\n", ",")
    dw.cut("\"")
      .promote(Array("ID","LATITUDE","LONGITUDE","USER_ID","DATE","COMMENT"))
      .cut((s => s.substring(s.indexOf("."))), List("LATITUDE", "LONGITUDE"))
      .split(" ", "DATETIME")
      .drop(4)
      .writeToFile()

    //println(dw.toString())
  } 
}
