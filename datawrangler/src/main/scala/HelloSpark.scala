package main.scala

//import SparkWrangler
import SparkWrangler._

object SparkTest {
  def main(args: Array[String]) {
    val location = 4
    println(args(0))
    val testFile = args(0) //"/afs/cs.stanford.edu/u/gibbons4/data/test.data"
    var dw : SparkWrangler = SparkWrangler(testFile, "\n", ",")
    //dw.cutAll("\"").cut("1")//.wrap(4)
    var now = System.currentTimeMillis()
    val p = dw/*.cutAll("\"")*/.cut("1") //.wrap(4)
    p.force
    println("\n\n\t Time : " + ((System.currentTimeMillis() - now) / 1e3.toDouble) + "\n\n")
    now = System.currentTimeMillis()
    val p1 = p.cut("2") 
    p1.force
    println("\n\n\t Time : " + ((System.currentTimeMillis() - now) / 1e3.toDouble) + "\n\n")
    now = System.currentTimeMillis()
    val p2 = p1.cut("2") 
    p2.force
    println("\n\n\t Time : " + ((System.currentTimeMillis() - now) / 1e3.toDouble) + "\n\n")
    //dw.wrapRow(4)
/*
      .promote(Array("ID","LATITUDE","LONGITUDE","USER_ID","DATE","COMMENT"))
      .cut((s => s.substring(s.indexOf("."))), List("LATITUDE", "LONGITUDE"))
      .split(" ", "DATETIME")
      .drop(4)
*/
    
    p.writeToFile()
    //println(dw.toString())
  } 
}
