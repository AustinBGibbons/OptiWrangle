package main.scala

//import SparkWrangler
import SparkWrangler._

object Flickr {
  def main(args: Array[String]) {
    val location = 4
    println(args(0))
    val testFile = args(0) //"/afs/cs.stanford.edu/u/gibbons4/data/test.data"
    var dw : SparkWrangler = SparkWrangler(testFile, "\n", ",", args(1))
    def regexMatch(x: String, y: String) = x.r.findFirstIn(y) match {
      case None => false
      case _ => true
    } 
    def rm(x: String)( y: String) = /*col.*/regexMatch(x, y)
    val now = System.currentTimeMillis()
    val f = dw.promote(0).cutAll("\"").drop(0).drop(5).delete(rm("1[0-8]") _, 0) 
    println("bulk: " + ((System.currentTimeMillis() - now) / 1e3))
   //dw.wrapRow(4)
/*
      .promote(Array("ID","LATITUDE","LONGITUDE","USER_ID","DATE","COMMENT"))
      .cut((s => s.substring(s.indexOf("."))), List("LATITUDE", "LONGITUDE"))
      .split(" ", "DATETIME")
      .drop(4)
*/
      f.writeToFile()

    //println(dw.toString())
  } 
}
