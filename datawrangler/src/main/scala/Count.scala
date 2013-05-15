package main.scala

//import SparkWrangler
import SparkWrangler._
import spark._
import spark.SparkContext._

object Count {
  def main(args: Array[String]) {
    val location = 4
    val testFile = args(0) //"/afs/cs.stanford.edu/u/gibbons4/data/counting8x3e6.csv"
    var dw : SparkWrangler = SparkWrangler(testFile, "\n", ",")
    /*
    dw = dw.cut("\"")
    dw = dw.cut("1")
    dw = dw.cut("3")
    dw = dw.split("5")
    */
    dw.cut("\"").cut("1").cut("3").split("5").writeToFile()
    //dw = dw.extract("7")
    //dw.writeToFile()
    //val sc = new SparkContext("local", "test")
    //println(sc.textFile(testFile).map(x => x.replaceAll("\"", "").replaceAll("1", "").replaceAll("3", "").split(",").flatMap(x => x.split("5"))).count())
  } 
}
