package main.scala

import spark._
import spark.SparkContext
import spark.SparkContext._

object GeneSpark {
  def gene_partition(table : RDD[Array[String]]) : Array[RDD[Array[String]]] = {
    table.groupBy(row => row(1).substring(4,8)).collect().map(p => sc.parallelize(p._2))
  }

  val g = "AGAT"
  val GENE_SIZE = g.size

  def clip(seq: String) = {
    if(seq.size < GENE_SIZE) false
    else seq.take(GENE_SIZE).zip(g).map{case(x,y) => (x == 'N') || (x==y)}.reduce(_ || _)
  }

  def gene_processing(table: RDD[Array[String]]) : RDD[Array[String]] = {
    table.filter(row => clip(row(1))).map(row => {
      if (row(1).size >= 13) Array(row(0), row(1).substring(13), row(2), row(3))
      else row
    })
  }

  var sc: SparkContext = null
  def main(args: Array[String]) {
    sc = new SparkContext("local["+args(1)+"]", "GeneSpark")
    val table = sc.textFile(args(0)).map(s => s.split("\t"))
    val now = System.currentTimeMillis()
    val tables = gene_partition(table)
    val filtered = tables.map(gene_processing)
    filtered.foreach(x => println(x.count()))
    println("\n\n\t Time : " + ((System.currentTimeMillis() - now) / 1e3.toDouble) + "\n\n")
 //   filtered.foreach(x => if(x.count() > 3) println((x.collect())(3)(1)))
  }
}
