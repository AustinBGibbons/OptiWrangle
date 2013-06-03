package main.scala

import SparkWrangler._

object Gene {
  def main(args: Array[String]) {
    val location = 4
    val fastqFilename = if(args.size > 0) args(0) else 
      "/afs/cs.stanford.edu/u/gibbons4/FASTQ/sampleInput.fastq"
    val output = "/afs/cs.stanford.edu/u/gibbons4/FASTQ/output/"
    var dw : SparkWrangler = SparkWrangler(fastqFilename, "\n")

    val g = "AGATCGGAAGAGCGGTTCAGCAGGAATGCCGAGACCGATCTCGTATGCCGTCTTCTGCTTG".toList
    val gsize = g.size
    // this can be user defined per operation
    // or can be made into a FASTQ dsl with clipper operation and familiar params
    def clip(in: String) = {
      if(in.size < gsize) false
      in.toList.zip(g).map{case(g,m) => (g == m) || (g == 'N')}.reduce(_ || _)
    }

    // Do it all at once :
    val header = Array("def", "seq", "line3", "line4")  
    val now = System.currentTimeMillis()
    val p = dw.wrap(4).promote(header)
      .partition(_.substring(location, location+4), "seq")
      .delete(clip, "seq")
      .cut(_.substring(0, 13), "seq")
    println("\n\n\t Time : " + ((System.currentTimeMillis() - now) / 1e3.toDouble))      

    p.writeToFile()
  } 
}
