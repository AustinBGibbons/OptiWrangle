package main.scala

import SparkWrangler._

object Gene {
  def main(args: Array[String]) {
    val location = 4
    val fastqFilename = if(args.size > 0) args(0) else 
      "/afs/cs.stanford.edu/u/gibbons4/FASTQ/sampleInput.fastq"
    //val output = "/afs/cs.stanford.edu/u/gibbons4/FASTQ/output/"
    var dw : SparkWrangler = SparkWrangler(fastqFilename, "\n", "\t", args(1))

    //val g = "AGATCGGAAGAGCGGTTCAGCAGGAATGCCGAGACCGATCTCGTATGCCGTCTTCTGCTTG".toList
    val g = "AGAT".toList
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
    //val p = dw.wrap(4).promote(header)
    val p = dw.promote(header)
    //  .partition(_.substring(location, location+4), "seq")
      .delete(clip, "seq")
      .cutBefore(13, "seq")
      .force
    println("\n\n\t Time : " + ((System.currentTimeMillis() - now) / 1e3.toDouble) + "\n\n")      

    //p.writeToFile()
  } 
}
