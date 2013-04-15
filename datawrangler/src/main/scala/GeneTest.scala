package main.scala

//import DataWrangler
import DataWrangler._

object GeneTest {
  def main(args: Array[String]) {
    val location = 4
    val fastqFilename = "/afs/cs.stanford.edu/u/gibbons4/FASTQ/sampleInput.fastq"
    var dw : DataWrangler = new DataWrangler(fastqFilename, "\n")
    dw.wrap(4)                                                        // wrap(row => row.contains("a")) // wrap(on="@")  // wrap(length=4)
    dw.promote(Array("def", "seq", "line3", "line4"))                 // setHeader // if you don't do this, you can index by number, ie dw(1) == dw("seq")
    dw("seq").partition(_.substring(location, location+4))              // dw.partition(row => { // dw.partition(column="seq", between=(4,8))
    //dw.cut()                                                          // complicated "containsAny" method
    dw("seq").cut(_.substring(0, 13))                                   // .cut(seq => seq.substring(0, 13)) //dw.cut(column="seq", before=13)
  } 
}


/* match { // if else is fine if that is what you prefer
  case "GGTT" => "GGTT"
  case "TTGT" => "TTGT"
  case "CAAT" => "CAAT"
  case "ACCT" => "ACCT"
  case "GGCG" => "GGCG"
  case "CCGG" => "CCGG" // whatever other file names you want
  case _ => //error handling if so desired
} */

