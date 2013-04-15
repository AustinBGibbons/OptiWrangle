import spark._
import spark.SparkContext._

import org.scalatest._

class SparkTest extends FlatSpec {
  val sc = new SparkContext("local", "Spark-Test")
  val arx = Array(1, 2, 3)
  val parx = sc.parallelize(arx)

  "collect" should "preserve the values" in {
    val sarx = parx.collect()
    assert(sarx(0) == 1)
    assert(sarx(1) == 2)
    assert(sarx(2) == 3)
  }
    
  if (sc != null) {
    //sc.stop()
    //sc = null
  }
  System.clearProperty("spark.driver.port")

}
