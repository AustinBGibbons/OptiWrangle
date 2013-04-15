import sbt._
import Keys._

object Root extends Build {

  lazy val root = Project(
    id = "root",
    base = file("."),
    aggregate = Seq(datawrangler)
  )

  lazy val datawrangler = Project(
    id = "datawrangler",
    base = file("datawrangler")
  )
/*
  lazy val rest = play Project(
    "emoticat-rest", "1.0", Nil
  )
*/
}
