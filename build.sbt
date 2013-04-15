name := "datawrangler"

version := "1.0"

libraryDependencies  ++= Seq(
            // other dependencies here
            // pick and choose:
            // "org.scalanlp" %% "breeze-math" % "0.1",
            // "org.scalanlp" %% "breeze-learn" % "0.1",
            // "org.scalanlp" %% "breeze-process" % "0.1",
            // "org.scalanlp" %% "breeze-viz" % "0.1",
            "org.spark-project" %% "spark-core" % "0.7.0"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "net.liftweb" %% "lift-json" % "2.5-RC2"

resolvers ++= Seq(
            // other resolvers here
            "spray repo" at "http://repo.spray.io",
            "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
            // if you want to use snapshot builds (currently 0.2-SNAPSHOT), use this.
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)

scalaVersion := "2.9.2"

