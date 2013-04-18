OptiWrangle
===========

A Scala library for structured to structured text processing. Parallelism supported by Spark.

In development
=======

    // Remove quotes, keep only Lat/Lon degrees, drop the time stamp

    var dw = SparkWrangler("myDataFile.csv", "\n", ",")
    dw.cut("\"")
      .promote(Array("ID", "LATITIUDE", "LONGITUDE", "DATETIME", "COMMENT"))
      .cut((s => s.sunstring(0, s.indexOf("."))), List("LATITUDE", "LONGITUDE"))
      .split(" ", "DATETIME")
      .drop(5)
      .writeToFile("cleaner.csv")
      
Consider
========

    var dw = SparkWrangler("myDataFile.csv", "\n", ",")
    dw.cut("\"")
    dw.promote(Array("ID", "LATITIUDE", "LONGITUDE", "DATETIME", "COMMENT"))
    dw.cut(after=".", on=".", List("LATITUDE", "LONGITUDE"))
    dw("DATETIME").split(" ").rename(("DATE", "TIME"))
    dw.drop("TIME")
    dw.writeToFile("cleaner.csv")
