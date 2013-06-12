package main.scala

import scala.util.matching.Regex

import spark._
import spark.SparkContext._

object Helper extends Serializable {
  def stretch(arr: Array[String], size: Int) = {
    arr ++ Array.fill[String](size - arr.length)("")
  }
  def pairwiseMax(x: Array[Int], y: Array[Int]) = {
    x.zip(y).map{case(x,y) => scala.math.max(x,y)}//.toArray
  }
}

class Base extends Serializable{
  // a hacky thing I use. Should refactor to eliminate use of this
  val OW_EXTENSION = "rdd"

  /**
  * Helper code to deal with warnings and errors
  * I should really make this its own package TODO
  * Or learn to believe in Exceptions
  */

  val options = scala.collection.mutable.HashMap[String, String]()
  val allOptions = Map(
    ("warnings", List("none", "one", "all")),
    //("warningsFile", List()),
    //("errorsFile", List()),
    ("onError", List("fail", "ignore", "warn"))
  )

  def warn(message: String) {
    val warnings = options.getOrElse("warnings", "one")
    warnings match {
      case "none" =>
      case "one" => {println("\n\n\tWarning: " + message + "\n\n") ; options.put("warnings", "none")}
      case "all" => println("Warning: " + message)
    }
  }

  def error(message: String) {
    val onError = options.getOrElse("onError", "fail")
    onError match {
      case "ignore" =>
      case "warn" => warn(message)
      case "fail" => goodbye("Error: " + message)
    }
  }

  def goodbye(msg: String) {
    System.err.println("\n\n\t" + msg + "\n\n")
    System.exit(-1)
  }
}

object Table extends Base {
  def apply(inFile: String, rows: String, cols: String=null, name: String = "OptiWrangle", sc: SparkContext) = {
    if(rows != "\n") error("SparkWrangler only supports newline separated rows")
    val t = new Table(sc.textFile(inFile).map(s => Array(s)), 1, null, name, sc)
    if(cols != null) t.splitAll(cols, List(0))
    else t
  }
}

class Table(val table: RDD[Array[String]], val width: Int, val header: Map[String, Int], val name: String, sc: SparkContext) extends Base {

  private def copy(_table: RDD[Array[String]]=table, _width: Int=width, _header: Map[String, Int]=header, _name: String=name, _sc: SparkContext=sc)
    = new Table(_table, _width, _header, _name, _sc)

  def getHeaderIndex(columnName: String): Int = {
    if(!header.contains(columnName)) header.foreach(println)
    if(!header.contains(columnName)) error("Requested a header which doesn't exist: " + columnName)
    header.get(columnName).get
  }

  def getColumn(column: Any) = column match {
    case x: Int => if(x < 0 || x >= width) error("Header index is bad: " + x) ; x
    case x: String => getHeaderIndex(x)
    case _ => error("getColumn only defined on Strings (header) and Int (index) " + column) ; -1
  }

  def getColumns(columns: Any) = columns match {
    case x: Seq[Any] => x.map(getColumn).toSet
    case x: Array[Any] => x.map(getColumn).toSet
    case _ => Set(getColumn(columns))
  }

  def promote(newHeader: Array[String]) = copy(_header = newHeader.zip(0 until width).toMap)
  def promote(row: Int) = {
    if(row >= table.count())
      error("Requesting to promote row " + row + " but there are only "+table.count()+" rows")
    val newHeader = table.toArray.apply(row).zip(0 until width).toMap
    copy(delete(row).table, _header = newHeader)
  }

  def demote(row: Int) = {
    val newRow = header.toList.sortBy(_._2).map(_._1).toArray
    val left = sc.parallelize(table.take(row)) // todo check row is small
    val right = table.subtract(left)
    copy(_table = (left ++ sc.parallelize(Array(newRow))) ++ right, _header = null)
  }

  // todo - no need to be private
  
  private def filter(f: String => Boolean, columns: Any): Table = {
    val indices = getColumns(columns)
    val _width = 0 until width
    copy(table.filter(row => row.zip(_width).map{case(cell, index) =>
      if(indices.contains(index)) f(cell)
      else true
    }.reduce(_ || _)))
  }

  private def map(f: String => String, columns: Any): Table = {
    val indices = getColumns(columns).toArray
    val _width = 0 until width
    copy(table.map(row => row.zip(_width).map{case(cell, index) => 
      //if(indices.contains(index)) {println("gotcha: " + cell) ; f(cell)}
      //else {println("missed : " + cell) ; cell}
      if(indices.contains(index)) f(cell)
      else cell
    }))
  }

  private def flatMap(f: String => Array[String], columns: Any): Table = {
    val indices = getColumns(columns)
    val _width = 0 until width
    println("width: " + width)
    val new_sizes = table.map(row => row.zip(_width).map{case(cell, index)  => 
      if(indices.contains(index)) f(cell).size
      else 1
    }).reduce(Helper.pairwiseMax)
    println("size: " + new_sizes)
    copy(table.map(row => row.zip(_width).flatMap{case(cell, index) => 
      if(indices.contains(index)) Helper.stretch(f(cell), new_sizes(index))
      else Helper.stretch(Array(cell), new_sizes(index))
    }), new_sizes.reduce(_+_))
  }

  // CUT
  // patial support ...
  def cutBefore(index: Int, columns: Any) = map(cell => {
    if (index >= cell.size) cell
    else cell.substring(index)
  }, columns)

  def cut(index: Int, columns: Any): Table = map(cell => {
    if(index < 0) error("Trying to cut on index: " + index)
    if(index >= cell.size) cell //warn("tried to cut a index larger than the string")
    else cell.substring(0, index) + cell.substring(index+1)
  }, columns) 

  def cut(value: String, columns: Any): Table = map(_.replaceFirst(value, ""), columns) 
  
  def cut(f: String => String, columns: Any): Table = map(cell => {
    try {
      val result = f(cell)
      val index = cell.indexOf(result)
      if(index == -1) cell
      else cell.substring(0, index) + cell.substring(index+result.size)
    } catch {
      case _ => cell // perhaps not the best thing to do...
    }
  }, columns)
  
  def cutRight(value: String, columns: Any): Table = map(cell => {
    val index = cell.lastIndexOf(value)
    if(index == -1) cell
    else cell.substring(0, index) + cell.substring(index + value.size)
  }, columns)

  //def cutAll(value: String, columns: Any): Table = map(_.replaceAllLiterallyLiterally(value, ""), columns) 
  def cutAll(value: String, columns: Any): Table = map(_.replaceAllLiterally(value, ""), columns) 

  // SPLIT
  
  def split(index: Int, columns: Any): Table = flatMap(cell => {
    if (index < 0) error("Trying to split on index: " + index)
    if(index < cell.size) Array(cell.substring(0, index), cell.substring(index+1))
    else Array(cell, "")
  }, columns)

  // TODO - should this just wrap to split (_ => value)
  def split(value: String, columns: Any): Table = flatMap(cell => {
    val index = cell.indexOf(value)
    if(index != -1) Array(cell.substring(0, index), cell.substring(index+value.size))
    else Array(cell, "")
  }, columns)
  
  // splitAll? look in column-major...
  def split(f: String => String, columns: Any): Table = flatMap(cell => {
    try {
      val result = f(cell)
      val index = cell.indexOf(result)
      if(index == -1) Array(cell, "")
      else Array(cell.substring(0, index), cell.substring(index+result.size))
    } catch {
      case _ => Array(cell, "") // perhaps not the best thing to do...
    }
  }, columns)

  def splitRight(value: String, columns: Any): Table = flatMap(cell => {
    val index = cell.lastIndexOf(value)
    if(index != -1) Array(cell.substring(0, index), cell.substring(index+value.size))
    else Array(cell, "")
  }, columns)

  def splitAll(value: String, columns: Any): Table = flatMap(_.split(value), columns) 
  
  // EXTRACT

  def extract(index: Int, columns: Any): Table = flatMap(cell => {
    if(index < 0) error("Trying to extract on index: " + index)
    if(index >= cell.size) Array(cell, "")
    else Array(cell, cell(index).toString)
  }, columns)

  def extract(value: String, columns: Any): Table = flatMap(cell => {
    if(cell.indexOf(value) != -1) Array(cell, value)
    else Array(cell, "")
  }, columns)

  def extract(f: String => String, columns: Any): Table = flatMap(cell => {
    try {
      val result = f(cell)
      val index = cell.indexOf(result)
      if(index == -1) Array(cell, "")
      else Array(cell, result)
    } catch {
      case _ => Array(cell, "")
    }
  }, columns)

  // EDIT : just wraps map atm
  def edit(f: String => String, columns: Any)= map(f, columns)

  // DELETE - no good in this format.
  def delete(del: Int) : Table = 
    copy(table.zip(sc.parallelize(0 until table.count().toInt)).filter{case(row, index) => del != index}.map{case(row, index) => row})
  def delete(f: String => Boolean, columns: Any): Table = filter(f, columns)

  // DROP
  def drop(index: Int) : Table = copy(table.map(row => row.take(index) ++ row.drop(index+1))) 
  def drop(value: String) : Table = drop(getHeaderIndex(value))
  def drop(columns: Seq[Any]) : Table = {
    val indices = getColumns(columns)
    copy(table.map(row => dropSeveral(row, indices)), _header = dropSeveral(header, indices))
  }
  def dropSeveral(row: Array[String], indices : Set[Int]) = {
    row.zip(0 until row.size).filter{case (cell, index) => ! indices.contains(index)}.map(_._1)
  }
  def dropSeveral(row: Map[String, Int], indices : Set[Int]) = {
    row.filter{case (cell, index) => ! indices.contains(index)}
  }

  // MERGE
  def merge(columns: Seq[Any], glue: String = ",") = {
    if(columns.size < 2) warn("Tried to merge less than two columns: (" + columns.size + ")")
    if(columns.size > width) error("Merging " + columns.size + " when table is size " + width)
    val indices = getColumns(columns).toArray.sortWith(_ < _)
    val first = indices.head
    val concatHeader = header.filter(c => indices.contains(c._2)).toList.sortBy(_._2).map(_._1).reduce(_+_)
    val newHeader = Map((concatHeader, first)) ++ header.filter(c => !(indices.contains(c._2))).toMap
    val newTable = table.map(row => {
      row(first) = row.foldLeft("")((t, col) => t+col)
      row.zip(0 until width).filter{case(col, index) => indices.contains(index) && index != first}.map(_._1)
    })
    copy(newTable, width - (indices.size - 1), newHeader)
  }

  // todo - rowmajor columnmajor hybrid
  def transpose = this //copy(sc.parallelize(table.collect().transpose.toArray), _header = null)

  // translate - add columns or rows - kinda bad naming
  // val b = blank?
  def blank(distance: Int) = Array.fill[String](distance)("")
  def translateLeft(distance: Int) = {
    val b = blank(distance)
    copy(table.map(row => b ++ row), _width = width+distance) // todo fix header
  }
  def translateRight(distance: Int) = {
    val b = blank(distance)
    copy(table.map(row => row ++ b), _width=width+distance) // todo fix header
  }
  def translateUp(distance: Int) = {
    val b = blank(width)
    copy(table.union(sc.parallelize(Array.fill[Array[String]](distance)(b))))
  }
  def translateDown(distance: Int) = {
    val b = blank(width)
    copy(sc.parallelize(Array.fill[Array[String]](distance)(b)).union(table))
  }

  // fold
  // wrap
  def wrapShort(wrap: Int) = copy(table.map{col => col.grouped(wrap).toArray}.map(x=>x.transpose).flatMap(x => x)) // width
  def wrapLong(wrap: Int) = {
    val grouper = sc.parallelize(for (i <- 0 until table.count().toInt) yield i / wrap).coalesce(table.partitions.size)
    println(table.coalesce(grouper.partitions.size).zip(grouper).groupBy(_._2).map(_._2.map(_._1).reduce(_ ++ _)))
    copy(table.coalesce(grouper.partitions.size).zip(grouper).groupBy(_._2).map(_._2.map(_._1).reduce(_ ++ _)), _width = wrap)
  }  
  def wrap(wrap: Int) = {
    println("wrapping: " + wrap)
    if(wrap < width) wrapShort(wrap)
    else wrapLong(wrap)
  }

  // fill

  // partition 
  def partition(f: String => String, columns: Any): Array[Table] = {
    val parts = getColumns(columns).toList
    if(parts.size > 1) goodbye(" Multi-partition not yet supported ")
    val index = parts(0)
    //val keys = table.map(row => f(row(index))).distinct()
    // could be better, could be better ...
    table.groupBy(row => f(row(index))).collect().map(p => copy(sc.parallelize(p._2), _name = p._1))
  }

  // join two tables together
  def merge (o: Table) = {
    //val _header = if(checkHeader(header, o.header)) header else 
    // val _name = ...
    copy(table ++ o.table)
  }

  // todo seperators, merge function, etc.
  override def toString(): String = {
    val h = if(header != null) header.map(_._1) + "\n" else ""
    h + table.coalesce(1).toArray.map(x => x.mkString(",")).mkString("\n")
  }

  def force {println("\nForced: " + table.count())}
}

object SparkWrangler extends Base {
  // normal case: replace myFile.csv with myFile.ow
  def parseFileName(fileName : String) = {
    // TODO - get directory file belongs to
    val dirName = if(fileName.contains("/")) {
      fileName.substring(0, fileName.lastIndexOf("/") + 1)
    } else { "./" }
    val shortName = if(fileName.contains("/")) {
      fileName.substring(fileName.lastIndexOf("/")+1)
    } else {
      fileName
    }
    if (shortName.size == 0) { error("Directories not currently supported") }
    if(shortName.contains(".")) {
      val extension = shortName.substring(shortName.lastIndexOf(".")+1)
      val newExtension =
        if (extension == OW_EXTENSION) OW_EXTENSION+"."+OW_EXTENSION
        else OW_EXTENSION
      (dirName, shortName.substring(0, shortName.lastIndexOf(".")) + "." + newExtension)
    } else {
      (dirName, shortName + "." + OW_EXTENSION)
    }
  }

  def apply(inFile: String, rows: String="\n", cols: String=null, numThreads: String="32") = {
    val file = parseFileName(inFile)
    val sc: SparkContext = new SparkContext("local["+numThreads+"]", "SparkWrangler")
    new SparkWrangler(Array(Table(inFile, rows, cols, file._2, sc)), sc, file._1)
  }

  def apply(inFile: String, rows: String, cols: String, sc: SparkContext) = {
    val file = parseFileName(inFile)
    new SparkWrangler(Array(Table(inFile, rows, cols, file._2, sc)), sc, file._1)
  }
}

class SparkWrangler(val tables: Array[Table], val sc: SparkContext, val inDir: String = "./") extends Base {

  private def copy(_tables: Array[Table]= tables, _sc: SparkContext= sc, _inDir: String = inDir) =
    new SparkWrangler(_tables, _sc, _inDir)

  def promote(header: Array[String]) = copy(tables.map(_.promote(header)))
  def promote(row: Int) = copy(tables.map(_.promote(row)))
  def demote(row: Int = 0) = copy(tables.map(_.demote(row)))

  def cutBefore(index: Int) = copy(tables.map(t => t.cutBefore(index, (0 until t.width))))
  def cutBefore(index: Int, columns: Any) = copy(tables.map(_.cutBefore(index, columns)))
  
  def cut(index: Int) = copy(tables.map(t => t.cut(index, (0 until t.width))))
  def cut(index: Int, columns: Any) = copy(tables.map(_.cut(index, columns)))
  def cut(value: String) = copy(tables.map(t => t.cut(value, (0 until t.width))))
  def cut(value: String, columns: Any) = copy(tables.map(_.cut(value, columns)))
  def cut(f: String => String) = copy(tables.map(t => t.cut(f, (0 until t.width))))
  def cut(f: String => String, columns: Any) = copy(tables.map(_.cut(f, columns)))
  def cutAll(value: String) = copy(tables.map(t => t.cutAll(value, (0 until t.width))))
  def cutAll(value: String, columns: Any) = copy(tables.map(_.cutAll(value, columns)))
  def cutRight(value: String) = copy(tables.map(t => t.cutRight(value, (0 until t.width))))
  def cutRight(value: String, columns: Any) = copy(tables.map(_.cutRight(value, columns)))
  //def cutRight(f: String => String) = copy(tables.map(t => t.cutRight(f, (0 until t.width))))
  //def cutRight(f: String => String, columns: Any) = copy(tables.map(_.cutRight(f, columns)))

  def split(index: Int) = copy(tables.map(t => t.split(index, (0 until t.width))))
  def split(index: Int, columns: Any) = copy(tables.map(_.split(index, columns)))
  def split(value: String) = copy(tables.map(t => t.split(value, (0 until t.width))))
  def split(value: String, columns: Any) = copy(tables.map(_.split(value, columns)))
  def split(f: String => String) = copy(tables.map(t => t.split(f, (0 until t.width))))
  def split(f: String => String, columns: Any) = copy(tables.map(_.split(f, columns)))
  def splitAll(value: String) = copy(tables.map(t => t.splitAll(value, (0 until t.width))))
  def splitAll(value: String, columns: Any) = copy(tables.map(_.splitAll(value, columns)))
  def splitRight(value: String) = copy(tables.map(t => t.splitRight(value, (0 until t.width))))
  def splitRight(value: String, columns: Any) = copy(tables.map(_.splitRight(value, columns)))

  def extract(index: Int) = copy(tables.map(t => t.extract(index, (0 until t.width))))
  def extract(index: Int, columns: Any) = copy(tables.map(_.extract(index, columns)))
  def extract(value: String) = copy(tables.map(t => t.extract(value, (0 until t.width))))
  def extract(value: String, columns: Any) = copy(tables.map(_.extract(value, columns)))
  def extract(f: String => String) = copy(tables.map(t => t.extract(f, (0 until t.width))))
  def extract(f: String => String, columns: Any) = copy(tables.map(_.extract(f, columns)))

  def drop(index: Int) = copy(tables.map(_.drop(index)))
  def drop(value: String) = copy(tables.map(_.drop(value)))
  // how to type this better? todo
  def drop(columns: Seq[Any]) = copy(tables.map(_.drop(columns)))

  def mergeAll(glue: String = ",") = copy(tables.map(t => t.merge((0 until t.width), glue)))
  def merge(columns: Seq[Any], glue: String = ",") = copy(tables.map(t => t.merge(columns, glue)))

  def delete(index: Int) = copy(tables.map(t => t.delete(index)))
  def delete(f: String => Boolean) = copy(tables.map(t => t.delete(f, 0 until t.width)))
  def delete(f: String => Boolean, columns: Any) = copy(tables.map(_.delete(f, columns)))

  // source is one column, target is one or more columns
  // fills left/right
  def fillColumn(source: Any, target: Any) = this
  def fillLeft(source: Any) = this
  def fillRight(source: Any) = this
  def fillColumn(value: String, source: Any, target: Any) = this
  def fillLeft(value: String, source: Any) = this
  def fillRight(value: String, source: Any) = this
  def fillColumn(f: (String => Boolean), source: Any, target: Any) = this
  def fillLeft(f: (String => Boolean), source: Any) = this
  def fillRight(f: (String => Boolean), source: Any) = this
  //fills up/down
  def fillRow(source: Any, target: Any) = this
  def fillUp(source: Any) = this
  def fillDown(source: Any) = this
  def fillRow(value: String, source: Any, target: Any) = this
  def fillUp(value: String, source: Any) = this
  def fillDown(value: String, source: Any) = this
  def fillRow(f: (String => Boolean), source: Any, target: Any) = this
  def fillUp(f: (String => Boolean), source: Any) = this
  def fillDown(f: (String => Boolean), source: Any) = this

  // Todo, I'm not too wild ablut this nomenclature. The time is 1:38 am. THat is probably why.
  /*
  def wrapColumn(width: Int) = this
  def wrapColumn(width: Int, columns: Any) = this
  def wrapColumn(f: (String => Boolean)) = this
  def wrapColumn(f: (String => Boolean), columns: Any) = this
  */
  def wrap(width: Int) = copy(tables.map(_.wrap(width)))
  //def wrapRow(width: Int, columns: Any) = this
  //def wrapRow(f: (String => Boolean)) = this
  //def wrapRow(f: (String => Boolean), columns: Any) = this

  def transpose() = copy(tables.map(_.transpose))

  // fold on header
  def fold() = this
  def fold(rows: Seq[Int]) = this
  def fold(f: (String => Boolean)) = this // perhaps not worth offering all columns explicitly. What does that even mean?
  def fold(f: (String => Boolean), columns: Any) = this

  // def unfold
  def unfold(column: Any, measure: Any) = this

  def translateLeft(distance: Int) = copy(tables.map(_.translateLeft(distance)))
  def translateRight(distance: Int) = copy(tables.map(_.translateRight(distance)))
  def translateUp(distance: Int) = copy(tables.map(_.translateUp(distance)))
  def translateDown(distance: Int) = copy(tables.map(_.translateDown(distance)))

  // todo - what other types of partitioning? Every n rows, by columns, etc.
  //def partition(f: (String => String)) = partition(f, 0 until tables.size)
  def partition(f: (String => String), columns: Any = List(0)) = copy(tables.flatMap(_.partition(f, columns)))

  // todo - name
  def merge(a: Table, b: Table): Table = a.merge(b)
  def merge(): SparkWrangler = merge(0 until tables.size)
  def merge(merge_tables: Seq[Int]): SparkWrangler =
    copy(Array(tables.drop(1).foldLeft[Table](tables(0))(merge)))
  

  // Todo - change to check File or Directory as a courtesy
  def writeToFile(outDir: String = inDir) : Unit = {
    tables.map(table => {
      val name = if(table.name.size < 1) "table" + scala.util.Random.nextInt(10000) else table.name
      val of = new java.io.PrintStream(new java.io.FileOutputStream(inDir + name))
      of.println(table)
      of.close()
    })
  }

  override def toString() : String = {
    val sep = "\n\t========== Begin Table ==========\t\n\n"
    sep + tables.map(table => table.toString()).mkString(sep)
  }

  def force = {tables.foreach(_.force) ; this}
}
