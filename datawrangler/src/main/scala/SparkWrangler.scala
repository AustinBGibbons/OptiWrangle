package main.scala

import scala.util.matching.Regex

import spark._
import spark.SparkContext._

class Base {

  // a hacky thing I use. Should refactor to eliminate use of this
  val OW_EXTENSION = "ow" 
 
  /**
  * Helper code to deal with warnings and errors
  * I should really make this its own package TODO
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

object Column extends Base {
  def apply(arr: Array[String], sc: SparkContext) = new Column(sc.parallelize(arr), null, sc)
}

class Column(val column: RDD[String], val header: String, sc: SparkContext) extends Base {
  // combine the RDD and the header and return as an array
  def collect() : Array[String] = {
    if(header != null) header +: column.collect()
    else column.collect()
  }

  def copy(_column: RDD[String]= column, _header: String= header, _sc: SparkContext= sc) = 
    new Column(_column, _header, _sc)

  def promote(row: Int) = {
    if(row >= column.count()) 
      error("Requesting to promote row " + row + " but there are only "+column.count()+" rows")
    val col = copy(_header = column.toArray.apply(row))
    col.delete(row)
  }
  def promote(h: String) = copy(_header = h)

  // cuts just once
  def cut(index: Int) = copy(column.map(cell => {
    println(index + " " + cell)
    if(index >= cell.size) cell //warn("tried to cut a index larger than the string")
    else cell.substring(0, index) + cell.substring(index+1)
  }))

  // cuts all. If you want to cut one, use the function operator
  def cut(value: String, all: Boolean = true) = copy(column.map(cell => {
    if (all) cell.replaceAllLiterally(value, "")
    else cell.replaceFirst(value, "")
  }))

  def cut(f: String => String) = copy(column.map(cell => {
    val result = f(cell)
    val index = cell.indexOf(result)
    if(index == -1) cell
    else cell.substring(0, index) + cell.substring(index+result.size)
  }))

  def extract(f: String => String) = copy(column.map(f))

  def delete(f: String => Boolean) = copy(column.filter(f))
  def edit(f: String => String) = copy(column.map(f))
  
  // what should my name be
  def split(f: String => String): Array[Column] = {
    val results = column.map(f)
    val before = column.zip(results).map{case(cell, result) => cell.substring(0, cell.indexOf(result))}
    val after = column.zip(results).map{case(cell, result) => cell.substring(cell.indexOf(result) + result.size)}
    Array(copy(before, "split"), copy(after, "split"))
  }

  //def split(f: String => String): Array[Column]

  // this is not supported in RDD
  def delete(row: Int) = {
    if(row >= column.count()) 
      error("Requesting to delete row " + row + " but there are only "+column.count()+" rows")
    val arr = column.toArray
    copy(sc.parallelize(arr.take(row) ++ arr.drop(row + 1)))
  }

}

object Table extends Base {
  // This could be better. How can spark deal with column-major?
  // this is probably the biggest argument for moving to row-major
  def apply(inFile: String, rows: String="\n", cols: String=null, name: String = "Table", sc: SparkContext) = {
    lazy val table : Table = new Table(
    scala.io.Source.fromFile(inFile).mkString.split(rows, -1).dropRight(1).map(x => if(cols != null) x.split(cols, -1) else Array(x)).transpose.map(arr => Column(arr, sc)), name, sc, null)
    table
  }
}

class Table(val table: Array[Column], val name: String = "Table", sc: SparkContext, headers: Map[String, Int]) extends Base {

  def copy(_table : Array[Column] = table, _name: String = name, _sc: SparkContext = sc, _headers: Map[String, Int] = headers) 
    = new Table(_table, _name, _sc, _headers)  

  def getColumn(column: Any) = column match {
    case x: Int => if(x < 0 || x >= headers.size) error("Header index is bad: " + x) ; x
    case x: String => if(!headers.contains(x)) error("No header: " + x); headers.get(x).get
    case _ => error("getColumn only defined on Strings (header) and Int (index)") ; -1
  }

  def getColumns(columns: Any) = columns match {
    case x: Seq[Any] => x.map(getColumn).toSet
    case _ => Set(getColumn(columns))
  }
  
  // create a header
  def promote(row: Int) : Table = {
    val nt = copy(table.map(_.promote(row))) 
    nt.copy(_headers = nt.table.map(_.header).zip(0 until table.size).toMap)
  }
  
  def promote(newHeader: Array[String]) : Table = {
    if(newHeader.size != table.size) 
      warn("Specified less headers ("+newHeader.size+") than columns ("+table.size+")")
    val header = if(newHeader.size > table.size) newHeader.take(table.size) else newHeader
    copy((0 until header.size).map(index => table(index).promote(header(index))).toArray ++ table.drop(table.size - header.size), _headers = header.zip(0 until header.size).toMap)
  }

  // todo - make a better name
  def mapConditional(transform: ((String => Boolean), Column) => Column, f: String => Boolean, columns: Seq[Any]) = {
    val indices = getColumns(columns)
    indices.foreach(index => table(index) = transform(f, table(index)))
    this
  } 

  def map(transform: ((String => String), Column) => Column, f: String => String, columns: Any) = {
    val indices = getColumns(columns)
    copy(table.zip(0 until table.size).flatMap{case(column, index) => {
      if(indicies.contains(index)) transform(f, column)
      else Array(column)
    })
  }

  def map(transform: (Int, Column) => Column, index: Int, columns: Any) = {
    val indices = getColumns(columns)
    indices.foreach(index => table(index) = transform(index, table(index)))
    this
  }

  def map(transform: (String, Boolean, Column) => Column, value: String, all: Boolean, columns: Any) = {
    val indices = getColumns(columns)
    indices.foreach(index => table(index) = transform(value, all, table(index)))
    this
  }

  // would cut(f: Any, ...) be better? I think not but maybe
  // cannot have both name duplication and default parameters
  // could move the burden to the frontend
  // could do columns: Any = (0 until table.size) but cannot overload name
  def cutIndex(index: Int, column: Column): Column = column.cut(index)
  def cutValue(value: String, all: Boolean, column: Column): Column = column.cut(value, all)
  def cutFunction(f: String => String, column: Column): Column = column.cut(f)
  def cut(index: Int, columns: Any): Table = {
    map(cutIndex _, index, columns)
  }
  def cut(value: String, all: Boolean, columns: Any): Table = {
    map(cutValue _, value, all, columns)
  }
  def cut(f: String => String, columns: Any): Table = {
    map(cutFunction _, f, columns)
  }

/*
  def extractWrapper(f: String => String, column: Column): Column = column.extract(f)
  def extract(f: String => String, columns: Seq[Any] = (0 until table.size)): Table = {
    map(extractWrapper, f, columns)
  }
  
  def deleteWrapper(f: String => Boolean, column: Column): Column = column.delete(f)
  def delete(f: String => Boolean, columns: Seq[Any] = (0 until table.size)): Table = {
    mapConditional(deleteWrapper, f, columns)
  }

  def editWrapper(f: String => String, column: Column): Column = column.edit(f)
  def edit(f: String => String, columns: Seq[Any] = (0 until table.size)): Table = {
    map(editWrapper, f, columns)
  }
*/
  def split(f: String => String) = copy(table.flatMap(_.split(f)))  

  def drop(column: Any) {
    column match {
      case x: Seq[Any] => x.foreach(drop)
      case _ => {
        val index = getColumn(column)
        val nt = copy(table.take(index) ++ table.drop(index+1))
        if(headers != null) nt.copy(_headers = headers.filter(x => x._2 != index))
      }
    }
  }

  // access column check me
  def apply(column: String) : Column = {
    if(headers == null) error("No headers are set (headers == null)")
    if(!headers.contains(column)) error("Requested a column that doesn't exist")
    table(headers.get(column).get)
  }
  
  def update(column: String, newColumn: Column) = {
    if(headers == null) error("No headers are set (headers == null)")
    if(!headers.contains(column)) error("Requested a column that doesn't exist")
    table(headers.get(column).get) = newColumn
  }

  def apply(column: Int) : Column = {
    if(column > table.size) error("Requested column "+column+" but there are only "+table.size+" columns")
    table(column)
  }

  def update(column: Int, newColumn: Column) = {
    if(column > table.size) error("Requested column "+column+" but there are only "+table.size+" columns")
    table(column) = newColumn
  }

  /*
  * IO
  */
  // similar column-major / transpose problems
  override def toString() : String = {
    if (table == null) "Arrrr matey this table be null!"
    else {
      //table.transpose.map(row => row.collect().mkString(",")).mkString("\n")
      // TODO add header
      table.map(col => col.collect().toArray).transpose.map(row => row.mkString(",")).mkString("\n")
    }
  }
}

// companion object for factory constructors
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

  val sc = new SparkContext("local", "SparkWrangler")
  def apply(inFile: String, rows: String="\n", cols: String=null) = {
    val file = parseFileName(inFile)
    new SparkWrangler(Array(Table(inFile, rows, cols, file._2, sc)), sc, file._1)
  }
}

class SparkWrangler(val tables: Array[Table], val sc: SparkContext, val inDir: String = "./") extends Base {

  def copy(_tables: Array[Table]= tables, _sc: SparkContext= sc, _inDir: String = inDir) =
    new SparkWrangler(_tables, _sc, _inDir)

  def apply(colName: String) = tables.map(_.apply(colName)).head // todo implicit conversion etc. etc.
  def update(colName: String, newCol: Column) = tables.map{table => table.update(colName, newCol)}
  def apply(colIndex: Int) = tables.map(_.apply(colIndex)).head // todo implicit conversion etc. etc.
  def update(colIndex: Int, newCol: Column) = tables.map{table => table.update(colIndex, newCol)}
  //def update(colName: String, newCols: Array[Column]) = tables.zip(newCols).map{case(table, column) => table.update(colName, column)}

  /*
  * User facing functions
  */
  def promote(header: Array[String]) = copy(tables.map(_.promote(header))) 
  def promote(row: Int) = copy(tables.map(_.promote(row))) 

  def cut(index: Int) = copy(tables.map(t => t.cut(index, (0 until t.table.size))))
  def cut(index: Int, columns: Any) = copy(tables.map(_.cut(index, columns)))
  def cut(value: String) = copy(tables.map(t => t.cut(value, true, (0 until t.table.size))))
  def cut(value: String, all: Boolean) = copy(tables.map(t => t.cut(value, all, (0 until t.table.size))))
  def cut(value: String, columns: Any) = copy(tables.map(_.cut(value, true, columns)))
  def cut(f: String => String) = copy(tables.map(t => t.cut(f, (0 until t.table.size))))
  def cut(f: String => String, columns: Any) = copy(tables.map(_.cut(f, columns)))

  /*
  * IO
  */

  // Todo - change to check File or Directory as a courtesy
  def writeToFile(outDir: String = inDir) : Unit = {
    tables.map(table => {
      val of = new java.io.PrintStream(new java.io.FileOutputStream(inDir + table.name))
      of.println(table)
      of.close()
    })
  }

  override def toString() : String = {
    val sep = "\n\t========== Begin Table ==========\t\n\n"
    sep + tables.map(table => table.toString()).mkString(sep)
  }
}
