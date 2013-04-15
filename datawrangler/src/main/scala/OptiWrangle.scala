package main.scala

import scala.util.matching.Regex

class DataWrangler(var table: Array[Array[String]], var header: Array[String]) {
  // drop EOF character
  val bigger = table.drop(1).map(x => table(0).length > x.length).reduce(_ && _)
  if (bigger || table.length == 1)
    table(0) = table(0).dropRight(1)

  val FAKE_STRING = "L!G0nb**g" + (new scala.util.Random()).nextInt

  /**
  * Helper code to deal with warnings and errors
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

  /**
  * Helper functions to OptiWrangle
  */
  def headerIndex(x: String) = {
    val index = header.indexOf(x)
    if(index == -1) error("Requested bad header: " + x)
    index
  }

  def getColumn(column: Any) = {
   column match {
      case col: Int => col
      case col: String => headerIndex(col)
    }
  }

  def stretch(arr: Array[String], size: Int) = {
    arr ++ Array.fill[String](size - arr.length)("")
  }

  /**
  * Apply the same operation to a column
  * TODO : see how much more general we can make the system
  * currently I'm erring on the side of "flexibility"...
  */
  def transformer(op: ((Array[String], Seq[(Any, String)], String, Int) => Array[String]), 
    actions: Seq[(Any, String)], column: Any, max: String, row: Int) {
    if(column != null) {
      val index = getColumn(column)
      table(index) = op(table(index), actions, max, row)
    }
    else {
      table = table.map(col => op(col, actions, max, row))
    }
  }

  def builderWrapper(op: ((Array[String], Seq[(Any, String)], String, Int) => Array[Array[String]]), 
    actions: Seq[(Any, String)], column: Any, max: String, row: Int) : Seq[(Int, Array[Array[String]])] = {
    column match {
      case null => builderWrapper(op, actions, (0 until table.length), max, row)
      case x: Seq[Any] => x flatMap(col => builderWrapper(op, actions, col, max, row))
      case _ => {
        val index = getColumn(column)
        val newColumns = op(table(index), actions, max, row)
        List((index, newColumns))
      }
    }
  }

  def builder(op: ((Array[String], Seq[(Any, String)], String, Int) => Array[Array[String]]), 
    actions: Seq[(Any, String)], column: Any, max: String, row: Int) {
    val newColList = builderWrapper(op, actions, column, max, row).sortWith(_._1 > _._1)
    val NEW_HEADER_NAME = "__new_header"
    newColList.foreach{case(index, newColumns) => 
      table = table.take(index) ++ newColumns ++ table.drop(index+1)
      if(header != null) 
        header = header.take(index) ++ 
          Array.fill[String](newColumns.length)(NEW_HEADER_NAME)++header.drop(index+1)
    }
  }

  /**
  * Bundle actions with their namesake
  */
  def makeActions(after: Any, before: Any, between: Any, on: Any) = {
    val actions = makeAction(on, "on") ++ makeAction(between, "between") ++ makeBetween(after, before)
    if(actions.length == 0) 
      warn("called cut with an empty set of rules")
    if(actions.toSet.size != actions.length)
        warn("cut actions is not unique: " + actions.mkString(" "))
    actions
  }
  
  def makeAction(action: Any, offset: String) = {
    var actions = scala.collection.mutable.Buffer[(Any, String)]()
    action match {
      case x: Seq[Any] => x foreach {y => actions += ((y, offset))}
      case null =>
      case _ => actions += ((action, offset))
    }
    actions.toList
  }

  def makeBetween(after: Any, before: Any) = {
    if(after != null && before != null) {
      val a = after match {
        case x: Seq[Any] => x
        case _ => List(after) 
      }
      val b = before match {
        case x: Seq[Any] => x
        case _ => List(before) 
      }
      a flatMap(ai => b map (bi =>  ((ai,bi), "between"))) toList
    } else {
      makeAction(after, "after") ++ makeAction(before, "before")
    }
  }

  // a little bit hacky. Makes it so on=List(3,4,5) cuts 3rd, 4th, and 5th, instead of
  // 3rd, 5th, and 8th (because indices change after cut is applied)
  // might not hold up in the long run (ie, (3, regex))
  def sortActions(actions: Seq[(Any, String)]) : Seq[(Any, String)] = {
    val intActions = actions.map(x => x._1 match {
      case y: Int => (y, x)
      case y: Tuple2[Any,Any] => y._1 match {
        case z: Int => y._2 match {
          case w: Int => (w, x)
          case _ => (z, x)
        }
        case _ => y._2 match {
          case z: Int => (z, x)
          case _ => (-1, x)
        }
      }
      case _ => (-1, x)
    })
    intActions.sortBy(_._1).reverse.map(_._2) // todo check overlaps and error()
  }

  // Transformations - I would like these each to be their own trait, 
  // But I am not quite sure how to Forge that.

  /**
  * Assign a row as the header row
  */
  def promote(row: Int= 0) {
    if(row >= table(0).length) 
      error("Cannot promote: " + row + " exceeds table length: " + table(0).length)
    header = table.map(col => col(row)).toArray
    if(header.toSet.size != header.length) error("trying to create a non-unique header")
    table = table map (col => col.take(row) ++ col.drop(row+1))
  } 

  /**
  * Move the header row into the table
  */
  def demote(row: Int=0) {
    if(row > table(0).length) 
      goodbye("Cannot demote: " + row + " exceeds table length: " + table(0).length)
    //table = (table.take(row) :+ header) ++ table.drop(row)
    table = table.zip(header) map {case(col, colHeader) => (col.take(row) :+ colHeader) ++ col.drop(row)}
    header = null
  }

  /**
  * Merge, a different name for zip
  * todo row, Seq[Tuple], etc.
  */

  def removeColumns(cols: List[Int]) = {
    table = table.zip(0 until table.length).filter{case (v,i) => !(cols.contains(i))}.map(x => x._1)
    if(header != null)
      header = header.zip(0 until header.length).filter{case (v,i) => !(cols.contains(i))}.map(x => x._1)
  }

  def mergeAll(cols: List[Int], glue: String) = {
    val merge = table.zip(0 until table.length).filter{case (v,i) => cols.contains(i)}.map(x => x._1)
    val first = merge(0)
    merge.drop(1).foldLeft(first)((r,c) => r.zip(c).map(x => x._1 + glue + x._2))
  }

  def merge(column: Any, glue: String = "") {
    val MERGED_COLUMN_NAME= "merged_column"
    var cols = column match {
      case x: Seq[Any] => x.map(y => getColumn(y))
      case x: Tuple2[Any, Any] => List(getColumn(x._1), getColumn(x._2))
    }
    if(cols.length < 2) {
      warn("called merge without wanting to merge anything")
    } else {
      val newCol = mergeAll(cols.toList, glue)
      removeColumns(cols.toList)
      cols = cols.sortWith(_ < _)
      val neighbors = cols.dropRight(1).zip(cols.drop(1)).map(x => x._1+1 == x._2).reduce(_ && _)
      if(neighbors) {
        val index = cols(0)
        table = (table.take(index) :+ newCol) ++ table.drop(index)
        if(header != null) header = (header.take(index) :+ MERGED_COLUMN_NAME) ++ header.drop(index)
      }
      else {
        table = table :+ newCol
        if(header != null) header = header :+ MERGED_COLUMN_NAME
      }
    }
  }
  
  /**
  * Drop: drop a column from the table
  */
  def drop(column: Any) {
    column match {
      case x: Seq[Any] => x.foreach(drop)
      case _ => {
        val index = getColumn(column)
        table = table.take(index) ++ table.drop(index+1)
        if(header != null) header = header.take(index) ++ header.drop(index+1)
      }
    }
  }

  /**
  * Delete: conditionally eliminate rows
  * where is a regex
  */
  def delete(row: Any = null, column: Any = null, where: Any = null) {
    column match {
      case x: Seq[Any] => x.foreach(delete(row, _, where))
      case null => delete(row, (0 until table.length), where)
      case _ => 
        val w = where match {
          case x: String => (x.r, 0)
          case x: Regex => (x, 0)
          case _ => ("".r, -1)
        }
        val col = getColumn(column)
        row match {
          case x: Int => {
            if(x > table(col).length) error("Row: " + x + " exceeds table length")
            else if(w._2== -1 || w._1.findFirstIn(table(col)(x)) != None) table = table.map(col => col.take(x) ++ col.drop(x+1))
          }
          case x: Seq[Int] => x.sortWith(_ > _ ).foreach(delete(_, col, where))
          case null => (0 until table(col).length).reverse.foreach(delete(_, col, where))
        }
    }
  }

  /**
  * Fill: propogate values
  */
  def fill(row: Any = null, column: Any = null, direction: String = "down") {
    column match {
      case x: Seq[Any] => {x foreach(y => fill(row, y, direction)) ; return }
      case null => {fill(row, (0 until table.length), direction) ; return }
      case _ =>
    }
    row match {
      case x: Seq[Any] => {x foreach(y => fill(y, column, direction)) ; return }
      case null => {fill((0 until table(0).length), row, direction) ; return }
      case _ =>
    }
    direction match {
      case "down" => {
        val col = getColumn(column)
        for(i <- 1 until table(col).length) {
          if (table(col)(i) == "")
            table(col)(i) = table(col)(i-1)
        }
      }
      case "up" => {
        val col = getColumn(column)
        for(i <- (0 until table(col).length-1).reverse) {
          if (table(col)(i) != "")
            table(col)(i) = table(col)(i+1)
        }
      }
      case "left" => {
        val r = row match {case r: Int => r}
        for(col <- 1 until table.length) {
          if (table(col)(r) != "")
            table(col)(r) = table(col)(r-1)
        }
      }
      case "right" => {
        val r = row match {case r: Int => r}
        for(col <- (0 until table.length-1).reverse) {
          if (table(col)(r) != "")
            table(col)(r) = table(col)(r+1)
        }
      } 
    } 
  }

  // todo : direction - currently only down on columns
  def wrap(row: Any = null, column: Any = null, where: Any = null) {
    if (column != null) {
      table.transpose
      wrap(column, null, where)
      table.transpose // todo
    }
    val w = where match {
      case x: String => x.r
      case x: Regex => x
      case _ => {error("Unrecognized conditional for wrap"); "".r}
    }
    table.transpose
    val size = table.flatMap(col => col.map(cell => if(w.findFirstIn(cell) != None) 1 else 0)).reduce(_+_) + 1 //+1 for first ro
    val newTable = Array.fill[Array[String]](size)(Array[String]())
    val arrSizes = Array.fill[Int](size)(0)
    var counter = 0
    var index = 0
    for(i <- 0 until table.length) {
      for (j <- 0 until table(i).length) {
        if(w.findFirstIn(table(i)(j)) != None) {
          arrSizes(index) = counter
          index += 1
          counter = 0
        }
        counter += 1
      }
    }
    arrSizes(index) = counter
    val maxSize = arrSizes.reduce(scala.math.max)
    var total = 0
    val origRow = table.length
    val origCol = table(0).length
    for(a <- 0 until size) {
      val newRow = Array.fill[String](maxSize)("")
      for(b <- 0 until arrSizes(a)) {
        newRow(b) = table(total / origCol)(total % origCol)
        total += 1
      }
      newTable(a) = newRow
    }
    table = newTable.transpose
    header = null // header loses meaning under wrap ?
  }

  /**
  * Edit: edit a field, column, or row
  */
  def edit(row: Any = null, column: Any = null, value: String = null) {
    column match {
      case x: Seq[Any] => {x foreach(y => edit(row, y, value)) ; return }
      case null => {edit(row, (0 until table.length), value) ; return }
      case _ =>
    }
    row match {
      case x: Seq[Any] => {x foreach(y => edit(y, column, value)) ; return}
      case null => {edit((0 until table(0).length), column, value) ; return }
      case _ =>
    }
    val c = getColumn(column)
    val r = row match {case r: Int => r}
    table(c)(r) = value
  }

  /**
  * Fold: Pair elements with a header
  * columns todo
  */
  def fold(column: Any = null, rows: Any = null) {
    val r = rows match {
      case x: Seq[Any] => x.map (j => j match {
        case y: Int => y
        case y: String => {demote(0) ; 0}
      })
      case x: Int => List(x)
      case x: String => {demote(0) ; List(0)}
    }
    val c = column match {
      case x: Seq[Any] => x map(getColumn)
      case _ => List(getColumn(column))
    }
    if(table.length > table(0).length) warn("Folding a fat table")
    // for each row generate repeating column
    // for each column intermix
    val foldRows = r.map(index => table.map(col => col(index)))
    delete(row=r)
    val splice = table.zip(0 until table.length).filter{case(v,i) => c.contains(i)}.map(x => x._1)
    val space = table.zip(0 until table.length).filter{case(v,i) => !(c.contains(i))}.map(x => x._1)
    space.zip(Array.fill[Array[String]](space(0).length)(Array.fill[String](splice.length)(""))).flatMap(x => Array(x._1) ++ x._2)
    val holdMe = table
    table = space 
    fill(direction="down")
    val foldCols = Array.fill[Array[String]](r.length)(Array.fill[String](splice.size)(""))
    for(i <- 0 until foldCols.length) {
      val size = foldRows(i).length
      for(j <- 0 until foldCols(0).length) {
        foldCols(i)(j) = foldRows(i)(j / size) // check me
      }
    }
    table = (table ++ foldCols) :+ splice.transpose.flatMap(x => x)
    header = null // header loses meaning under fold ?
  }

  /**
  * Unfold: Live dangerously and separate values
  */
  def repSize(arr: Array[String]) = {
    var i = 1
    while(arr(i) != arr(0) && !(arr.take(i).zip(arr.drop(i).take(i)).map(x => x._1 == x._2).reduce(_ && _))) i += 1 
    i
  }

  def unfold(column: Any = null, measure: Any = null) {
    val c = column match {
      case x: Seq[Any] => {
        warn("Unfolding multiple columns, assuming well-formed")
        x map (getColumn)
      }
      case _ => List(getColumn(column))
    }
    val size = c.map(col => repSize(table(col))).reduce(scala.math.max)
    if (table.length * table(0).length % size != 0) warn("Truncating leftover values from unfold")
    val unfoldCol = c.map(col => table(col).take(size))
    table = table.zip(0 until table.length).filter(col => c.contains(col._2)).map(x => x._1)
    val newTable = Array.fill[Array[String]](size)(Array.fill[String](table(0).length * table.length / size)(""))
    for(i <- 0 until table.length) {
      for(j <- 0 until table(0).length) {
        val total = i*table(0).length + j
        newTable(total % size)(total / size) = table(i)(j) // check me
      }
    }
    table = newTable.zip(unfoldCol).map(x => x._2 ++ x._1)
    header = null // header loses meaning under unfold ?
  }

  /**
  * Translate: add a blank cell or row
  */
  def translateHelper(index: Int, direction: String, distance: Int) {
    direction match {
      case "down" => table(index) = Array.fill[String](distance)("") ++ table(index)//.dropRight(distance)
      case "up" => table(index) = table(index).drop(distance) ++ Array.fill[String](distance)("")
      //case "left" => 
      //case "right" => 
    }
  }

  def translate(column: Any = null, direction: String = "down", distance: Int = 1) {
    column match {
      case null => translate((0 until table.length), direction, distance)
      case x: Seq[Any] => x.map(x => getColumn(x)).foreach(translateHelper(_, direction, distance))
      case _ => translateHelper(getColumn(column), direction, distance)
    }
  }


  /**
  * Applies a transpose, naturally. Need to re-write :-/ todo
  */
  def transpose = {table.transpose ; header = null}

  /////////////////////////////////////////////////////
  /**
  * Cut : removes items from table elements
  * by position, string value, regex, or ranges thereof
  */
  /////////////////////////////////////////////////////

  def cutHelper(cell: String, replace: String, max: String, offset: String) = {
    if(cell.indexOf(replace) == -1) cell 
    else {
      max match {
        case "first" => 
          offset match {
            case "on" => cell.replaceFirst(replace, "")
            case "after" => cell.substring(0, cell.indexOf(replace) + replace.length)
            case "before" => cell.substring(cell.indexOf(replace))
            case _ => {error("trying to cut first using non-existant: " + offset) ; cell}
          }
        case "all" =>
          offset match {
            case "on" => cell.replaceAll(replace, "")
            case _ => {error("trying to cut all using non-existant: " + offset) ; cell}
          }
      }
    }
  }

  def cutHelper(cell: String, replace: Regex, max: String, offset: String) = {
    if(cell.indexOf(replace.findFirstIn(cell).getOrElse(FAKE_STRING)) == -1) cell
    else {
      max match {
        case "first" => 
          offset match {
            case "on" => replace.replaceFirstIn(cell, "")
            case "after" => {
              val m = replace.findFirstIn(cell).getOrElse(FAKE_STRING)
              cell.substring(0, cell.indexOf(m) + m.length)
            }
            case "before" =>  
              cell.substring(cell.indexOf(replace.findFirstIn(cell).getOrElse(FAKE_STRING)))
            case _ => {error("trying to cut once using non-existant: " + offset) ; cell}
          }
        case "all" =>
          offset match {
            case "on" => replace.replaceAllIn(cell, "")
            case _ => {error("trying to cut all using non-existant: " + offset) ; cell}
          }
      }
    }
  }

  def cutHelper(cell: String, replace: Int, max: String, offset: String) = {
    offset match {
      case "on" => {
        if(cell.length > replace) cell.substring(0,replace) + cell.substring(replace+1)
        else cell
      }
      case "after" =>  {
        if(cell.length > replace) cell.substring(0, replace)
        else cell
      }
      case "before" => {
        if(cell.length > replace) cell.substring(replace)
        else ""
      }
      case _ => {error("trying to cut using non-existant: " + offset) ; cell}
    }
  }

  // todo - is this general 
  def cutIndex(x: Any, temp: String, start: Int, offset: String, pos: String) = {
    // -10 because Forge doesn't do Option {yet}
    val index = x match {
      case y: Int => (y, 1)
      case y: String => {
        if(temp.substring(start).indexOf(y) == -1) (-10, 0)
        else (temp.substring(start).indexOf(y)+start, y.length)
      }
      case y: Regex => {
        val m = y.findFirstIn(temp.substring(start)).getOrElse(FAKE_STRING)
        if (temp.substring(start).indexOf(m) == -1) (-10, 0)
        else {
          (temp.substring(start).indexOf(m)+start, m.length)
        }
      }
    }
    if(index._1 == -10) -10
    else offset match {
      case "on" => 
        if(pos=="first") index._1 else index._1 + index._2
      case "between" => 
        if(pos=="first") index._1 + index._2 else index._1
    }
  }

  def cutHelper(cell: String, replace: Tuple2[Any, Any], max: String, offset: String) = {
    var temp = cell
    var first = -10
    var second = -10
    var start = 0
    var repeat = true
    while(repeat) {
      first = cutIndex(replace._1, temp, start, offset, "first")
      if(first == -10) repeat = false
      else {
        second = cutIndex(replace._2, temp.substring(first), scala.math.max(start-first,0), offset, "second") + first
        if(second == -10) repeat = false
        else if(first >= second) {
            error("Cutting between posistions " +first+ " and  " +second)
            repeat = false
        } else {
          max match {
            case "first" => repeat = false
            case "all" => start += first 
          }
          temp = temp.substring(0, first) + temp.substring(second)
          if(start > temp.length) repeat = false
        }
      }
    }
    temp
  }

  def cutCell(cell: String, actions: Seq[(Any, String)], max: String) = {
    var temp = cell;
    actions foreach(o => o._1 match {
      case x: String => temp=cutHelper(temp, x, max, o._2)
      case x: Regex => temp=cutHelper(temp, x, max, o._2)
      case x: Int => temp=cutHelper(temp, x, max, o._2)
      case x: Tuple2[Any, Any] => temp=cutHelper(temp, x, max, o._2)
    })
    temp
  }

  def cutColumn(column: Array[String], actions: Seq[(Any, String)], max: String, row: Int) = {
    if(row == -1) column.map(cell => cutCell(cell, actions, max))
    else {column(row) = cutCell(column(row), actions, max); column}
  }

  def cut(after:Any= null, before:Any= null, between: Any = null, column: Any = null, max: String = "first", on: Any= null, row: Int= -1) {
    val actions = makeActions(after, before, between, on)
    transformer(cutColumn, sortActions(actions), column, max, row)
  }

  /////////////////////////////////////////////////////
  /**
  * Split : create new column (a,b) => (a) (b)
  * by position, string value, regex, or ranges thereof
  */
  /////////////////////////////////////////////////////

  def splitHelperString(cell: String, replace: String, max: String, offset: String) = {
    if(cell.indexOf(replace) == -1) Array(cell)
    else {
      max match {
        case "first" =>
          offset match {
            case "on" => Array(cell.substring(0, cell.indexOf(replace)), cell.substring(cell.indexOf(replace)+replace.length))
            case "after" => Array(cell.substring(0, cell.indexOf(replace)+replace.length), cell.substring(cell.indexOf(replace)+replace.length))
            case "before" => Array(cell.substring(0, cell.indexOf(replace)), cell.substring(cell.indexOf(replace)))
            case _ => {error("trying to split first using non-existant: " + offset) ; Array(cell)}
          }
        case "all" =>
          offset match {
            case "on" => cell.split(replace)
            case "after" => cell.split(replace).map(x => x + offset)
            case "before" => cell.split(replace).map(x => offset + x)
            case _ => {error("trying to split all using non-existant: " + offset) ; Array(cell)}
          }
      }
    }
  }

  def splitHelperInt(cell: String, replace: Int, max: String, offset: String) = {
    if(cell.length < replace) Array(cell)
    else {
      offset match {
        case "on" => Array(cell.substring(0,replace), cell.substring(replace+1))
        case "after" =>  Array(cell.substring(0, replace+1), cell.substring(replace+1))
        case "before" => Array(cell.substring(0, replace), cell.substring(replace))
        case _ => {error("trying to split using non-existant: " + offset) ; Array(cell)}
      }
    }
  }

  // todo - is this general
  def splitIndex(x: Any, temp: String, start: Int, offset: String, pos: String) = {
    // -10 because Forge doesn't do Option {yet}
    val index = x match {
      case y: Int => (y, 1)
      case y: String => {
        if(temp.substring(start).indexOf(y) == -1) (-10, 0)
        else (temp.substring(start).indexOf(y)+start, y.length)
      }
      case y: Regex => {
        if(temp.substring(start).indexOf(y.toString) == -1) (-10, 0)
        else (temp.substring(start).indexOf(y.toString)+start, y.toString.length)
      }
    }
    if(index._1 == -10) -10
    else offset match {
      case "on" =>
        if(pos=="first") index._1 else index._1 + index._2
      case "between" =>
        if(pos=="first") index._1 + index._2 else index._1
    }
  }

  def splitHelperTuple(cell: String, replace: Tuple2[Any, Any], max: String, offset: String) = {
    var temps = Array(cell)
    var first = -10
    var second = -10
    var start = 0
    var repeat = true
    while(repeat) {
      temps = temps flatMap (temp => {
        first = splitIndex(replace._1, temp, start, offset, "first")
        if(first == -10) {repeat = false ; Array(temp)}
        else {
          second = splitIndex(replace._2, temp.substring(first), scala.math.max(start-first, 0), offset, "second") 
          if(second == -10) {
            repeat = false
            Array(temp)
          }
          else if(first >= (second+first)) {
            error("Splitting between posistions " +first+ " and  " +second)
            repeat = false
            Array(temp)
          } else {
            second += first
            max match {
              case "first" => repeat = false
              case "all" => //start += first
            }
            if(start > temp.length) repeat = false
            Array(temp.substring(0, first), temp.substring(second))
          }
        }
      })
    }
    temps
  }

  def splitCell(cell: String, actions: Seq[(Any, String)], max: String) = {
    var temp = Array(cell)
    actions foreach(o => o._1 match {
      case x: String => temp=temp.flatMap(splitHelperString(_, x, max, o._2))
      case x: Regex => temp=temp.flatMap(splitHelperString(_, x.toString, max, o._2))
      case x: Int => temp=temp.flatMap(splitHelperInt(_, x, max, o._2))
      case x: Tuple2[Any, Any] => temp=temp.flatMap(splitHelperTuple(_, x, max, o._2))
    })
    temp
  }

  def splitColumn(column: Array[String], actions: Seq[(Any, String)], max: String, row: Int) = {
    if(row != -1) error("split not implemented on a single row")
    val size = column.map(cell => splitCell(cell,actions,max).size).reduce(scala.math.max)
    if(size != 1) column.map(cell => stretch(splitCell(cell, actions, max), size)).transpose
    else Array(column)
    //else {column(row) = splitCell(column(row), actions, max); column}
  }

  def split(after:Any= null, before:Any= null, between: Any = null, column: Any = null, max: String = "first", on: Any= null, row: Int= -1) {
    val actions = makeActions(after, before, between, on)
    if(actions.length == 0) {
      warn("called cut with an empty set of rules")
    } else {
      if(actions.toSet.size != actions.length)
        warn("cut actions is not unique: " + actions.mkString(" "))
      builder(splitColumn, sortActions(actions), column, max, row)
    }
  }

  /////////////////////////////////////////////////////
  /**
  * Extract : create new column (a,b) => (ab) (,)
  * by position, string value, regex, or ranges thereof
  */
  /////////////////////////////////////////////////////

  def extractHelperString(cell: String, replace: String, max: String, offset: String) = {
    if(cell.indexOf(replace) == -1) Array(cell)
    else {
      val s = cell.indexOf(replace)
      val r = replace.length
      max match {
        case "first" =>
          offset match {
            case "on" => Array(cell.substring(0, s) + cell.substring(s+r), cell.substring(s, s+r))
            //case "after" => Array(cell.substring(0, s) + cell.substring(s+r), cell.sunstring(s, s+r))
            //case "before" => Array(cell.substring(0, s) + cell.substring(s+r), cell.sunstring(s, s+r))
            case _ => {error("trying to extract first using non-existant: " + offset) ; Array(cell)}
          }
        case "all" =>
          offset match {
            case "on" => {
              val split = cell.split(replace)
              val arr = Array.fill[String](split.length)(replace)
              arr(0) = split.mkString
              arr
            }
            case _ => {error("trying to extract all using non-existant: " + offset) ; Array(cell)}
          }
      }
    }
  }

  def extractHelperInt(cell: String, replace: Int, max: String, offset: String) = {
    if(cell.length < replace) Array(cell)
    else {
      offset match {
        case "on" => Array(cell.substring(0,replace) + cell.substring(replace+1), cell(replace).toString)
        case _ => {error("trying to extract using non-existant: " + offset) ; Array(cell)}
      }
    }
  }

  // todo - is this general
  def extractIndex(x: Any, temp: String, start: Int, offset: String, pos: String) = {
    // -10 because Forge doesn't do Option {yet}
    val index = x match {
      case y: Int => (y, 1)
      case y: String => {
        if(temp.substring(start).indexOf(y) == -1) (-10, 0)
        else (temp.substring(start).indexOf(y)+start, y.length)
      }
      case y: Regex => {
        if(temp.substring(start).indexOf(y.toString) == -1) (-10, 0)
        else (temp.substring(start).indexOf(y.toString)+start, y.toString.length)
      }
    }
    if(index._1 == -10) -10
    else offset match {
      case "on" =>
        if(pos=="first") index._1 else index._1 + index._2
      case "between" =>
        if(pos=="first") index._1 + index._2 else index._1
    }
  }

  def extractCell(cell: String, actions: Seq[(Any, String)], max: String) = {
    var temp = Array(cell)
    actions foreach(o => o._1 match {
      case x: String => temp=temp.flatMap(extractHelperString(_, x, max, o._2))
      case x: Regex => temp=temp.flatMap(extractHelperString(_, x.toString, max, o._2))
      case x: Int => temp=temp.flatMap(extractHelperInt(_, x, max, o._2))
      case x: Tuple2[Any, Any] => temp=temp.flatMap(extractHelperTuple(_, x, max, o._2))
    })
    temp
  }

 def extractHelperTuple(cell: String, replace: Tuple2[Any, Any], max: String, offset: String) = {
    var temps = Array(cell)
    var first = -10
    var second = -10
    var start = 0
    var repeat = true
    while(repeat) {
      temps = temps flatMap (temp => {
        first = extractIndex(replace._1, temp, start, offset, "first")
        if(first == -10) {repeat = false ; Array(temp)}
        else {
          second = extractIndex(replace._2, temp.substring(first), scala.math.max(start-first, 0), offset, "second")
          if(second == -10) {
            repeat = false
            Array(temp)
          }
          else if(first >= (second+first)) {
            error("Extractting between posistions " +first+ " and  " +second)
            repeat = false
            Array(temp)
          } else {
            second += first
            max match {
              case "first" => repeat = false
              case "all" => //start += first
            }
            if(start > temp.length) repeat = false
            Array(temp.substring(0, first)+ temp.substring(second), temp.substring(first, second))
          }
        }
      })
    }
    temps
  }

  def extractColumn(column: Array[String], actions: Seq[(Any, String)], max: String, row: Int) = {
    if(row != -1) error("extract not implemented on a single row")
    val size = column.map(cell => extractCell(cell,actions,max).size).reduce(scala.math.max)
    /*if(row == -1)*/ column.map(cell => stretch(extractCell(cell, actions, max), size)).transpose
    //else {column(row) = extractCell(column(row), actions, max); column}
  }

  def extract(after:Any= null, before:Any= null, between: Any = null, column: Any = null, max: String = "first", on: Any= null, row: Int= -1) {
    val actions = makeActions(after, before, between, on)
    if(actions.length == 0) {
      warn("called cut with an empty set of rules")
    } else {
      if(actions.toSet.size != actions.length)
        warn("cut actions is not unique: " + actions.mkString(" "))
      builder(extractColumn, sortActions(actions), column, max, row)
    }
  }

  // IO
  def this(inFile: String, rows: String="\n", cols: String=",") = this(
    scala.io.Source.fromFile(inFile).mkString.split(rows, -1).map(x => x.split(cols, -1)).transpose, null
  )

  def writeToFile(outFile: String) : Unit = {
    val of = new java.io.PrintStream(new java.io.FileOutputStream(outFile))
    if(header != null) of.println(header.mkString(","))
    table.transpose foreach { row =>
      of.println(row.mkString(","))
    }
    of.close()
  }

  override def toString() : String = {
    if (table == null) "Arrrr matey this table be null!"
    else {
      {if(header != null) header.mkString(",") + "\n" else ""} +
      table.transpose.map(x => x.mkString(",")).mkString("\n")
    }
  }
}
