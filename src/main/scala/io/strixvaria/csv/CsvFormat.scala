package io.strixvaria.csv

import java.io.{File, Writer}
import scala.io.Source

case class CsvFormat(
  lineBreak: String = "\r\n"
) {

  def read(src: Source): Iterator[Row] =
    // this could be wrong if newline is enclosed in quotes
    read(src.getLines())

  def read(lines: Iterator[String]): Iterator[Row] =
    lines.zipWithIndex.map {
      case (line, idx) => parseLine(idx + 1, line)
    }

  def parseLine(num: Int, line: String): Row =
    // TODO handle quotes
    Row(line.split(",").map(_.trim), Some(num))

  def writeLine(row: Row, out: Writer): Unit = writeLine(row.values, out)

  def writeLine(values: Seq[String], out: Writer): Unit = {
    var i = 0

    while (i < values.size) {
      if (i > 0) out.append(',')
      writeValue(values(i), out)
      i += 1
    }

    out.append(lineBreak)
  }

  def writeValue(v: String, out: Writer): Unit = {
    val vlen = v.length
    val quote = v.indexOf(',') >= 0 || v.indexOf('\n') >= 0 || v.indexOf('"') >= 0

    if (!quote) {
      out.append(v)
    } else {
      var j = 0
      out.append('"')

      while (j < vlen) {
        v.charAt(j) match {
          case '"' => out.append('"').append('"')
          case c => out.append(c)
        }
        j += 1
      }

      out.append('"')
    }
  }
}

object CsvFormat {
  implicit val default = CsvFormat()
}
