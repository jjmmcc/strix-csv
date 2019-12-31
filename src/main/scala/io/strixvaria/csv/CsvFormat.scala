package io.strixvaria.csv

import java.io.{File, Writer}
import scala.io.Source

case class CsvFormat(
  lineBreak: String = "\r\n",
  sepChar: Char = ','
) {
  import CsvFormat._

  def read(src: Source): Iterator[Row] = {
    val parser = new CsvParser()
    parser.parseRows(parser.tokenize(CharReader(src))).toIterator
  }

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

case class CsvFormatException(msg: String) extends Exception(msg)

object CsvParser {
  sealed trait CsvToken
  case class FieldToken(field: String) extends CsvToken
  case object CommaToken extends CsvToken
  case object NewlineToken extends CsvToken
  case object EofToken extends CsvToken
}

class CsvParser() {
  import CsvParser._

  def tokenize(r: CharReader): Stream[CsvToken] =
    parseToken(r) match {
      case (EofToken, _) => Stream.empty
      case (tkn, tail) => tkn #:: tokenize(tail)
    }

  def parseToken(r: CharReader): (CsvToken, CharReader) =
    if (r.atEnd) 
      (EofToken, r)
    else {
      r.head match {
        case ',' => (CommaToken, r.tail)
        case '\n' => (NewlineToken, r.tail)
        case '\r' => (NewlineToken, skip('\n', r.tail))
        case '"' => parseQuoted(r)
        case c => parseUnquoted(r)
      }
    }

  def skip(expected: Char, r: CharReader): CharReader =
    if (r.atEnd)
      throw new CsvFormatException("Unexpected end of character stream")
    else {
      var actual = r.head
      if (actual != expected)
        throw new CsvFormatException(s"Expected `$expected`, found `$actual`")
      else
        r.tail
    }

  def parseUnquoted(r: CharReader): (CsvToken, CharReader) = {
    val (str, r2) = r.takeWhile(c => c != ',' && c != '\r' && c != '\n')
    (FieldToken(str), r2)
  }

  def parseQuoted(r0: CharReader): (CsvToken, CharReader) = {
    var r = skip('"', r0)
    val buf = new StringBuilder()
    var quote = false
    var closed = false

    while (!r.atEnd && !closed) {
      if (quote) {
        r.head match {
          case ',' | '\r' | '\n' => closed = true
          case '"' =>
            buf.append('"')
            quote = false
            r = r.tail
          case c =>
            throw new CsvFormatException(
              s"Line ${r.lineNum}, column ${r.colNum}: Unexpected char after double-quote: `$c`")
        }
      } else {
        r.head match {
          case '"' => quote = true
          case c => buf.append(c)
        }
        r = r.tail
      }
    }

    (FieldToken(buf.toString), r)
  }

  def parseRows(ts: Stream[CsvToken], lineNum: Int = 1): Stream[Row] = 
    parseRow(ts, lineNum) match {
      case (None, _) => Stream.empty[Row]
      case (Some(row), tail) => row #:: parseRows(tail, lineNum + 1)
    }

  def parseRow(ts: Stream[CsvToken], lineNum: Int): (Option[Row], Stream[CsvToken]) = {
    ts match {
      case Stream.Empty => (None, Stream.empty)
      case NewlineToken #:: Stream.Empty => 
        // treat empty line at end of file as simply the end of the file
        (None, Stream.empty)
      case NewlineToken #:: tail => 
        // treat empty line before the end as a single column with an empty string field
        (Some(Row(Vector(""), Some(lineNum))), tail)
      case _ =>
        val rowTs = ts.takeWhile(_ != NewlineToken)
        val buf = Vector.newBuilder[String]
        var justReadField = false

        for (t <- rowTs) {
          t match {
            case FieldToken(txt) => 
              buf += txt
              justReadField = true

            case CommaToken =>
              if (!justReadField) buf += ""
              justReadField = false

            case _ =>
          }
        }

        // if ending on a comma, or line is empty, infer empty string field
        if (!justReadField) buf += ""

        (Some(Row(buf.result, Some(lineNum))), ts.drop(rowTs.size + 1))
    }
  }
}

trait CharReader {
  def atEnd: Boolean
  def head: Char
  def lineNum: Int
  def colNum: Int
  def tail: CharReader

  def takeWhile(f: Char => Boolean): (String, CharReader) = {
    val buf = new StringBuilder
    var r = this

    while (!r.atEnd && f(r.head)) {
      buf.append(r.head)
      r = r.tail
    }

    (buf.toString, r)
  }
}

object CharReader {
  case class Empty(lineNum: Int, colNum: Int) extends CharReader {
    def atEnd = true
    def head = throw new NoSuchElementException("At end of character stream.")
    def tail = this
  }

  def apply(cs: Iterator[Char], lineNum: Int = 1, colNum: Int = 1): CharReader =
    if (!cs.hasNext) Empty(lineNum, colNum)
    else {
      val c = cs.next
      val ln = lineNum
      val cn = colNum
      val ln2 = if (c == '\n') ln + 1 else ln
      val cn2 = if (c == '\n') 1 else cn + 1
      new CharReader {
        val head = c
        def atEnd = false
        def lineNum = ln
        def colNum = cn
        lazy val tail = CharReader(cs, ln2, cn2) 
      }
    }
}
