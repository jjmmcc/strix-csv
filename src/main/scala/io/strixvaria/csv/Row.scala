package io.strixvaria.csv

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}

trait Row {
  lazy val readCursor = ReadCursor(this)
  def values: Seq[String]
  def lineNum: Option[Int] = None
  def size: Int = values.size
  def apply(idx: Int): String = values(idx)
  def read[A](layout: Layout[A]): Try[A] = Try(layout()(readCursor))
}

object Row {
  def apply(values: String*): Row = Row(values.toIndexedSeq)

  def apply(seq: IndexedSeq[String], lineNum: Option[Int] = None): Row = {
    val _lineNum = lineNum
    new Row { 
      override def values = seq
      override def lineNum = _lineNum
    }
  }
}

trait RowBuffer extends Row {
  lazy val writeCursor = WriteCursor(this)

  def update(idx: Int, v: String): Unit

  def write[A](x: A, layout: Layout[A]): Unit =
    (layout := x)(writeCursor)
}

object RowBuffer {
  def apply(size: Int): RowBuffer = {
    val buf = new ArrayBuffer[String](size)
    var i = 0
    while (i < size) {
      buf += ""
      i += 1
    }
    RowBuffer(buf)
  }

  def apply(buf: ArrayBuffer[String]): RowBuffer =
    new RowBuffer { 
      override def values: Seq[String] = buf
      override def update(idx: Int, v: String): Unit = buf(idx) = v
    }
}

sealed trait ReadCursor {
  /**
   * Returns the `Row` being read.
   */
  def row: Row

  def apply(offset: Int): Try[String]

  def read[A](offset: Int)(implicit f: ValueFormat[String, A]): Try[A] = 
    this(offset).flatMap(f.to)

  def slice(from: Int, until: Int): ReadCursor
}

sealed trait WriteCursor {
  def update(offset: Int, v: String): Unit

  def write[A](offset: Int, x: A)(implicit f: ValueFormat[String, A]): Try[Unit] = 
    f.from(x).map(this(offset) = _)

  def slice(from: Int, until: Int): WriteCursor
}

object ReadCursor {
  def apply(row: Row): ReadCursor =
    apply(row, 0, row.size)

  def apply(row: Row, start: Int, end: Int): ReadCursor = {
    val _row = row
    new ReadCursor {
      override def row = _row

      override def apply(offset: Int): Try[String] = {
        val idx = start + offset
        if (idx >= 0 && idx < end) Success(row(idx))
        else Failure(new IndexOutOfBoundsException(idx.toString))
      }

      override def slice(from: Int, until: Int): ReadCursor =
        ReadCursor(row, start + from, end.min(start + until))
    }
  }
}

object WriteCursor {
  def apply(row: RowBuffer): WriteCursor =
    apply(row, 0, row.size)

  def apply(row: RowBuffer, start: Int, end: Int): WriteCursor =
    new WriteCursor {
      override def update(offset: Int, v: String): Unit = {
        val idx = start + offset
        if (idx >= 0 && idx < end) row(idx) = v
        else throw new IndexOutOfBoundsException(idx.toString)
      }

      override def slice(from: Int, until: Int): WriteCursor =
        WriteCursor(row, start + from, end.min(start + until))
    }
}
