package io.strixvaria.csv

import scala.collection.mutable.Buffer
import scala.util.{Try, Success, Failure}

case class Row(values: IndexedSeq[String], lineNum: Option[Int] = None) {
  lazy val cursor = ReadCursor(this)
  def size: Int = values.size
  def apply(idx: Int): String = values(idx)
}

case class RowBuffer(values: Buffer[String]) {
  def size: Int = values.size
  def update(idx: Int, v: String): Unit = values(idx) = v
}

sealed trait ReadCursor {
  /**
   * Returns the `Row` being read.
   */
  def row: Row

  def apply(offset: Int): Try[String]

  def read[A](offset: Int)(implicit f: ValueFormat[String, A]): Try[A] = 
    this(offset).flatMap(f.to)

  def read[A](layout: Layout[A]): Try[A] =
    Try(layout()(this))

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
