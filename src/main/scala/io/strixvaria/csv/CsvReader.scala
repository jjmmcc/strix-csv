package io.strixvaria.csv

import java.io.Closeable
import scala.io.Source
import scala.util.Try

trait CsvReader[A] extends Closeable {
  def layout: Layout[A]

  /**
   * An `Iterator` of the raw `Row`s (not parsed via `layout`).
   */
  def rows: Iterator[Row]

  /** 
   * Returns the next `Row` as a header, instead of parsing it via `layout`. 
   * Throws `NoSuchElementException` if there are no more rows to read.
   */
  def readHeader: Row = rows.next

  /**
   * An `Iterator` over the results of parsing `Row`s with `layout`.
   */
  def values: Iterator[Try[A]] = rows.map(_.read(layout))

  /**
   * Returns a new `CsvReader` with a different `Layout`, but reading from the same
   * underlying `Iterator`.  This can be useful if you dynamically determine the layout
   * by first reading the header.
   */
  def withLayout[B](layout: Layout[B]): CsvReader[B] =
    CsvReader(rows, layout, this)
}

object CsvReader {
  def apply[A](
    source: Source,
    layout: Layout[A],
    format: CsvFormat = CsvFormat.default
  ): CsvReader[A] =
    CsvReader(format.read(source), layout, source)

  def apply[A](rows: Iterator[Row], layout: Layout[A]): CsvReader[A] = 
    CsvReader(rows, layout, NullCloseable)

  def apply[A](
    rows: Iterator[Row],
    layout: Layout[A],
    closer: Closeable
  ): CsvReader[A] = {
    val _rows = rows
    val _layout = layout
    new CsvReader[A] {
      override def layout = _layout
      override def rows = _rows
      override def close() = closer.close()
    }
  }

  object NullCloseable extends Closeable {
    override def close(): Unit = ()
  }
}
