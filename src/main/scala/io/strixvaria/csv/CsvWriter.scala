package io.strixvaria.csv

import java.io.{Closeable, Writer}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

trait CsvWriter[A] extends Closeable {
  protected val defaultHeader = layout.header
  protected val columnCount = defaultHeader.size

  def layout: Layout[A]

  def writeDefaultHeader(): Unit = writeRow(defaultHeader)

  /**
   * Writes a raw `Row`, skipping formatting by `layout`.
   */
  def writeRow(row: Row): Unit

  /**
   * Writes a row by converting `x` to a `Row` using `layout`.
   */
  def write(x: A): Unit = {
    val buf = RowBuffer(columnCount)
    buf.write(x, layout)
    writeRow(buf)
  }

  def writeAll(xs: TraversableOnce[A]): Unit =
    xs.foreach(write)
}

object CsvWriter {
  def apply[A](
    out: Writer, 
    layout: Layout[A],
    csvFormat: CsvFormat = CsvFormat.default
  ): CsvWriter[A] = 
    apply(
      row => csvFormat.writeLine(row, out),
      layout,
      out
    )

  def apply[A](
    consumer: Row => Unit,
    layout: Layout[A],
    closer: Closeable
  ): CsvWriter[A] = {
    val _layout = layout
    new CsvWriter[A] {
      override def layout = _layout
      override def writeRow(row: Row): Unit = consumer(row)
      override def close(): Unit = closer.close()
    }
  }
}
