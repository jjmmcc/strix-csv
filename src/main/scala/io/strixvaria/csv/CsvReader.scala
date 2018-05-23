package io.strixvaria.csv

import scala.io.Source
import scala.util.Try

case class CsvReader[A](stream: Stream[Row], layout: Layout[A]) {
  def readHeader: (Row, CsvReader[A]) =
    stream match {
      case header #:: tail => (header, copy(stream = tail))
      case Stream.Empty => throw new ParseException(0, "Stream is empty")
    }

  def skipHeader: CsvReader[A] = readHeader._2

  def toStream: Stream[Try[A]] =
    stream.map(_.cursor.read(layout))

  override protected def finalize() {
    println(s"*** finalizing: $this ***")
  }
}

object CsvReader {
  def apply[A](
    source: Source,
    layout: Layout[A],
    format: CsvFormat = CsvFormat.default
  ): CsvReader[A] =
    CsvReader(format.read(source), layout)

  def raw(
    source: Source,
    format: CsvFormat = CsvFormat.default
  ): CsvReader[Row] =
    CsvReader(format.read(source), RawLayout)
}
