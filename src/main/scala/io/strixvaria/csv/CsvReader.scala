package io.strixvaria.csv

import scala.io.Source
import scala.util.Try

case class CsvReader[A](stream: Stream[Row], parser: RowParser[A]) {
  def readHeader: (Row, CsvReader[A]) =
    stream match {
      case header #:: tail => (header, copy(stream = tail))
      case Stream.Empty => throw new ParseException(0, "Stream is empty")
    }

  def skipHeader: CsvReader[A] = readHeader._2

  def toStream: Stream[Try[A]] =
    stream.map(parser.to)

  def ~>[B](f2: ValueFormat[A, B]): CsvReader[B] =
    copy(parser = parser ~> f2)
}

object CsvReader {
  def apply[A : RowParser](
    source: Source,
    format: CsvFormat = CsvFormat.default
  ): CsvReader[A] =
    CsvReader(format.read(source), implicitly[RowParser[A]])
}
