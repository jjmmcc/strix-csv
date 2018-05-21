package io.strixvaria.csv

import java.io.Writer
import scala.util.Try

case class CsvWriter[A](
  out: Writer, 
  valueFormat: ValueFormat[A, Seq[String]],
  csvFormat: CsvFormat
) {
  def writeHeader(values: Seq[String]) =
    csvFormat.writeLine(values, out)

  def writeRow(row: A): Unit =
    csvFormat.writeLine(valueFormat.to(row).get, out)

  def writeRows(rows: Stream[A]) =
    rows.foreach(writeRow(_))

  def <~[B](f2: ValueFormat[B, A]): CsvWriter[B] =
    copy(valueFormat = f2 ~> valueFormat)
}

object CsvWriter {
  def apply[A](out: Writer, csvFormat: CsvFormat = CsvFormat.default)(
    implicit valueFormat: ValueFormat[A, Seq[String]]
  ): CsvWriter[A] =
    CsvWriter(out, valueFormat, csvFormat)
}
