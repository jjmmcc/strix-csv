package io.strixvaria.csv

import scala.util.Try

abstract class Select {
  def apply(row: Row): Try[String]

  def ~>[A](format: ValueFormat[String, A]): ValueFormat[Row, A] =
    ValueFormat.oneWay[Row, A](row => this(row).flatMap(format.to))
}

object Select {
  def apply(index: Int): Select =
    new Select {
      def apply(row: Row): Try[String] = Try(row(index))
    }
}
