package io.strixvaria.csv

case class Row(values: IndexedSeq[String], line: Int = -1) {
  def size: Int = values.size
  def apply(idx: Int): String = values(idx)
}
