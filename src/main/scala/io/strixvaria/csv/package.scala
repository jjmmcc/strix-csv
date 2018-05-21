package io.strixvaria

import scala.util.Failure

package object csv {
  type RowParser[A] = ValueFormat[Row, A]

  implicit def rowParser[A](implicit format: ValueFormat[IndexedSeq[String], A]): RowParser[A] =
    ValueFormat[Row, A](
      row => 
        format.to(row.values)
          .recoverWith { case t => Failure[A](new ParseException(row.line, t)) },
      a => format.from(a).map(Row(_))
    )

  implicit val rawRowParser: RowParser[Row] =
    ValueFormat.success[Row, Row](identity(_), identity(_))
}

package csv {
  class ParseException(lineNum: Int, msg: String, cause: Throwable = null) 
    extends Exception(s"$lineNum: $msg", cause)
  {
    def this(lineNum: Int, cause: Throwable) = this(lineNum, "", cause)
  }
}