package io.strixvaria.csv

import scala.util.{Try, Success, Failure}

case class ValueFormat[A, B](
  to: A => Try[B],
  from: B => Try[A]
) {
  def ~>[C](next: ValueFormat[B, C]): ValueFormat[A, C] =
    ValueFormat[A, C](
      to = (x: A) => to(x).flatMap(next.to),
      from = (y: C) => next.from(y).flatMap(from)
    )

  def reverse: ValueFormat[B, A] = ValueFormat(from, to)
}

object ValueFormat {
  implicit val StringToString = 
    ValueFormat[String, String](
      s => Success(s),
      s => Success(s)
    )
 
  implicit val StringToBoolean = 
    ValueFormat[String, Boolean](
      s => Try(s.toBoolean),
      b => Success(b.toString)
    )
 
  implicit val StringToInt = 
    ValueFormat[String, Int](
      s => Try(s.toInt),
      i => Success(i.toString)
    )
 
  implicit val StringToLong = 
    ValueFormat[String, Long](
      s => Try(s.toLong),
      l => Success(l.toString)
    )

  def to[A, B](x: A)(implicit f: ValueFormat[A, B]): Try[B] = f.to(x)
  def from[A, B](y: B)(implicit f: ValueFormat[A, B]): Try[A] = f.from(y)

  def optional[A](implicit f: ValueFormat[String, A]): ValueFormat[String, Option[A]] =
    ValueFormat[String, Option[A]](
      str => if (str.length == 0) Success(None) else f.to(str).map(Some(_)),
      {
        case None => Success("")
        case Some(x) => f.from(x)
      }
    )

  def oneWay[A, B](to: A => Try[B]): ValueFormat[A, B] =
    ValueFormat(to, y => Failure(OneWayException(y)))

  def success[A, B](to: A => B, from: B => A): ValueFormat[A, B] =
    ValueFormat(x => Success(to(x)), y => Success(from(y)))

  def handle[A, B](to: A => B, from: B => A): ValueFormat[A, B] =
    ValueFormat(x => Try(to(x)), y => Try(from(y)))

  case class OneWayException(value: Any) 
    extends Exception("Format is one-way, cannot covert: " + value)
}
