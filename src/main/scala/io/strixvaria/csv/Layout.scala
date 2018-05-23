package io.strixvaria.csv

abstract class Layout[A] {
  def apply()(implicit cursor: ReadCursor): A
  def :=(x: A)(implicit cursor: WriteCursor): Unit
}

trait Column[A] extends Layout[A] {
  val name: String
  val valueFormat: ValueFormat[String, A]
}

private[csv] sealed trait Sublayout[T, A] extends Layout[A] {
  private[csv] val extractor: T => A
  private[csv] val offset: Int

  private[csv] def columnNames: Seq[String]

  private[csv] def writeSub(x: T)(implicit cur: WriteCursor): Unit =
    this := extractor(x)
}

private[csv] case class BoundColumn[T, A](
  name: String,
  valueFormat: ValueFormat[String, A],
  extractor: T => A,
  offset: Int
) extends Column[A] with Sublayout[T, A] {
  implicit val impFormat = valueFormat

  private[csv] override val columnNames: Seq[String] = Vector(name)

  override def apply()(implicit cursor: ReadCursor): A =
    cursor.read[A](offset).get

  override def :=(x: A)(implicit cursor: WriteCursor): Unit =
    cursor.write[A](offset, x).get
}

abstract class ColumnSet[A]() extends Layout[A] {
  private var subs = Vector.empty[Sublayout[A, _]]
  private var nextOffset: Int = 0

  def columnCount: Int = nextOffset
  def columnNames: Seq[String] = subs.flatMap(_.columnNames)

  def add[B](
    name: String,
    extractor: A => B
  )(implicit valueFormat: ValueFormat[String, B]): Layout[B] = {
    val col = BoundColumn[A, B](name, valueFormat, extractor, nextOffset)
    subs = subs :+ col
    nextOffset += 1
    col
  }

  def addAll[B](colset: ColumnSet[B], extractor: A => B, prefix: Option[String] = None): Layout[B] = {
    val sub = BoundColumnSet(colset, extractor, nextOffset, prefix)
    subs = subs :+ sub
    nextOffset += colset.columnCount
    sub
  }

  override def apply()(implicit cursor: ReadCursor): A =
    throw new UnsupportedOperationException(s"$this.read")

  override def :=(x: A)(implicit cursor: WriteCursor): Unit =
    subs.foreach(_.writeSub(x))
}

private[csv] case class BoundColumnSet[T, A](
  columnSet: ColumnSet[A],
  extractor: T => A,
  offset: Int,
  prefix: Option[String]
) extends Sublayout[T, A] {

  override def columnNames: Seq[String] =
    prefix match {
      case None => columnSet.columnNames
      case Some(pre) => columnSet.columnNames.map(pre + _)
    }

  override def apply()(implicit cursor: ReadCursor): A =
    columnSet()(cursor.slice(offset, offset + columnSet.columnCount))

  override def :=(x: A)(implicit cursor: WriteCursor): Unit =
    columnSet.:=(x)(cursor.slice(offset, offset + columnSet.columnCount))
}

object RawLayout extends Layout[Row] {
  def apply()(implicit cursor: ReadCursor): Row = cursor.row

  def :=(x: Row)(implicit cursor: WriteCursor): Unit =
    for (i <- 0.until(x.size)) {
      cursor(i) = x(i)
    }
}
