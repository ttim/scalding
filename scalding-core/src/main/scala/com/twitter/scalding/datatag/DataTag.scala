package com.twitter.scalding.datatag

import scala.reflect.runtime.universe._

sealed trait DataTag[T]
object DataTag extends Implicits

sealed trait PrimitiveTag[T] extends DataTag[T]

object PrimitiveTag {
  case object Boolean extends PrimitiveTag[scala.Boolean]

  case object Short extends PrimitiveTag[scala.Short]
  case object Int extends PrimitiveTag[scala.Int]
  case object Long extends PrimitiveTag[scala.Long]

  case object Float extends PrimitiveTag[scala.Float]
  case object Double extends PrimitiveTag[scala.Double]

  case object String extends PrimitiveTag[scala.Predef.String]
}

sealed trait ContainerTag[T] extends DataTag[T]

object ContainerTag {
  sealed case class Option[T](tag: DataTag[T]) extends ContainerTag[scala.Option[T]]
}

sealed trait TupleTag[T] extends DataTag[T]

object TupleTag {
  sealed case class Tuple1[T1](_1: DataTag[T1]) extends TupleTag[scala.Tuple1[T1]]
  sealed case class Tuple2[T1, T2](_1: DataTag[T1], _2: DataTag[T2]) extends TupleTag[(T1, T2)]
}

// define equality on CaseClassTag and UnionTag to be based on TypeTag & members? or just TypeTag?
// Is TypeTag serializable? Do we want DataTag to be serializable?
// write method which accepts TypeTag and returns Option[DataTag]?
sealed trait CaseClassTag[T] extends DataTag[T] {
  val typeTag: TypeTag[T]
}

object CaseClassTags {
  case class Component[T](name: String, tag: DataTag[T])

  case class CaseClass1[T, T1](
    typeTag: TypeTag[T],

    _1: Component[T1],

    construct: T1 => T,
    deconstruct: T => T1
  ) extends CaseClassTag[T]

  case class CaseClass2[T, T1, T2](
    typeTag: TypeTag[T],

    _1: Component[T1],
    _2: Component[T2],

    construct: (T1, T2) => T,
    deconstruct: T => (T1, T2)
  ) extends CaseClassTag[T]
}

sealed trait UnionTag[T] extends DataTag[T] {
  val typeTag: TypeTag[T]
}

object UnionTag {
  case class Component[T](name: String, tag: DataTag[T])

  case class Union1[T, T1](
    typeTag: TypeTag[T],

    _1: Component[T1],

    construct: T1 => T,
    deconstruct: T => T1
  ) extends UnionTag[T]

  case class Union2[T, T1, T2](
    typeTag: TypeTag[T],

    _1: Component[T1],
    _2: Component[T2],

    construct: Choice2[T1, T2] => T,
    deconstruct: T => Choice2[T1, T2]
  ) extends UnionTag[T]

  case class Union3[T, T1, T2, T3](
    typeTag: TypeTag[T],

    _1: Component[T1],
    _2: Component[T2],
    _3: Component[T3],

    construct: Choice3[T1, T2, T3] => T,
    deconstruct: T => Choice3[T1, T2, T3]
  ) extends UnionTag[T]
}

trait Implicits {
  // Primitives
  implicit val int: PrimitiveTag[Int] = PrimitiveTag.Int
  implicit val float: PrimitiveTag[Float] = PrimitiveTag.Float
  implicit val double: PrimitiveTag[Double] = PrimitiveTag.Double

  implicit val string: PrimitiveTag[String] = PrimitiveTag.String

  // Containers
  implicit def option[T: DataTag]: ContainerTag[Option[T]] = ContainerTag.Option(implicitly[DataTag[T]])

  // Tuples
  implicit def tuple1[T1: DataTag]: TupleTag[Tuple1[T1]] = TupleTag.Tuple1(implicitly[DataTag[T1]])
  implicit def tuple2[T1: DataTag, T2: DataTag]: TupleTag[Tuple2[T1, T2]] = TupleTag.Tuple2(
    implicitly[DataTag[T1]],
    implicitly[DataTag[T2]]
  )

  // Case classes macro?
  // Union classes macro?
}

object Test {
  def main(args: Array[String]): Unit = {
    val t1 = implicitly[DataTag[Int]]
    val t2 = implicitly[DataTag[Int]]
    val t3 = implicitly[DataTag[(Int, Int)]]
    val t4 = implicitly[DataTag[(Option[String], Int)]]

    val t_ = ???
  }
}
