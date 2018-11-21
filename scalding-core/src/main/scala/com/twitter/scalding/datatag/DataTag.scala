package com.twitter.scalding.datatag

import com.twitter.scalding.macros.Macros
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering

sealed trait DataTag[T]

// Containers

// Case classes macro?
// Union classes macro?

object DataTag extends PrimitiveTagImplicits with ContainerTagImplicits with TupleTagImplicits

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
  final case class Option[T](tag: DataTag[T]) extends ContainerTag[scala.Option[T]]
}

sealed trait ProductTag[T, P <: Product] extends DataTag[T] {
  val arity: Int
  val components: ProductTag.Components

  def toProduct(value: T): P
  def fromProduct(product: P): T
}

object ProductTag {
  sealed trait Components

  object Components {
    case class Named(components: List[(String, DataTag[_])]) extends Components
    case class Unnamed(components: List[DataTag[_]]) extends Components
  }

  trait _1[T, P1] extends ProductTag[T, Product1[P1]]
  trait _2[T, P1, P2] extends ProductTag[T, Product2[P1, P2]]
  trait _3[T, P1, P2, P3] extends ProductTag[T, Product3[P1, P2, P3]]
}

sealed trait UnionTag[T] extends DataTag[T]

object UnionTag {
  final case class Component[T](name: String, tag: DataTag[T])

  final case class Union1[T, T1](
    _1: Component[T1],

    construct: T1 => T,
    deconstruct: T => T1
  ) extends UnionTag[T]

  final case class Union2[T, T1, T2](
    _1: Component[T1],
    _2: Component[T2],

    construct: Choice2[T1, T2] => T,
    deconstruct: T => Choice2[T1, T2]
  ) extends UnionTag[T]

  final case class Union3[T, T1, T2, T3](
    _1: Component[T1],
    _2: Component[T2],
    _3: Component[T3],

    construct: Choice3[T1, T2, T3] => T,
    deconstruct: T => Choice3[T1, T2, T3]
  ) extends UnionTag[T]
}

trait PrimitiveTagImplicits {
  implicit val int: PrimitiveTag[Int] = PrimitiveTag.Int
  implicit val float: PrimitiveTag[Float] = PrimitiveTag.Float
  implicit val double: PrimitiveTag[Double] = PrimitiveTag.Double

  implicit val string: PrimitiveTag[String] = PrimitiveTag.String
}

trait ContainerTagImplicits {
  implicit def option[T: DataTag]: ContainerTag.Option[T] = ContainerTag.Option(implicitly[DataTag[T]])
}

object Test {
  def main(args: Array[String]): Unit = {
    val t1 = implicitly[DataTag[Int]]
    val t2 = implicitly[DataTag[Int]]
    val t3 = implicitly[DataTag[(Int, Int)]]
    val t4 = implicitly[DataTag[(Option[String], Int)]]

    implicit val t2DataTag: DataTag[T2] = implicitly[DataTag[Int]].asInstanceOf[DataTag[T2]]

    val t5 = Macros.caseClassDataTag[Test]
    val t6 = Macros.caseClassDataTag[Test2[Int]]
    val t8 = Macros.caseClassDataTag[Test2[T2]]
//    val t9 = Macros.caseClassDataTag[Test2[_]]
//    val t10 = Macros.caseClassDataTag[T2]
    val t11 = Macros.caseClassDataTag[T3]
//    val t12 = Macros.caseClassDataTag[Rec]

    val ord1 = BinaryOrdering.ordSer[Test]

    val t_ = ???
  }

  case class Test(t: Int)
  case class Test2[T](t: T)

  class T2 extends Test(1)

  case class T3(v1: Int, v2: String, v3: Double)

  case class Rec(v: Int, rec: Rec)

}
