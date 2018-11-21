package com.twitter.scalding.datatag

import com.twitter.scalding.macros.Macros
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering

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


trait TupleTag[T]

object TupleTag {

  final case class _1[T1](component1: DataTag[T1]) extends ProductTag._1[Tuple1[T1], T1] {
    override val arity: Int = 1
    override val components: ProductTag.Components = ProductTag.Components.Unnamed(List(component1))

    override def toProduct(value: Tuple1[T1]): Product1[T1] = value

    override def fromProduct(product: Product1[T1]): Tuple1[T1] =
      if (product.isInstanceOf[Tuple1[T1]]) product.asInstanceOf[Tuple1[T1]] else Tuple1(product._1)
  }

  final case class _2[T1, T2](
    component1: DataTag[T1],
    component2: DataTag[T2]
  ) extends ProductTag._2[Tuple2[T1, T2], T1, T2] {
    override val arity: Int = 2
    override val components: ProductTag.Components = ProductTag.Components
      .Unnamed(List(component1, component2))

    override def toProduct(value: Tuple2[T1, T2]): Product2[T1, T2] = value

    override def fromProduct(product: Product2[T1, T2]): Tuple2[T1, T2] =
      if (product.isInstanceOf[Tuple2[T1, T2]])
        product.asInstanceOf[Tuple2[T1, T2]]
      else
        Tuple2(product._1, product._2)
  }

  final case class _3[T1, T2, T3](
    component1: DataTag[T1],
    component2: DataTag[T2],
    component3: DataTag[T3]
  ) extends ProductTag._3[Tuple3[T1, T2, T3], T1, T2, T3] {
    override val arity: Int = 3
    override val components: ProductTag.Components = ProductTag.Components
      .Unnamed(List(component1, component2, component3))

    override def toProduct(value: Tuple3[T1, T2, T3]): Product3[T1, T2, T3] = value

    override def fromProduct(product: Product3[T1, T2, T3]): Tuple3[T1, T2, T3] =
      if (product.isInstanceOf[Tuple3[T1, T2, T3]]) product
        .asInstanceOf[Tuple3[T1, T2, T3]] else Tuple3(product._1, product._2, product._3)
  }
}


// TODO: add type tag (or maybe class tag?) as part of declaration?
// or guarantee that construct & deconstruct are equal generated for same class?
// define equality on CaseClassTag and UnionTag to be based on TypeTag & members? or just TypeTag?
// Is TypeTag serializable? Do we want DataTag to be serializable?
// write method which accepts TypeTag and returns Option[DataTag]?
sealed trait CaseClassTag[T] extends DataTag[T] {
  val arity: Int
  val components: List[CaseClassTag.Component[_]]
  def toArray(arg: T): Array[_]
  def fromArray(arr: Array[_]): T
}

//trait Format[Ref] {
//  def create(id: Ref): Any
//}
//case class SourceTag[Ref](format: Format[Ref], ref: Ref)
// for each thrift java, thrift scala and case classes it's possible to write: ClassTag => DataTag
object CaseClassTag {

  // add component get&put?
  final case class Component[T](name: String, tag: DataTag[T])

  // !!! Make CaseClass1 & others non final and serializable with writeReplace and with methods instead of fn params
  // Maybe unify with Tuple?
  // trait ScroogeTag // CaseClassTag { val clazz: Class[_] } , trait TupleTag
  // arity, getComponent(i), reflectConstruct, reflectDeconstruct
  // in the beginning enough to put enough for typedescriptor logic and grouping logic

  trait CaseClass1[T, T1] extends CaseClassTag[T] {
    val _1: Component[T1]

    def construct(arg: T1): T
    def deconstruct(arg: T): T1
  }

  final case class CaseClass2[T, T1, T2](
    _1: Component[T1],
    _2: Component[T2],

    construct: (T1, T2) => T,
    deconstruct: T => (T1, T2)
  ) extends CaseClassTag[T]

  final case class CaseClass3[T, T1, T2, T3](
    _1: Component[T1],
    _2: Component[T2],
    _3: Component[T3],

    construct: (T1, T2, T3) => T,
    deconstruct: T => (T1, T2, T3)
  ) extends CaseClassTag[T]
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

trait Implicits {
  // Primitives
  implicit val int: PrimitiveTag[Int] = PrimitiveTag.Int
  implicit val float: PrimitiveTag[Float] = PrimitiveTag.Float
  implicit val double: PrimitiveTag[Double] = PrimitiveTag.Double

  implicit val string: PrimitiveTag[String] = PrimitiveTag.String

  // Containers
  implicit def option[T: DataTag]: ContainerTag.Option[T] = ContainerTag.Option(implicitly[DataTag[T]])

  // Tuples
  implicit def tuple1[T1: DataTag]: TupleTag._1[T1] = TupleTag._1(implicitly[DataTag[T1]])

  implicit def tuple2[T1: DataTag, T2: DataTag]: TupleTag._2[T1, T2] = TupleTag._2(
    implicitly[DataTag[T1]],
    implicitly[DataTag[T2]]
  )

  implicit def tuple3[T1: DataTag, T2: DataTag, T3: DataTag]: TupleTag._3[T1, T2, T3] = TupleTag._3(
    implicitly[DataTag[T1]],
    implicitly[DataTag[T2]],
    implicitly[DataTag[T3]]
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
