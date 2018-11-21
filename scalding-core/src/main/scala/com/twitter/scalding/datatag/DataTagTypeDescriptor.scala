package com.twitter.scalding.datatag

import com.twitter.scalding.TypeDescriptor
import com.twitter.scalding.TypeDescriptor.typeDescriptor
import scala.util.{Failure, Success, Try}

//@implicitNotFound("""This class is used to bind together a Fields instance to an instance of type T. There is a implicit macro that generates a TypeDescriptor[T]
// for any type T where T is Boolean, String, Short, Int, Long, FLoat, or Double, or an option of these (with the exception of Option[String]),
// or a tuple or case class of a supported type. (Nested tuples and case classes are allowed.)
// Note: Option[String] specifically is not allowed as Some("") and None are indistinguishable.
// If your type T is not one of these, then you must write your own TypeDescriptor.""")
object DataTagTypeDescriptor {
  trait Pointer
  case object Root extends Pointer
  case class Tuple(index: Int, next: Pointer) extends Pointer
  case class Field(name: String, next: Pointer) extends Pointer

  case class Component(pointer: Pointer, isOption: Boolean, tag: PrimitiveTag[_])

  private def components(dataTag: DataTag[_]): Try[List[Component]] = dataTag match {
    case tag: PrimitiveTag[_] => Success(List(Component(Root, isOption = false, tag)))

    case ContainerTag.Option(inner) =>
      components(inner).flatMap { innerComponents =>
        if (innerComponents.forall(!_.isOption)) {
          Success(innerComponents.map(_.copy(isOption = true)))
        } else {
          Failure(new Exception("Can't have Option inside of Option"))
        }
      }

    case TupleTag.Tuple1(_1) =>
      for (cmp1 <- components(_1)
      ) yield repoint(Tuple(0, _))(cmp1)
    case TupleTag.Tuple2(_1, _2) =>
      for (cmp1 <- components(_1);
           cmp2 <- components(_2)
      ) yield repoint(Tuple(0, _))(cmp1) ++ repoint(Tuple(1, _))(cmp2)

    // TODO: case classes
  }

  def repoint(fn: Pointer => Pointer): List[Component] => List[Component] =
    components => components.map(comp => comp.copy(pointer = fn(comp.pointer)))

  def create[T](dataTag: DataTag[T]): Option[TypeDescriptor[T]] = ???

  def main(args: Array[String]): Unit = {
//    val t1 = create(typeTag[Int])
//    val t2 = create(typeTag[Boolean])
//    val t3 = create(typeTag[(Int, Int)])
//    val t4 = create(typeTag[(Int, Boolean)])
//    val t5 = create(typeTag[(Int, (Int, Boolean))])

////    val td1 = typeDescriptor[Option[(Int, Int)]]
////    val td2 = typeDescriptor[Option[String]]
////    val td3 = typeDescriptor[Option[(Int, Option[Int])]]
//    val td4 = typeDescriptor[T1]
//    val td5 = typeDescriptor[T2[Option[Int]]]
////    val td6 = typeDescriptor[T2[_]]
//    val td7 = typeDescriptor[T2[T2[Option[Int]]]]
//    val td8 = typeDescriptor[T2[(Int, Int)]]
//
//    val td9 = typeDescriptor[Int]
//
//    val td10 = typeDescriptor[(Int, (Int, Int))]
//
//    val td11 = typeDescriptor[T2[(Int, (Int, Int))]]
//
//    val td12 = typeDescriptor[(Int, T2[(Int, Int)])]
//
//    val c1 = components(implicitly[DataTag[Int]])
//    val c2 = components(implicitly[DataTag[Option[Int]]])
//    val c3 = components(implicitly[DataTag[(Int, Int)]])
//    val c4 = components(implicitly[DataTag[(Int, (Option[Int], String))]])
//    val c5 = components(implicitly[DataTag[Option[(Int, Int)]]])
//
//
//    val t7 = ???
  }

  case class T1(a: Int)
  case class T2[A](a: A)
}
