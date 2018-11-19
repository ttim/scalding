package com.twitter.scalding.source2

import com.twitter.bijection.Bijection

// grouping from typed descriptor if ordering is regular
// grouping from ordering if ordering is non trivial
// search for typed descriptor in cats & co
trait TypeDescriptor[T]

object TypeDescriptor {
  case object BooleanTD extends TypeDescriptor[Boolean]
  case object ShortTD extends TypeDescriptor[Short]
  case object IntTD extends TypeDescriptor[Int]
  case object LongTD extends TypeDescriptor[Long]
  case object FloatTD extends TypeDescriptor[Float]
  case object DoubleTD extends TypeDescriptor[Double]

  case class Bijected[A, B](inner: TypeDescriptor[A], bijection: Bijection[A, B])
    extends TypeDescriptor[B]

  case class Tuple2[T1, T2](
    td1: TypeDescriptor[T1],
    td2: TypeDescriptor[T2]
  ) extends TypeDescriptor[(T1, T2)]

  case class CaseClass1[T, T1](
    deconstruct: T => (T1),
    construct: (T1) => T,

    field1: (String, TypeDescriptor[T1])
  )

  case class CaseClass2[T, T1, T2](
    deconstruct: T => (T1, T2),
    construct: (T1, T2) => T,

    field1: (String, TypeDescriptor[T1]),
    field2: (String, TypeDescriptor[T2])
  )
}
