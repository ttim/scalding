package com.twitter.scalding.grouping

import com.twitter.scalding.grouping.Grouping.Containers.{
  ForOption,
  ForTuple2,
  ForTuple3
}
import com.twitter.scalding.grouping.Grouping.{ByOrdering, Generated}
import com.twitter.scalding.grouping.Grouping.Primitives.{
  ForInt,
  ForLong,
  ForString,
  ForUnit
}
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering

object GroupingFactory {
  def toOrdering[T](grouping: Grouping[T]): Ordering[T] =
    (grouping match {
      case ForUnit           => Ordering.Unit
      case ForString         => Ordering.String
      case ForLong           => Ordering.Long
      case ForInt            => Ordering.Int
      case ForOption(inner)  => Ordering.Option(toOrdering(inner))
      case ForTuple2(g1, g2) => Ordering.Tuple2(toOrdering(g1), toOrdering(g2))
      case ForTuple3(g1, g2, g3) =>
        Ordering.Tuple3(toOrdering(g1), toOrdering(g2), toOrdering(g3))
      case Generated(ord)  => ord
      case ByOrdering(ord) => ord
    }).asInstanceOf[Ordering[T]]

  def toOrderedSerialization[T](
      grouping: Grouping[T]): Option[OrderedSerialization[T]] =
    (grouping match {
      case ForUnit   => Some(BinaryOrdering.ordSer[Unit])
      case ForString => Some(BinaryOrdering.ordSer[String])
      case ForLong   => Some(BinaryOrdering.ordSer[Long])
      case ForInt    => Some(BinaryOrdering.ordSer[Int])
      case ForOption(inner) =>
        toOrderedSerialization(inner).map(optionOrdSer(_))
      case ForTuple2(g1, g2) =>
        for (ordSer1 <- toOrderedSerialization(g1);
             ordSer2 <- toOrderedSerialization(g2))
          tuple2OrdSer(ordSer1, ordSer2)
      case ForTuple3(g1, g2, g3) =>
        for (ordSer1 <- toOrderedSerialization(g1);
             ordSer2 <- toOrderedSerialization(g2);
             ordSer3 <- toOrderedSerialization(g3))
          tuple3OrdSer(ordSer1, ordSer2, ordSer3)
      case Generated(ord)                                               => Some(ord)
      case ByOrdering(ord) if ord.isInstanceOf[OrderedSerialization[_]] => Some(ord)
      case _                                                            => None
    }).asInstanceOf[Option[OrderedSerialization[T]]]

  def onlyOrderedSerialization[T](
    grouping: Grouping[T]
  ): Option[OrderedSerialization[T]] = grouping match {
    case ByOrdering(ord) if ord.isInstanceOf[OrderedSerialization[_]] =>
      Some(ord.asInstanceOf[OrderedSerialization[T]])
    case _ =>
      None
  }

  private def optionOrdSer[T: OrderedSerialization]
    : OrderedSerialization[Option[T]] =
    BinaryOrdering.ordSer[Option[T]]

  private def tuple2OrdSer[T1: OrderedSerialization, T2: OrderedSerialization]
    : OrderedSerialization[(T1, T2)] =
    BinaryOrdering.ordSer[(T1, T2)]

  private def tuple3OrdSer[T1: OrderedSerialization,
                           T2: OrderedSerialization,
                           T3: OrderedSerialization]
    : OrderedSerialization[(T1, T2, T3)] =
    BinaryOrdering.ordSer[(T1, T2, T3)]
}
