package com.twitter.scalding.grouping

import com.twitter.scalding.grouping.Grouping.Containers.{ForOption, ForTuple2, ForTuple3}
import com.twitter.scalding.grouping.Grouping.{ByOrdering, Generated}
import com.twitter.scalding.grouping.Grouping.Primitives.{ForInt, ForLong, ForString, ForUnit}

object KeyGroupingFactory {
  def toOrdering[T](grouping: Grouping[T]): Ordering[T] = (grouping match {
    case ForUnit => Ordering.Unit
    case ForString => Ordering.String
    case ForLong => Ordering.Long
    case ForInt => Ordering.Int
    case ForOption(inner) => Ordering.Option(toOrdering(inner))
    case ForTuple2(g1, g2) => Ordering.Tuple2(toOrdering(g1), toOrdering(g2))
    case ForTuple3(g1, g2, g3) => Ordering.Tuple3(toOrdering(g1), toOrdering(g2), toOrdering(g3))
    case Generated(ord) => ord
    case ByOrdering(ord) => ord
  }).asInstanceOf[Ordering[T]]

  //  def toOrderedSerialization[T](grouping: KeyGrouping[T]): Option[OrderedSerialization[T]] = (grouping match {
  //    case ForUnit => BinaryOrdering.ordSer[Unit]
  //    case ForString => BinaryOrdering.ordSer[String]
  //    case ForLong => BinaryOrdering.ordSer[Long]
  //    case ForInt => BinaryOrdering.ordSer[Int]
  //    case ForOption(inner) => toOrderedSerialization(inner).map(optionOrdSer(_))
  //    case ForTuple2(g1, g2) => toOrderedSerialization(g1).flatMap { ordG1 =>
  //      toOrderedSerialization(g2).map { ordG2 =>
  //        tuple2OrdSer(ordG1, ordG2)
  //      }
  //    }
  //    case Generated(ord) => ord
  //    case ByOrdering(ord) if ord.isInstanceOf[OrderedSerialization[_]] => ord
  //    case _ => None
  //  }).asInstanceOf[Option[OrderedSerialization[T]]]
  //
  //  private def optionOrdSer[T: OrderedSerialization]: OrderedSerialization[Option[T]] =
  //    BinaryOrdering.ordSer[Option[T]]
  //
  //  private def tuple2OrdSer[T1: OrderedSerialization, T2: OrderedSerialization]: OrderedSerialization[(T1, T2)] =
  //    BinaryOrdering.ordSer[(T1, T2)]
}
