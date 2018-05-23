package com.twitter.scalding.grouping

import com.twitter.scalding.serialization.OrderedSerialization
import scala.math.Ordering
import scala.language.experimental.macros

sealed trait Grouping[T] extends Equiv[T]

object Grouping extends GroupingImplicits {
  object Primitives {
    case object ForUnit extends Grouping[Unit] {
      override def equiv(x: Unit, y: Unit): Boolean = true
    }

    case object ForString extends Grouping[String] {
      override def equiv(x: String, y: String): Boolean = x == y
    }

    case object ForLong extends Grouping[Long] {
      override def equiv(x: Long, y: Long): Boolean = x == y
    }

    case object ForInt extends Grouping[Int] {
      override def equiv(x: Int, y: Int): Boolean = x == y
    }
  }

  object Containers {
    sealed case class ForOption[T](inner: Grouping[T])
      extends Grouping[Option[T]] {
      override def equiv(x: Option[T], y: Option[T]): Boolean = (x, y) match {
        case (Some(_x), Some(_y)) => inner.equiv(_x, _y)
        case (Some(_x), None) => false
        case (None, Some(_y)) => false
        case (None, None) => true
      }
    }

    sealed case class ForTuple2[T1, T2](g1: Grouping[T1], g2: Grouping[T2])
      extends Grouping[(T1, T2)] {
      override def equiv(x: (T1, T2), y: (T1, T2)): Boolean =
        g1.equiv(x._1, y._1) && g2.equiv(x._2, y._2)
    }

    sealed case class ForTuple3[T1, T2, T3](g1: Grouping[T1], g2: Grouping[T2], g3: Grouping[T3])
      extends Grouping[(T1, T2, T3)] {
      override def equiv(x: (T1, T2, T3), y: (T1, T2, T3)): Boolean =
        g1.equiv(x._1, y._1) && g2.equiv(x._2, y._2) && g3.equiv(x._3, y._3)
    }
  }

  sealed case class Generated[T](ord: OrderedSerialization[T]) extends Grouping[T] {
    override def equiv(x: T, y: T): Boolean = ord.equiv(x, y)
  }

  sealed case class ByOrdering[T](ord: Ordering[T]) extends Grouping[T] {
    override def equiv(x: T, y: T): Boolean = ord.equiv(x, y)
  }
}

trait GroupingImplicits extends LowPriorityGroupingImplicits {
  implicit def unit: Grouping[Unit] = macro GroupingMacro.Primitive.unit
  implicit def int: Grouping[Int] = macro GroupingMacro.Primitive.int
  implicit def long: Grouping[Long] = macro GroupingMacro.Primitive.long
  implicit def string: Grouping[String] = macro GroupingMacro.Primitive.string

  implicit def option[T]: Grouping[Option[T]] = macro GroupingMacro.Container.option[T]

  implicit def tuple2[T1, T2]: Grouping[(T1, T2)] = macro GroupingMacro.Container.tuple2[T1, T2]
  implicit def tuple3[T1, T2, T3]: Grouping[(T1, T2, T3)] = macro GroupingMacro.Container.tuple3[T1, T2, T3]

  implicit def ordering[T: Ordering]: Grouping[T] =
    Grouping.ByOrdering(implicitly[Ordering[T]])

  // This one is needed for explicitly provided Ordering.
  implicit def orderingExplicit[T](ord: Ordering[T]): Grouping[T] =
    Grouping.ByOrdering(ord)
}

// Needs to be separately to not conflict with `ordering` implicit.
trait LowPriorityGroupingImplicits {
  implicit def generate[T]: Grouping[T] = macro GroupingMacro.generate[T]
}
