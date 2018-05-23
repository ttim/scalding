package com.twitter.scalding.grouping

import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering
import org.scalatest.FunSuite

class A(val id: Long)

sealed case class Aa(id: Long)

class GroupingTest extends FunSuite {
  test("key grouping implicit resolution works") {
    {
      assert(implicitly[Grouping[Unit]] == Grouping.Primitives.ForUnit)
    }

    {
      implicit val ord: Ordering[Unit] = Ordering.Unit
      assert(implicitly[Grouping[Unit]] == Grouping.ByOrdering(ord))
    }

    {
      assert(
        implicitly[Grouping[(Unit, Int)]] == Grouping.Containers
          .ForTuple2(Grouping.Primitives.ForUnit,
            Grouping.Primitives.ForInt))
    }

    {
      implicit val ord: Ordering[Unit] = Ordering.Unit
      assert(implicitly[Grouping[(Unit, Int)]] == Grouping.Containers
        .ForTuple2(Grouping.ByOrdering(ord), Grouping.Primitives.ForInt))
    }

    {
      implicit val ord: Ordering[(Unit, Int)] =
        BinaryOrdering.ordSer[(Unit, Int)]
      assert(
        implicitly[Grouping[(Unit, Int)]] == Grouping.ordering(ord))
    }

    {
      implicit val unitOrd: Ordering[Unit] = BinaryOrdering.ordSer[Unit]
      implicit val pairOrd: Ordering[(Unit, Unit)] =
        BinaryOrdering.ordSer[(Unit, Unit)]
      assert(
        implicitly[Grouping[(Unit, Unit)]] == Grouping.ordering(
          pairOrd))
    }

    {
      implicit val unitGrouping: Grouping[Unit] =
        Grouping.ByOrdering(Ordering.Unit)
      assert(
        implicitly[Grouping[(Unit, Int)]] == Grouping.Containers
          .ForTuple2(unitGrouping, Grouping.Primitives.ForInt))
    }

    {
      implicit val ordSer: OrderedSerialization[Unit] = BinaryOrdering.ordSer[Unit]
      assert(implicitly[Grouping[Unit]] == Grouping.ByOrdering(ordSer))
    }

    {
      implicit val unitGrouping: Grouping[Unit] =
        Grouping.ByOrdering(Ordering.Unit)
      implicit val ord: Ordering[(Unit, Unit)] =
        BinaryOrdering.ordSer[(Unit, Unit)]
      assert(
        implicitly[Grouping[(Unit, Unit)]] == Grouping.ByOrdering(ord))
    }

    {
      implicit val grouping: Grouping[A] =
        Grouping.ByOrdering(Ordering.by(_.id))
      assert(
        implicitly[Grouping[(A, A)]] == Grouping.Containers
          .ForTuple2(grouping, grouping))
    }

    {
      assert(implicitly[Grouping[Aa]].isInstanceOf[Grouping.Generated[_]])
    }

    {
      implicit val intGrouping: Grouping[Int] = Grouping.ordering[Int]
      assert(implicitly[Grouping[(Int, String, Unit)]] == Grouping.Containers.ForTuple3(
        intGrouping,
        Grouping.Primitives.ForString,
        Grouping.Primitives.ForUnit))
    }

    {
      assert(implicitly[Grouping[Option[String]]] == Grouping.Containers.ForOption(
        Grouping.Primitives.ForString))
    }
  }
}
