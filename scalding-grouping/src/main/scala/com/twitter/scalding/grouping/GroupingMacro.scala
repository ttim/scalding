package com.twitter.scalding.grouping

import scala.reflect.macros.blackbox

object GroupingMacro {
  object Primitive {
    def unit(c: blackbox.Context): c.Expr[Grouping[Unit]] = primitive(c)(
      c.typeOf[Ordering.Unit.type],
      c.typeOf[Grouping.Primitives.ForUnit.type])

    def int(c: blackbox.Context): c.Expr[Grouping[Int]] = primitive(c)(
      c.typeOf[Ordering.Int.type],
      c.typeOf[Grouping.Primitives.ForInt.type])

    def long(c: blackbox.Context): c.Expr[Grouping[Long]] = primitive(c)(
      c.typeOf[Ordering.Long.type],
      c.typeOf[Grouping.Primitives.ForLong.type])

    def string(c: blackbox.Context): c.Expr[Grouping[String]] = primitive(c)(
      c.typeOf[Ordering.String.type],
      c.typeOf[Grouping.Primitives.ForString.type])

    private def primitive[T](c: blackbox.Context)(from: c.Type, to: c.Type)(
      implicit T: c.WeakTypeTag[T]): c.Expr[Grouping[T]] = {
      import c.universe._

      val ordImplicit = ordering[T](c)(T)
      val tree = if (ordImplicit == EmptyTree || ordImplicit.symbol == from.termSymbol) {
        q"""${to.termSymbol}"""
      } else {
        groupingByOrdering(c)(ordImplicit)
      }

      c.Expr[Grouping[T]](tree)
    }
  }

  object Container {
    def option[T](c: blackbox.Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Grouping[Option[T]]] = {
      import c.universe._
      val ordImplicit = ordering[Option[T]](c)
      val ordTupleMethod = c.typeOf[Ordering.type].member(c.universe.TermName("Option"))

      val tree = if (ordImplicit == EmptyTree || ordImplicit.symbol == ordTupleMethod) {
        q"""${typeOf[Grouping.Containers.ForOption.type].termSymbol}(${grouping[T](c)})"""
      } else {
        groupingByOrdering(c)(ordImplicit)
      }

      c.Expr[Grouping[Option[T]]](tree)
    }

    def tuple2[T1, T2](c: blackbox.Context)(implicit T1: c.WeakTypeTag[T1], T2: c.WeakTypeTag[T2]): c.Expr[Grouping[(T1, T2)]] =
      tuple[(T1, T2)](c)(List(T1, T2))

    def tuple3[T1, T2, T3](c: blackbox.Context)(implicit T1: c.WeakTypeTag[T1], T2: c.WeakTypeTag[T2], T3: c.WeakTypeTag[T3]): c.Expr[Grouping[(T1, T2, T3)]] =
      tuple[(T1, T2, T3)](c)(List(T1, T2, T3))

    private def tuple[T](c: blackbox.Context)(es: List[c.WeakTypeTag[_]])(implicit T: c.WeakTypeTag[T]): c.Expr[Grouping[T]] = {
      import c.universe._
      val ordImplicit = ordering[T](c)

      val ordTupleMethod = c.typeOf[Ordering.type].member(c.universe.TermName(s"Tuple${es.size}"))
      val groupTupleType = c.typeOf[Grouping.Containers.type].member(c.universe.TermName(s"ForTuple${es.size}"))

      val tree = if (ordImplicit == EmptyTree || ordImplicit.symbol == ordTupleMethod) {
        val params = es.map { elementTypeTag => grouping(c)(elementTypeTag) }
        q"""$groupTupleType(...${List(params)})"""
      } else {
        groupingByOrdering(c)(ordImplicit)
      }

      c.Expr[Grouping[T]](tree)
    }
  }

  def generate[T](c: reflect.macros.Context)(
    implicit T: c.WeakTypeTag[T]): c.Expr[Grouping[T]] =
    c.Expr[Grouping[T]](groupingByOrdSer(c)(
      com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl.apply[T](c).tree))

  private def grouping[T](c: blackbox.Context)(implicit tag: c.WeakTypeTag[T]): c.Tree =
    c.inferImplicitValue(implicitly[c.WeakTypeTag[Grouping[T]]].tpe)

  private def ordering[T](c: blackbox.Context)(implicit tag: c.WeakTypeTag[T]): c.Tree =
    c.inferImplicitValue(implicitly[c.WeakTypeTag[Ordering[T]]].tpe)

  private def groupingByOrdering(c: blackbox.Context)(ord: c.Tree): c.Tree = {
    import c.universe._
    q"""${c.typeOf[Grouping.ByOrdering.type].termSymbol}($ord)"""
  }

  private def groupingByOrdSer(c: blackbox.Context)(ordSer: c.Tree): c.Tree = {
    import c.universe._
    q"""${c.typeOf[Grouping.Generated.type].termSymbol}($ordSer)"""
  }
}
