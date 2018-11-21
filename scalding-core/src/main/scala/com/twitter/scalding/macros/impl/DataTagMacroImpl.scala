package com.twitter.scalding.macros.impl

import com.twitter.scalding.datatag.{CaseClassTag, DataTag}
import scala.reflect.macros.blackbox

object DataTagMacroImpl {
  def caseClassDataTagImpl[T](c: blackbox.Context)(implicit T: c.WeakTypeTag[T]): c.Expr[DataTag[T]] = {
    import c.universe._

    @annotation.tailrec
    def normalized(tpe: Type): Type = {
      val norm = tpe.normalize
      if (!(norm =:= tpe))
        normalized(norm)
      else
        tpe
    }

    def dataTagType(tpe: Type): Type =
      typeOf[DataTag[scala.Unit]].map { t =>
        if (t <:< typeOf[scala.Unit]) tpe else t
      }

    def dataTag(tpe: Type): Tree =
      c.inferImplicitValue(dataTagType(tpe))

    val tpe = normalized(T.tpe)
    if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) {
      // todo: order?
      val fields = tpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { accessorMethod =>
          val fieldName = accessorMethod.name
          val fieldType = normalized(accessorMethod.returnType.asSeenFrom(tpe, tpe.typeSymbol.asClass))
          val dt = dataTag(fieldType)
          (fieldName, dt)
        }
      val brokenFields = fields.filter(_._2 == EmptyTree)
      if (brokenFields.isEmpty) {
        val components = fields.map { case (name, dt) =>
          val nameStr = name.toString
          q"""com.twitter.scalding.datatag.CaseClassTag.Component($nameStr, $dt)"""
        }
        val caseClassTagType = c.typeOf[CaseClassTag.type].member(c.universe.TermName(s"CaseClass${components.size}"))
        val args: List[c.universe.Tree] = components.toList ++ List(q"""null""", q"""null""")
        c.Expr[DataTag[T]](q"""$caseClassTagType(...${List(args)})""")
      } else {
        c.abort(c.enclosingPosition, s"Can't generate data tag for $tpe, " +
          s"no data tags defined for fields: ${brokenFields.map(_._1).mkString(", ")}.")
      }
    } else {
      c.abort(c.enclosingPosition, s"$tpe isn't a case class")
    }
  }
}
