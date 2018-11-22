package com.twitter.scalding.datatag

// TODO: add type tag (or maybe class tag?) as part of declaration?
// or guarantee that construct & deconstruct are equal generated for same class?
// define equality on CaseClassTag and UnionTag to be based on TypeTag & members? or just TypeTag?
// Is TypeTag serializable? Do we want DataTag to be serializable?
// write method which accepts TypeTag and returns Option[DataTag]?
trait CaseClassTag[T, P <: Product] {
  val arity: Int
  val components: ProductTag.Components

  def toProduct(value: T): P
  def fromProduct(product: P): T
}

object CaseClassTag {
  // in the beginning enough to describe typedescriptor logic and grouping logic

  // This is base classes for generated case class tags. Most likely they aren't needed
  trait _1[T, T1] extends CaseClassTag[T, Product1[T1]] with ProductTag._1[T, T1]
  trait _2[T, T1, T2] extends CaseClassTag[T, Product2[T1, T2]] with ProductTag._2[T, T1, T2]
  trait _3[T, T1, T2, T3] extends CaseClassTag[T, Product3[T1, T2, T3]] with ProductTag._3[T, T1, T2, T3]
}
