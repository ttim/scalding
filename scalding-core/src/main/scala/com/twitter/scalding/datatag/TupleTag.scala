package com.twitter.scalding.datatag

trait TupleTag

object TupleTag {

  final case class _1[T1](component1: DataTag[T1]) extends ProductTag._1[Tuple1[T1], T1] with TupleTag {
    override val arity: Int = 1
    override val components: ProductTag.Components = ProductTag.Components.Unnamed(List(component1))

    override def toProduct(value: Tuple1[T1]): Product1[T1] = value

    override def fromProduct(product: Product1[T1]): Tuple1[T1] =
      if (product.isInstanceOf[Tuple1[T1]]) product.asInstanceOf[Tuple1[T1]] else Tuple1(product._1)
  }

  final case class _2[T1, T2](
    component1: DataTag[T1],
    component2: DataTag[T2]
  ) extends ProductTag._2[Tuple2[T1, T2], T1, T2] with TupleTag {
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
  ) extends ProductTag._3[Tuple3[T1, T2, T3], T1, T2, T3] with TupleTag {
    override val arity: Int = 3
    override val components: ProductTag.Components = ProductTag.Components
      .Unnamed(List(component1, component2, component3))

    override def toProduct(value: Tuple3[T1, T2, T3]): Product3[T1, T2, T3] = value

    override def fromProduct(product: Product3[T1, T2, T3]): Tuple3[T1, T2, T3] =
      if (product.isInstanceOf[Tuple3[T1, T2, T3]]) product
        .asInstanceOf[Tuple3[T1, T2, T3]] else Tuple3(product._1, product._2, product._3)
  }
}

trait TupleTagImplicits {
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
}
