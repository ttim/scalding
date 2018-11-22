package com.twitter.scalding.datatag

sealed trait CoProduct {
  val value: Any
}

sealed trait CoProduct1[T1] extends CoProduct
object CoProduct1 {
  final case class _1[T1](value: T1) extends CoProduct1[T1]
}

sealed trait CoProduct2[T1, T2] extends CoProduct
object CoProduct2 {
  final case class _1[T1, T2](value: T1) extends CoProduct2[T1, T2]
  final case class _2[T1, T2](value: T2) extends CoProduct2[T1, T2]
}

sealed trait CoProduct3[T1, T2, T3] extends CoProduct
object CoProduct3 {
  final case class _1[T1, T2, T3](value: T1) extends CoProduct3[T1, T2, T3]
  final case class _2[T1, T2, T3](value: T2) extends CoProduct3[T1, T2, T3]
  final case class _3[T1, T2, T3](value: T3) extends CoProduct3[T1, T2, T3]
}

sealed trait CoProduct4[T1, T2, T3, T4] extends CoProduct
object CoProduct4 {
  final case class _1[T1, T2, T3, T4](value: T1) extends CoProduct4[T1, T2, T3, T4]
  final case class _2[T1, T2, T3, T4](value: T2) extends CoProduct4[T1, T2, T3, T4]
  final case class _3[T1, T2, T3, T4](value: T3) extends CoProduct4[T1, T2, T3, T4]
  final case class _4[T1, T2, T3, T4](value: T4) extends CoProduct4[T1, T2, T3, T4]
}

sealed trait CoProduct5[T1, T2, T3, T4, T5] extends CoProduct
object CoProduct5 {
  final case class _1[T1, T2, T3, T4, T5](value: T1) extends CoProduct5[T1, T2, T3, T4, T5]
  final case class _2[T1, T2, T3, T4, T5](value: T2) extends CoProduct5[T1, T2, T3, T4, T5]
  final case class _3[T1, T2, T3, T4, T5](value: T3) extends CoProduct5[T1, T2, T3, T4, T5]
  final case class _4[T1, T2, T3, T4, T5](value: T4) extends CoProduct5[T1, T2, T3, T4, T5]
  final case class _5[T1, T2, T3, T4, T5](value: T5) extends CoProduct5[T1, T2, T3, T4, T5]
}
