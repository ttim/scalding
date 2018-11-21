package com.twitter.scalding.datatag

sealed trait Choice {
  val value: Any
}

sealed trait Choice1[T1] extends Choice
object Choice1 {
  sealed case class _1[T1](value: T1) extends Choice1[T1]
}

sealed trait Choice2[T1, T2] extends Choice
object Choice2 {
  sealed case class _1[T1, T2](value: T1) extends Choice2[T1, T2]
  sealed case class _2[T1, T2](value: T2) extends Choice2[T1, T2]
}

sealed trait Choice3[T1, T2, T3] extends Choice
object Choice3 {
  sealed case class _1[T1, T2, T3](value: T1) extends Choice3[T1, T2, T3]
  sealed case class _2[T1, T2, T3](value: T2) extends Choice3[T1, T2, T3]
  sealed case class _3[T1, T2, T3](value: T3) extends Choice3[T1, T2, T3]
}

sealed trait Choice4[T1, T2, T3, T4] extends Choice
object Choice4 {
  sealed case class _1[T1, T2, T3, T4](value: T1) extends Choice4[T1, T2, T3, T4]
  sealed case class _2[T1, T2, T3, T4](value: T2) extends Choice4[T1, T2, T3, T4]
  sealed case class _3[T1, T2, T3, T4](value: T3) extends Choice4[T1, T2, T3, T4]
  sealed case class _4[T1, T2, T3, T4](value: T4) extends Choice4[T1, T2, T3, T4]
}

sealed trait Choice5[T1, T2, T3, T4, T5] extends Choice
object Choice5 {
  sealed case class _1[T1, T2, T3, T4, T5](value: T1) extends Choice5[T1, T2, T3, T4, T5]
  sealed case class _2[T1, T2, T3, T4, T5](value: T2) extends Choice5[T1, T2, T3, T4, T5]
  sealed case class _3[T1, T2, T3, T4, T5](value: T3) extends Choice5[T1, T2, T3, T4, T5]
  sealed case class _4[T1, T2, T3, T4, T5](value: T4) extends Choice5[T1, T2, T3, T4, T5]
  sealed case class _5[T1, T2, T3, T4, T5](value: T5) extends Choice5[T1, T2, T3, T4, T5]
}
