package com.twitter.scalding.source2

import com.twitter.bijection.Injection

trait Format[T]

object Format {
  case class Tsv[T](
    typeDescriptor: TypeDescriptor[T],
    separator: String,
    strict: Boolean,
    safe: Boolean
  ) extends Format[T]

  case class Lzo[T](format: Format[T]) extends Format[T]

  // Sequence file with Fields.FIRST
  case class Kryo[T]() extends Format[T]

  case class KeyValue[K, V](
    injection: Injection[(K, V), (Array[Byte], Array[Byte])],
    maxFailures: Int
  ) extends Format[(K, V)]
}
