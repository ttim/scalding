package com.twitter.scalding.datatag

// Generated case class construct classes should have same logic as this
// This will establish contract on what's generated
object CaseClassGenerator {
  trait Construct {
    val className: String
    val generatorVersion: Int // ???

    def construct(args: List[AnyRef]): AnyRef = ???

    override def toString: String = s"GeneratedConstruct($className, version: $generatorVersion)"

    override def equals(obj: scala.Any): Boolean = obj match {
      case another: CaseClassGenerator.Construct =>
        another.className == className && another.generatorVersion == generatorVersion
      case _ =>
        false
    }

    override def hashCode: Int = toString.hashCode
  }

  trait Deconstruct {
    val className: String
    val generatorVersion: Int // ???

    def deconstruct(args: List[AnyRef]): AnyRef = ???

    override def toString: String = s"GeneratedDeconstruct($className, version: $generatorVersion)"

    override def equals(obj: scala.Any): Boolean = obj match {
      case another: CaseClassGenerator.Deconstruct =>
        another.className == className && another.generatorVersion == generatorVersion
      case _ =>
        false
    }

    override def hashCode: Int = toString.hashCode
  }
}
