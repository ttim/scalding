package com.twitter.scalding.datatag

// Base trait for generated Product* tags for case classes
trait CaseClassTag


// TODO: add type tag (or maybe class tag?) as part of declaration?
// or guarantee that construct & deconstruct are equal generated for same class?
// define equality on CaseClassTag and UnionTag to be based on TypeTag & members? or just TypeTag?
// Is TypeTag serializable? Do we want DataTag to be serializable?
// write method which accepts TypeTag and returns Option[DataTag]?
sealed trait CaseClassTag[T] extends DataTag[T] {
  val arity: Int
  val components: List[CaseClassTag.Component[_]]
  def toArray(arg: T): Array[_]
  def fromArray(arr: Array[_]): T
}

//trait Format[Ref] {
//  def create(id: Ref): Any
//}
//case class SourceTag[Ref](format: Format[Ref], ref: Ref)
// for each thrift java, thrift scala and case classes it's possible to write: ClassTag => DataTag
object CaseClassTag {

  // add component get&put?
  final case class Component[T](name: String, tag: DataTag[T])

  // !!! Make CaseClass1 & others non final and serializable with writeReplace and with methods instead of fn params
  // Maybe unify with Tuple?
  // trait ScroogeTag // CaseClassTag { val clazz: Class[_] } , trait TupleTag
  // arity, getComponent(i), reflectConstruct, reflectDeconstruct
  // in the beginning enough to put enough for typedescriptor logic and grouping logic

  trait CaseClass1[T, T1] extends CaseClassTag[T] {
    val _1: Component[T1]

    def construct(arg: T1): T
    def deconstruct(arg: T): T1
  }

  final case class CaseClass2[T, T1, T2](
    _1: Component[T1],
    _2: Component[T2],

    construct: (T1, T2) => T,
    deconstruct: T => (T1, T2)
  ) extends CaseClassTag[T]

  final case class CaseClass3[T, T1, T2, T3](
    _1: Component[T1],
    _2: Component[T2],
    _3: Component[T3],

    construct: (T1, T2, T3) => T,
    deconstruct: T => (T1, T2, T3)
  ) extends CaseClassTag[T]
}
