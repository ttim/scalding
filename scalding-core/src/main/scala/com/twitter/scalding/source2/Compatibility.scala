package com.twitter.scalding.source2

import com.twitter.scalding._
import org.apache.hadoop.fs.Path

// Various sources which were implemented before implemented as sources2.

/**
 * This object gives you easy access to text formats (possibly LZO compressed) by
 * using a case class to describe the field names and types.
 */
object TypedText {
  case class TypedSep(str: String) extends AnyVal

  val TAB = TypedSep("\t")
  val ONE = TypedSep("\u0001")
  val COMMA = TypedSep(",")

  // fixed path
  def tsv[T: TypeDescriptor](path: String*): SourceSink[T] = Sources.FixedPath(format(TAB), path)
  def osv[T: TypeDescriptor](path: String*): SourceSink[T] = Sources.FixedPath(format(ONE), path)
  def csv[T: TypeDescriptor](path: String*): SourceSink[T] = Sources.FixedPath(format(COMMA), path)

  // hourly
  def hourlyTsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.hourly(format(TAB), prefix, dr)

  def hourlyOsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.hourly(format(ONE), prefix, dr)

  def hourlyCsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.hourly(format(COMMA), prefix, dr)

  // daily
  def dailyTsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.daily(format(TAB), prefix, dr)

  def dailyOsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.daily(format(ONE), prefix, dr)

  def dailyCsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.daily(format(COMMA), prefix, dr)

  // daily prefix suffix
  def dailyPrefixSuffixTsv[T: TypeDescriptor](prefix: String, suffix: String)
    (implicit dr: DateRange): SourceSink[T] = Sources.dailyPrefixSuffix(format(TAB), prefix, suffix)

  def dailyPrefixSuffixOsv[T: TypeDescriptor](prefix: String, suffix: String)
    (implicit dr: DateRange): SourceSink[T] = Sources.dailyPrefixSuffix(format(ONE), prefix, suffix)

  def dailyPrefixSuffixCsv[T: TypeDescriptor](prefix: String, suffix: String)
    (implicit dr: DateRange): SourceSink[T] = Sources.dailyPrefixSuffix(format(COMMA), prefix, suffix)

  def mostRecent[T: TypeDescriptor](sep: TypedSep, path: String)
    (implicit dr: DateRange): SourceSink[T] = Sources.MostRecent(format(sep), path, dr, DateOps.UTC)

  private def format[T: TypeDescriptor](sep: TypedSep): Format[T] =
    Format.Tsv(implicitly[TypeDescriptor[T]], sep.str, strict = true, safe = true)
}

object TypedSequenceFile {
  def apply[T](path: String): SourceSink[T] = SourceSink[T](
    Source.FixedFormat(Format.Kryo[T](), Paths.Fixed(List(new Path(path)))),
    Sink.FixedFormat(Format.Kryo[T](), new Path(path), replace = true)
  )
}

/*
 * To use these, you will generally want to
 * import com.twitter.scalding.commons.source.typedtext._
 * to get the implicit TypedDescriptor.
 * Then use TypedText.lzoTzv[MyCaseClass]("path")
 */
object LzoTypedText {
  import TypedText._

  // fixed path
  def lzoTsv[T: TypeDescriptor](path: String*): SourceSink[T] = Sources.FixedPath(format(TAB), path)
  def lzoOsv[T: TypeDescriptor](path: String*): SourceSink[T] = Sources.FixedPath(format(ONE), path)
  def lzoCsv[T: TypeDescriptor](path: String*): SourceSink[T] = Sources.FixedPath(format(COMMA), path)

  // hourly
  def hourlyLzoTsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.hourly(format(TAB), prefix, dr)

  def hourlyLzoOsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.hourly(format(ONE), prefix, dr)

  def hourlyLzoCsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.hourly(format(COMMA), prefix, dr)

  // daily
  def dailyLzoTsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.daily(format(TAB), prefix, dr)

  def dailyLzoOsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.daily(format(ONE), prefix, dr)

  def dailyLzoCsv[T: TypeDescriptor](prefix: String)(implicit dr: DateRange): SourceSink[T] =
    Sources.daily(format(COMMA), prefix, dr)

  // daily prefix suffix
  def dailyPrefixSuffixLzoOsv[T: TypeDescriptor](prefix: String, suffix: String)
    (implicit dr: DateRange): SourceSink[T] = Sources.dailyPrefixSuffix(format(ONE), prefix, suffix)

  private def format[T: TypeDescriptor](sep: TypedSep): Format[T] =
    Format.Lzo(Format.Tsv(implicitly[TypeDescriptor[T]], sep.str, strict = true, safe = true))
}
