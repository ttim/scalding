package com.twitter.scalding.source2

import com.twitter.bijection.Injection
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSource
import java.util.TimeZone
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.{Failure, Success, Try}

trait Paths

trait FsPaths extends Paths {
  def get(fsResolver: Path => FileSystem, config: Config): Try[Seq[Path]]
}

object FsPaths {
  case class Fixed(paths: Seq[Path]) extends FsPaths {
    override def get(fsResolver: Path => FileSystem, config: Config): Try[Seq[Path]] =
      Success(paths)
  }

  case class Time(patterns: Seq[String], dateRange: DateRange, tz: TimeZone) extends Fixed(
    patterns
      .flatMap { pattern: String =>
        Globifier(pattern)(tz).globify(dateRange)
      }
      .map(new Path(_))
  )

  case class Version(path: Path, optVersion: Option[Long]) extends FsPaths {
    override def get(fsResolver: Path => FileSystem, config: Config): Try[Seq[Path]] = Try {
      val versionToRead = optVersion match {
        case Some(version) => version
        case None => ??? //getAllVersions(path, skipVersionSuffix = false).max
      }
      List(new Path(path, versionToRead.toString))
    }
  }

  case class Last(paths: FsPaths) extends FsPaths {
    override def get(fsResolver: Path => FileSystem, config: Config): Try[Seq[Path]] =
      paths.get(fsResolver, config).map(resolvedPaths => List(resolvedPaths.last))
  }

  abstract class PathsCheck(paths: FsPaths) extends FsPaths {
    override def get(fsResolver: Path => FileSystem, config: Config): Try[Seq[Path]] =
      paths.get(fsResolver, config).flatMap { resolved =>
        val badPaths = resolved.filter(!pathIsGood(_, fsResolver))
        if (badPaths.isEmpty)
          Success(resolved)
        else
          Failure(new Exception(s"Broken check ${ this.getClass } for $badPaths"))
      }

    abstract def pathIsGood(path: Path, fsResolver: Path => FileSystem): Boolean
  }

  // Check every path has non hidden paths inside
  case class HasNonHiddenPaths(paths: FsPaths, verbose: Boolean) extends PathsCheck(paths) {
    override def pathIsGood(path: Path, fsResolver: Path => FileSystem): Boolean =
      FileUtils.globHasNonHiddenPaths(path, fsResolver, verbose)
  }

  // Check every path has success file inside
  case class HasSuccessFiles(paths: FsPaths) extends PathsCheck(paths) {
    override def pathIsGood(path: Path, fsResolver: Path => FileSystem): Boolean =
      FileUtils.allGlobFilesWithSuccess(path, fsResolver, hiddenFilter = true)
  }

  /**
   * Determines if a path is 'valid' for this source. In strict mode all paths must be valid.
   * In non-strict mode, all invalid paths will be filtered out.
   *
   * Subclasses can override this to validate paths.
   *
   * The default implementation is a quick sanity check to look for missing or empty directories.
   * It is necessary but not sufficient -- there are cases where this will return true but there is
   * in fact missing data.
   *
   * TODO: consider writing a more in-depth version of this method in [[TimePathedSource]] that looks for
   * TODO: missing days / hours etc.
   */
  case class FileSourceCompatibleCheck(paths: FsPaths) extends FsPaths {
    override def get(fsResolver: Path => FileSystem, config: Config): Try[Seq[Path]] = {
      val check = if (config.getBoolean("scalding.require_success_file", orElse = false)) {
        HasSuccessFiles(paths)
      } else {
        val verbose = config.getBoolean(Config.VerboseFileSourceLoggingKey, orElse = false)
        HasNonHiddenPaths(paths, verbose)
      }
      check.get(fsResolver, config)
    }
  }
}

trait Source[T]

object Source {
  case class FixedFormat[T](format: Format[T], paths: Paths) extends Source[T]
  // For compatibility, todo: add implicit conversion
  case class Typed[T](source: TypedSource[T]) extends Source[T]

  // case class MultiFormat[T](formatDetector: FormatDetector, clazz: Class[T], paths: Paths) extends Source[T]

  // add condition on P being string or list of strings
  case class Partitioned[P, T](
    format: Format[(P, T)],
    path: Path,
    template: String
  ) extends Source[(P, T)] with Sink[(P, T)]
}

trait Sink[T]

object Sink {
  case class FixedFormat[T](format: Format[T], path: Path, replace: Boolean) extends Sink[T]
  // For compatibility, todo: add implicit conversion
  case class Typed[T](sink: TypedSink[T]) extends Sink[T]

  // todo: add boolean flag to force materialization?
  case class Null[T]() extends Sink[T]

  case class Versioned[T](
    format: Format[T],
    path: Path,
    version: Option[Long],
    versionsToKeep: Int
  ) extends Sink[T]
}

case class SourceSink[T](source: Source[T], sink: Sink[T]) extends Source[T] with Sink[T]

// Sources which correspond to old sources
object Sources {
  case class FixedPath[T](format: Format[T], paths: Seq[String]) extends SourceSink[T](
    Source.FixedFormat(format, FsPaths.FileSourceCompatibleCheck(FsPaths.Fixed(paths.map(new Path(_))))),
    Sink.FixedFormat(format, new Path(paths.last), replace = false)
  )

  case class TimePathed[T](
    format: Format[T],
    pattern: String,
    dateRange: DateRange,
    tz: TimeZone
  ) extends SourceSink[T](
    Source.FixedFormat(format, FsPaths.FileSourceCompatibleCheck(FsPaths.Time(Seq(pattern), dateRange, tz))),
    Sink.FixedFormat(
      format,
      new Path(TimePathedSource.writePathFor(pattern, dateRange, tz)),
      replace = false
    )
  )

  case class MostRecent[T](
    format: Format[T],
    pattern: String,
    dateRange: DateRange,
    tz: TimeZone
  ) extends SourceSink[T](
    Source.FixedFormat(
      format,
      FsPaths.Last(FsPaths.FileSourceCompatibleCheck(FsPaths.Time(Seq(pattern), dateRange, tz)))
    ),
    Sink.FixedFormat(
      format,
      new Path(TimePathedSource.writePathFor(pattern, dateRange, tz)),
      replace = false
    )
  )

  case class VersionedKeyVal[K, V](
    path: String,
    sourceVersion: Option[Long] = None,
    sinkVersion: Option[Long] = None,
    maxFailures: Int,
    versionsToKeep: Int,
    codec: Injection[(K, V), (Array[Byte], Array[Byte])]
  ) extends SourceSink[(K, V)](
    Source.FixedFormat(
      Format.KeyValue(codec, maxFailures),
      FsPaths.Version(new Path(path), sourceVersion)
    ),
    Sink.Versioned(
      Format.KeyValue(codec, maxFailures),
      new Path(path),
      sinkVersion,
      versionsToKeep
    )
  )

  /**
   * Prefix might be "/logs/awesome"
   */
  def hourly[T](format: Format[T], prefix: String, dr: DateRange): SourceSink[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    TimePathedSource(format, prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", dr,
      DateOps.UTC)
  }

  def daily[T](format: Format[T], prefix: String, dr: DateRange): SourceSink[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    TimePathedSource(format, prefix + TimePathedSource.YEAR_MONTH_DAY + "/*", dr,
      DateOps.UTC)
  }

  def dailyPrefixSuffix[T](format: Format[T], prefix: String, suffix: String)
    (implicit dr: DateRange): SourceSink[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    require(suffix.head == '/', "suffix should include a preceding /")
    TimePathedSource(format, prefix + TimePathedSource.YEAR_MONTH_DAY + suffix + "/*", dr,
      DateOps.UTC)
  }
}
