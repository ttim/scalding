package com.twitter.scalding.source2

import cascading.scheme.Scheme
import com.twitter.scalding.source2.Format.{Lzo, Tsv}
import com.twitter.scalding._
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.Try
import cascading.scheme.hadoop.{TextDelimited => CHTextDelimited}
import cascading.scheme.local.{TextDelimited => CLTextDelimited}
import cascading.tuple.Fields

trait FormatInterpreter {
  def interpretHdfs(format: Format[_], hdfsMode: Hdfs): Option[Scheme[_, _, _, _, _]]

  def interpretLocal(format: Format[_], localMode: Local): Option[Scheme[_, _, _, _, _]]
}

object FormatInterpreter {
  case object Tsv extends FormatInterpreter {
    override def interpretHdfs(format: Format[_], hdfs: Hdfs): Option[Scheme[_, _, _, _, _]] =
      format match {
        case Tsv(typeDescriptor, sep, strict, safe) =>
          val fields = tsvFields(typeDescriptor)
          Some(HadoopSchemeInstance(new CHTextDelimited(fields, null /* compression */ , false, false,
            sep, strict, null /* quote */ ,
            fields.getTypesClasses, safe).asInstanceOf[Scheme[_, _, _, _, _]])
          )
        case _ =>
          None
      }

    override def interpretLocal(format: Format[_], local: Local): Option[Scheme[_, _, _, _, _]] =
      format match {
        case Tsv(typeDescriptor, sep, strict, safe) =>
          val fields = tsvFields(typeDescriptor)
          Some(new CLTextDelimited(fields, false, false, sep, strict, null /* quote */ ,
            fields.getTypesClasses, safe))
        case _ =>
          None
      }
  }

  case object LzoTsv extends FormatInterpreter {
    override def interpretHdfs(format: Format[_], hdfs: Hdfs): Option[Scheme[_, _, _, _, _]] =
      format match {
        case Lzo(Tsv(typeDescriptor, sep, strict, safe)) =>
//      HadoopSchemeInstance(new LzoTextDelimited(typeDescriptor.fields, false, false,
//      separator.str, strict, null /* quote */ ,
//      typeDescriptor.fields.getTypesClasses, safe).asInstanceOf[Scheme[_, _, _, _, _]])
          ???
        case _ =>
          None
      }

    override def interpretLocal(format: Format[_], local: Local): Option[Scheme[_, _, _, _, _]] =
      format match {
        case Lzo(tsv@Tsv(_, _, _, _)) =>
          // todo: ?
          FormatInterpreter.Tsv.interpretLocal(tsv, local)
        case _ =>
          None
      }
  }

  case object KeyVal extends FormatInterpreter {
    override def interpretHdfs(format: Format[_], hdfsMode: Hdfs): Option[Scheme[_, _, _, _, _]] =
      interpret(format)

    override def interpretLocal(format: Format[_], localMode: Local): Option[Scheme[_, _, _, _, _]] =
      interpret(format)

    private def interpret(format: Format[_]): Option[Scheme[_, _, _, _, _]] = format match {
      case Format.KeyValue(injection, maxFailures) =>
        //  new MaxFailuresCheck(maxFailures)(codecBox.get)
//        val keyField = "key"
//        val valField = "value"
//        val fields = new Fields(keyField, valField)
//        HadoopSchemeInstance(new KeyValueByteScheme(fields).asInstanceOf[Scheme[_, _, _, _, _]])
        Some(???)
      case _ =>
        None
    }
  }

  private[this] def tsvFields[T](typeDescriptor: TypeDescriptor[T]): Fields = {
    ???
  }
}

abstract class CascadingInterpreter(formatInterpreters: List[FormatInterpreter])(config: Config, mode: Mode) {
  assert(mode.isInstanceOf[CascadingMode])

  def source[T](source: Source[T]): Mappable[T] = ???
  def sink[T](sink: Sink[T]): TypedSink[T] = ???

  private[this] def interpretPaths(paths: Paths): Try[Seq[Path]] = paths match {
    case fsPaths: FsPaths => fsPaths.get(fsResolver, config)
    case _ => ???
  }

  private[this] def interpretFormat[T](format: Format[T]): Scheme[_, _, _, _, _] =
    mode match {
      // TODO support strict in Local
      case localMode@Local(_) =>
        val schemes = formatInterpreters.flatMap(interpreter => interpreter.interpretLocal(format, localMode))
        assert(schemes.size == 1)
        schemes.head
      case hdfsMode@Hdfs(_, _) =>
        val schemes = formatInterpreters.flatMap(interpreter => interpreter.interpretHdfs(format, hdfsMode))
        assert(schemes.size == 1)
        schemes.head
      case _ =>
        ???
    }

  private[this] def fsResolver: (Path => FileSystem) = mode match {
    case hdfsMode@Hdfs(_, _) =>
      path => path.getFileSystem(hdfsMode.jobConf)
    case localMode@Local(_) =>
      ???
  }
}
