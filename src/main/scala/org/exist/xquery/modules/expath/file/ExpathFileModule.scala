/**
 * Copyright © 2015, eXist-db
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.exist.xquery.modules.expath.file

import org.exist.dom.QName
import org.exist.util.serializer.XQuerySerializer
import org.exist.xdm.Function.{Parameter, ResultType}
import org.exist.xdm.Type
import org.exist.xdm.XdmImplicits._
import org.exist.xquery.ErrorCodes.ErrorCode
import org.exist.xquery.{ 
  AbstractInternalModule,
  BasicFunction,
  Cardinality,
  FunctionDef,
  FunctionSignature,
  XPathException,
  XQueryContext }
import org.exist.xquery.util.SerializerUtils
import org.exist.xquery.value.{
  Base64BinaryValueType,
  BooleanValue,
  BinaryValue,
  BinaryValueFromInputStream,
  Item,
  IntegerValue,
  NodeValue,
  Sequence,
  StringValue,
  ValueSequence }
import org.exquery.expath.module.file.{FileModule, FileModuleError, FileModuleErrors}
import FileModule._

import java.io.{ByteArrayOutputStream => JByteArrayOutputStream, OutputStreamWriter => JOutputStreamWriter}
import java.net.{URI => JURI}
import java.util.{Date, List => JList, Map => JMap, Properties => JProperties}

import resource._
import scalaz._
import Maybe._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import scodec.bits.ByteVector
import scala.collection.mutable.ArrayBuffer


/**
 * Implementation of the EXPath File Module for eXist
 *
 * @author Adam Retter <adam.retter@googlemail.com>
 */
class ExpathFileModule(parameters: JMap[String, JList[_]]) extends AbstractInternalModule(ExpathFileModule.functions, parameters) {
  override def getNamespaceURI: String = FileModule.NAMESPACE
  override def getDescription: String = "EXPath file module"
  override def getDefaultPrefix: String = FileModule.PREFIX
  override def getReleaseVersion: String = "2.3"
}

object ExpathFileModule {
  private val pathParam = Parameter("path", Type.string, "A filesystem path")
  private val itemsParam = Parameter("items", Type.item_*, "The items to write to the file")
  private val paramsParam = Parameter("params", Type.element, "Controls the serialization")
  private val fileParam = Parameter("file", Type.string, "A file on the filesystem")
  private val valueParam = Parameter("value", Type.string, "A string value")
  private val encodingParam = Parameter("encoding", Type.string, "The character encoding to use")
  private val linesParam = Parameter("lines", Type.string_*, "A lines of text")
  private val prefixParam = Parameter("prefix", Type.string, "The prefix for the name")
  private val suffixParam = Parameter("suffix", Type.string, "The suffix for the name")
  private def dirParam(description: String = "A path to a directory on the filesystem") = Parameter("dir", Type.string, description)
  private val recursiveParam = Parameter("recursive", Type.boolean, "If the parameter $recursive is set to true(), sub-directories will be incorporated as well")
  private val offsetParam = Parameter("offset", Type.integer, "The offset in bytes to start at")
  private val lengthParam = Parameter("length", Type.integer, "The length in bytes")
  private val binaryValueParam = Parameter("value", Type.base64Binary, "A binary value")
  private val valuesParam = Parameter("value", Type.string_*, "The string values")

  private val pathResultType = ResultType(Type.string, "The full path to the resource created on the filesystem")
  private val binaryResultType = ResultType(Type.base64Binary, "The binary data")
  private val textResultType = ResultType(Type.string, "The text data")

  private def signatures(name: String, description: String, multiParameters: Seq[Seq[Parameter]], resultType: Maybe[ResultType]) : Seq[FunctionSignature] = org.exist.xdm.Function.signatures(new QName(name, FileModule.NAMESPACE, FileModule.PREFIX), description, multiParameters, resultType)
  private def functionDefs(signatures: Seq[FunctionSignature]) = signatures.map(new FunctionDef(_, classOf[ExpathFileFunctions]))

  val functions = Seq(
    functionDefs(signatures("exists", "Tests if the file or directory pointed by $path exists.", Seq(Seq(pathParam)), just(ResultType(Type.boolean, "true if the file exists.")))),
    functionDefs(signatures("is-dir", "Tests if $path points to a directory. On UNIX-based systems the root and the volume roots are considered directories.", Seq(Seq(pathParam)), just(ResultType(Type.boolean, "true if the path is a directory.")))),
    functionDefs(signatures("is-file", "Tests if $path points to a file.", Seq(Seq(pathParam)), just(ResultType(Type.boolean, "true if the path is a file.")))),
    functionDefs(signatures("last-modified", "Returns the last modification time of a file or directory.", Seq(Seq(pathParam)), just(ResultType(Type.dateTime, "The last modification time of the directory or file.")))),
    functionDefs(signatures("size", "Returns the byte size of a file, or the value 0 for directories.", Seq(Seq(fileParam)), just(ResultType(Type.integer, "The size in bytes of a file, or 0 if a directory.")))),
    functionDefs(signatures("append", "Appends a sequence of items to a file. If the file pointed by $file does not exist, a new file will be created.", Seq(Seq(fileParam, itemsParam), Seq(fileParam, itemsParam, paramsParam)), empty)),
    functionDefs(signatures("append-binary", "Appends a Base64 item as binary to a file. If the file pointed by $file does not exist, a new file will be created.", Seq(Seq(fileParam, binaryValueParam)), empty)),
    functionDefs(signatures("append-text", "Appends a string to a file. If the file pointed by $file does not exist, a new file will be created. Encoding is assumed to be UTF-8 if not specified.", Seq(Seq(fileParam, valueParam), Seq(fileParam, valueParam, encodingParam)), empty)),
    functionDefs(signatures("append-text-lines", "Appends a sequence of strings to a file, each followed by the system-dependent newline character. If the file pointed by $file does not exist, a new file will be created. Encoding is assumed to be UTF-8 if not specified", Seq(Seq(fileParam, linesParam), Seq(fileParam, linesParam, encodingParam)), empty)),
    functionDefs(signatures("copy", "Copies a file or a directory given a source and a target path/URI.", Seq(Seq(Parameter("source", Type.string, "The path to the file or directory to copy"), Parameter("target", Type.string, "The path to the target file or directory for the copy"))), empty)),
    functionDefs(signatures("create-dir", "Creates a directory, or does nothing if the directory already exists. The operation will create all non-existing parent directories.", Seq(Seq(dirParam())), empty)),
    functionDefs(signatures("create-temp-dir", "Creates a temporary directory and all non-existing parent directories.", Seq(Seq(prefixParam, suffixParam), Seq(prefixParam, suffixParam, dirParam("A directory in which to create the temporary directory"))), just(pathResultType))),
    functionDefs(signatures("create-temp-file", "Creates a temporary file and all non-existing parent directories.", Seq(Seq(prefixParam, suffixParam), Seq(prefixParam, suffixParam, dirParam("A directory in which to create the temporary file"))), just(pathResultType))),
    functionDefs(signatures("delete", "Deletes a file or a directory from the file system.", Seq(Seq(pathParam), Seq(pathParam, recursiveParam)), empty)),
    functionDefs(signatures("list", """Lists all files and directories in a given directory. The order of the items in the resulting sequence is not defined. The "." and ".." items are never returned. The returned paths are relative to the provided directory $dir.""", Seq(Seq(dirParam()), Seq(dirParam(), recursiveParam), Seq(dirParam(), recursiveParam, Parameter("pattern", Type.string, "Defines a name pattern in the glob syntax. Only the paths of the files and directories whose names are matching the pattern will be returned."))), empty)),
    functionDefs(signatures("move", "Moves a file or a directory given a source and a target path/URI.", Seq(Seq(Parameter("source", Type.string, "The path to the file or directory to move"), Parameter("target", Type.string, "The path to the target file or directory for the move"))), empty)),
    functionDefs(signatures("read-binary", "Returns the content of a file in its Base64 representation.", Seq(Seq(fileParam), Seq(fileParam, offsetParam), Seq(fileParam, offsetParam, lengthParam)), just(binaryResultType))),
    functionDefs(signatures("read-text", "Returns the content of a file in its string representation. Encoding is assumed to be UTF-8 if not specified.", Seq(Seq(fileParam), Seq(fileParam, encodingParam)), just(textResultType))),
    functionDefs(signatures("read-text-lines", "Returns the contents of a file as a sequence of strings, separated at newline boundaries. Encoding is assumed to be UTF-8 if not specified.", Seq(Seq(fileParam), Seq(fileParam, encodingParam)), just(textResultType))),
    functionDefs(signatures("write", "Writes a sequence of items to a file. If the file pointed to by $file already exists, it will be overwritten.", Seq(Seq(fileParam, itemsParam), Seq(fileParam, itemsParam, paramsParam)), empty)),
    functionDefs(signatures("write-binary", "Writes a Base64 item as binary to a file. If the file pointed to by $file already exists, it will be overwritten.", Seq(Seq(fileParam, binaryValueParam), Seq(fileParam, binaryValueParam, offsetParam)), empty)),
    functionDefs(signatures("write-text", "Writes a string to a file. If the file pointed to by $file already exists, it will be overwritten. Encoding is assumed to be UTF-8.", Seq(Seq(fileParam, valueParam), Seq(fileParam, valueParam, encodingParam)), empty)),
    functionDefs(signatures("write-text-lines", "Writes a sequence of strings to a file, each followed by the system-dependent newline character. If the file pointed to by $file already exists, it will be overwritten. Encoding is assumed to be UTF-8 if bit specified.", Seq(Seq(fileParam, valuesParam), Seq(fileParam, valuesParam, encodingParam)), empty)),
    functionDefs(signatures("name", "Returns the name of a file or directory.", Seq(Seq(pathParam)), just(ResultType(Type.string, "The name of the directory or file.")))),
    functionDefs(signatures("parent", "Transforms the given path into an absolute path, as specified by file:resolve-path, and returns the parent directory.", Seq(Seq(pathParam)), just(ResultType(Type.string_?, "The name of the parent or the empty-sequence if the parent is a filesystem root.")))),
    functionDefs(signatures("path-to-native", "Transforms a URI, an absolute path, or relative path to a canonical, system-dependent path representation. A canonical path is both absolute and unique and thus contains no redirections such as references to parent directories or symbolic links.", Seq(Seq(pathParam)), just(ResultType(Type.string, "The resulting native path.")))),
    functionDefs(signatures("path-to-uri", "Transforms a file system path into a URI with the file:// scheme. If the path is relative, it is first resolved against the current working directory.", Seq(Seq(pathParam)), just(ResultType(Type.uri, "The resulting path URI.")))),
    functionDefs(signatures("resolve-path", "Transforms a relative path into an absolute operating system path by resolving it against the current working directory. If the resulting path points to a directory, it will be suffixed with the system-specific directory separator.", Seq(Seq(pathParam)), just(ResultType(Type.string, "The absolute filesystem path.")))),
    functionDefs(signatures("dir-separator", """Returns the value of the operating system-specific directory separator, which usually is / on UNIX-based systems and \ on Windows systems.""", Seq.empty, just(ResultType(Type.string, "The directory separator")))),
    functionDefs(signatures("line-separator", "Returns the value of the operating system-specific line separator, which usually is &#10; on UNIX-based systems, &#13;&#10; on Windows systems and &#13; on Mac systems.", Seq.empty, just(ResultType(Type.string, "The line separator")))),
    functionDefs(signatures("path-separator", "Returns the value of the operating system-specific path separator, which usually is : on UNIX-based systems and ; on Windows systems.", Seq.empty, just(ResultType(Type.string, "The path separator")))),
    functionDefs(signatures("temp-dir", "Returns the path to the default temporary-file directory of an operating system.", Seq.empty, just(ResultType(Type.string, "The path of the temporary directory.")))),
    functionDefs(signatures("base-dir", "Returns the parent directory of the static base URI. If the Base URI property is undefined, the empty sequence is returned.", Seq.empty, just(ResultType(Type.string_?, "The parent directory of the static base URI.")))),
    functionDefs(signatures("current-dir", "Returns the current working directory.", Seq.empty, just(ResultType(Type.string, "The current working directory."))))
  ).reduceLeft(_ ++ _).toArray
}

/**
 * Implementation of the functions within the EXPath
 * File Module for eXist
 */
class ExpathFileFunctions(context: XQueryContext, signature: FunctionSignature) extends BasicFunction(context, signature) {

  private lazy val fm = new FileModule {}

  @throws[XPathException]
  override def eval(args: Array[Sequence], contextSequence: Sequence) : Sequence = {
    
    signature.getName.getLocalPart match {
      case "exists" =>
        fileProperty(args)(fm.exists)

      case "is-dir" =>
        fileProperty(args)(fm.isDir)

      case "is-file" =>
        fileProperty(args)(fm.isFile)

      case "last-modified" =>
        fileProperty(args)(fm.lastModified).map(LongToXdmDateTime)

      case "size" =>
        fileProperty(args)(fm.fileSize).map(LongToXdmInteger)

      case "append" =>
        appendOrWrite(args, append = true)

      case "append-binary" =>
        appendBinary(args)

      case "append-text" =>
        appendOrWriteText(args, append = true)

      case "append-text-lines" =>
        appendOrWriteTextLines(args, append = true)

      case "copy" =>
        val source = sarg(args)(0)
        val target = sarg(args)(1)

        source.zip(target) match {
          case Just((source, target)) =>
            fm.copy(source, target)
          case empty =>
            invalidArg
        }

      case "create-dir" =>
        sarg(args)(0) match {
          case Just(dir) =>
            fm.createDir(dir)
          case empty =>
            invalidArg
        }

      case "create-temp-dir" =>
        val prefix = sarg(args)(0)
        val suffix = sarg(args)(1)
        val dir = sarg(args)(2)

        prefix.zip(suffix).map {
          case (prefix, suffix) =>
            fm.createTempDir(prefix, suffix, dir).map(StringToXdmString(_))
        } | invalidArg

      case "create-temp-file" =>
        val prefix = sarg(args)(0)
        val suffix = sarg(args)(1)
        val dir = sarg(args)(2)

        prefix.zip(suffix).map {
          case (prefix, suffix) =>
            fm.createTempFile(prefix, suffix, dir).map(StringToXdmString(_))
        } | invalidArg

      case "delete" =>
        val path = sarg(args)(0)
        val recursive: Boolean = bv2b(barg(args)(1)) | false
        path match {
          case Just(path) =>
            fm.delete(path, recursive)
          case empty =>
            invalidArg
        }

      case "list" =>
        val dir = sarg(args)(0)
        val recursive = bv2b(barg(args)(1)) | false
        val pattern = sarg(args)(0)

        dir.map {
          dir =>
            val seq = new ValueSequence()
            valueOrError(fm.list(dir, recursive, pattern).map(_.to(seqSink(seq)).run.attemptRun)).map(_ => seq)
        } | invalidArg

      case "move" =>
        val source = sarg(args)(0)
        val target = sarg(args)(1)

        source.zip(target) match {
          case Just((source, target)) =>
            fm.move(source, target)
          case empty =>
            invalidArg
        }
        
      case "read-binary" =>
        val file = sarg(args)(0)
        val offset = iv2i(iarg(args)(1)) | 0
        val length = iarg(args)(2)

        file.map {
          file =>
            fm.readBinary(file, offset, length).map {
              chan =>
                val p = Process.constant(FileModule.DEFAULT_BUF_SIZE).toSource.through(chan)
                BinaryValueFromInputStream.getInstance(context, new Base64BinaryValueType(), new ProcessInputStream(p))
            }

        } | invalidArg

      case "read-text" =>
        val file = sarg(args)(0)
        val encoding = sv2s(sarg(args)(1)) | FileModule.DEFAULT_CHAR_ENCODING

        file.map {
          file =>
            val buf = new ArrayBuffer[String]
            valueOrError(fm.readText(file, encoding).map(_.to(io.fillBuffer(buf)).run.attemptRun)).map(_ => StringToXdmString(buf.reduceLeft(_ + _)))
        } | invalidArg

      case "read-text-lines" =>
        val file = sarg(args)(0)
        val encoding = sv2s(sarg(args)(1)) | FileModule.DEFAULT_CHAR_ENCODING

        file.map {
          file =>
            val buf = new ArrayBuffer[String]
            valueOrError(fm.readText(file, encoding).map(_.to(io.fillBuffer(buf)).run.attemptRun)).map(_ => new ValueSequence(buf.map(StringToXdmString(_)):_*))
        } | invalidArg

      case "write" =>
        appendOrWrite(args, append = false)

      case "write-binary" =>
        writeBinary(args)

      case "write-text" =>
        appendOrWriteText(args, append = false)

      case "write-text-lines" =>
        appendOrWriteTextLines(args, append = false)

      case "name" =>
        fileProperty(args)(fm.name)

      case "parent" =>
        fileProperty(args)(fm.parent)

      // case "children" =>
      //   val path = sarg(args)(0)

      //   path.map {
      //     path =>
      //       val seq = new ValueSequence()
      //       valueOrError(fm.children(path).map(_.to(seqSink(seq)).run.attemptRun)).map(_ => seq)
      //   } | invalidArg

      case "path-to-native" =>
        fileProperty(args)(fm.pathToNative).map(StringToXdmString(_))

      case "path-to-uri" =>
        fileProperty(args)(fm.pathToUri)

      case "resolve-path" =>
        fileProperty(args)(fm.resolvePath)

      case "dir-separator" =>
        fm.dirSeparator

      case "line-separator" =>
        fm.lineSeparator

      case "path-separator" =>
        fm.pathSeparator

      case "temp-dir" =>
        fm.tempDir

      case "base-dir" =>
        fm.parent(context.getBaseURI.toString)

      case "current-dir" =>
        fm.currentDir

      case _ =>
        throw new XPathException("Unknown function signature")
    }
  }

  /**
   * Creates a Sink which appends Strings to the provided
   * ValueSequence
   */
  private def seqSink(seq: ValueSequence) : Sink[Task, String] = {
    io.resource(Task.delay(seq))(seq => Task.delay()) {
      seq => Task.now((s: String) => Task.delay(seq.add(StringToXdmString(s))))
    }
  }

  @throws[XPathException]
  private def appendOrWrite(args: Array[Sequence], append: Boolean) : Sequence = {
    val file = sarg(args)(0)
    val items : Maybe[Sequence] = args.get(1)
    val outputProperties = new JProperties()
    arg[NodeValue](args)(2).map {
      params =>
        SerializerUtils.getSerializationOptions(this, params, outputProperties)
    }

    file.zip(items).map {
      case (file, items) =>
        val serializedStream = Process.eval(Task.delay(items)).pipe(process1.lift(serializeSequence(outputProperties)))
        appendOrWrite(fm.write(file, append), serializedStream)
    } | invalidArg
  }

  @throws[XPathException]
  private def appendBinary(args: Array[Sequence]) : Sequence = appendOrWriteBinary(args, fm.write(_, append = true))

  @throws[XPathException]
  private def writeBinary(args: Array[Sequence]) : Sequence = {
    val offset = iarg(args)(2).map(_.getInt) | 0
    appendOrWriteBinary(args, fm.writeBinary(_, offset))
  }

  @throws[XPathException]
  private def appendOrWriteBinary(args : Array[Sequence], fasf: (String) => \/[FileModuleError, AppendStreamFn[ByteVector]]) : Sequence = {
    val file = sarg(args)(0)
    val value = args.get(1).map(_.itemAt(0).asInstanceOf[BinaryValue])

    file.zip(value).map {
      case (file, value) =>
        val bin = Process.constant(DEFAULT_BUF_SIZE).toSource.through(io.chunkR(value.getInputStream))
        appendOrWrite(fasf(file), bin)
    } | invalidArg
  }

  @throws[XPathException]
  private def appendOrWriteText(args: Array[Sequence], append: Boolean) : Sequence = {
    val file = sarg(args)(0)
    val value = sarg(args)(1)
    val encoding = arg[StringValue](args)(2).map(_.getStringValue) | "UTF-8"

    file.zip(value).map {
      case (file, value) =>
        val txt = Process.eval(Task.delay[String](value))
        appendOrWrite(fm.write(file, append, encoding), txt)
    } | invalidArg
  }

  @throws[XPathException]
  private def appendOrWriteTextLines(args: Array[Sequence], append: Boolean) : Sequence = {
    val file = sarg(args)(0)
    val lines: Maybe[Sequence] = args.get(1)
    val encoding = arg[StringValue](args)(2).map(_.getStringValue) | "UTF-8"

    file.zip(lines).map {
      case (file, lines) if (!lines.isEmpty) =>
        val lineStream = io.resource(Task.delay(lines))(lines => Task.delay()) {
          lines =>
            lazy val it = lines.iterate // A stateful iterator
            Task.delay {
              if (it.hasNext)
                it.nextItem.asInstanceOf[StringValue].getStringValue
              else
                throw Cause.Terminated(Cause.End)
            }
        }
        appendOrWrite(fm.write(file.getStringValue, append, encoding), lineStream)
    } | invalidArg
  }

  /**
   * Helper function for FileModule functions that operate on a single
   * string parameter... typically a path.
   */
  @throws[XPathException]
  private def fileProperty[T](args: Array[Sequence])(fn: (String) => T) = sarg(args)(0).map(fn(_)) | invalidArg

  /**
   * Helper function for appending or writing to a process
   */
  private def appendOrWrite[T](asf: => \/[FileModuleError, AppendStreamFn[T]], source: Process[Task, T]) =
    asf.map(_(source))
      .flatMap(_.map(_ => Sequence.EMPTY_SEQUENCE))
  
  /**
   * Serializes a sequence
   *
   * @param outputProperties Any output properties to set on the serializer
   * @param seq The sequence of items to serialize
   * 
   * @return The serialized items
   */
  @throws[RuntimeException]
  private def serializeSequence(outputProperties: JProperties)(seq: Sequence) : ByteVector = {
    managed(new JByteArrayOutputStream()).map {
      baos =>
        managed(new JOutputStreamWriter(baos)).map {
          writer =>
            val xqSerializer = new XQuerySerializer(context.getBroker(), outputProperties, writer)
            xqSerializer.serialize(seq)
        }.either.rightMap(_ => baos.toByteArray)
    }.either.joinRight match {
      case Left(ex) =>
        throw new RuntimeException(ex(0))
      case Right(data) =>
        ByteVector.view(data)
    }
  }

  //TODO when serialization becomes stream of item based as opposed to sequence based
//  def toStream(seq: => Sequence) : Process[Task, Item] =
//    io.resource(Task.delay(seq))(seq => Task.delay()) {
//      seq =>
//        lazy val seqIt = seq.iterate() // A stateful iterator
//        Task.delay {
//          if (seqIt.hasNext)
//            seqIt.nextItem
//          else
//            throw Cause.Terminated(Cause.End)
//        }
//    }
  
  //TODO when serialization becomes stream of item based as opposed to sequence based
//  def serializeItem(outputProperties: JProperties)(item: Item) : ByteVector = {
//    managed(new JByteArrayOutputStream()).map {
//      baos =>
//        managed(new JOutputStreamWriter(baos)).map {
//          writer =>
//            val xqSerializer = new XQuerySerializer(context.getBroker(), outputProperties, writer)
//            xqSerializer.serialize(item)
//        }.either.rightMap(_ => baos.toByteArray)
//    }.either.joinRight match {
//      case Left(ex) =>
//        throw new RuntimeException(ex(0))
//      case Right(data) =>
//        ByteVector.view(data)
//    }
//  }

  /**
   * Throw a standard invalid argument XPathException
   */
  @throws[XPathException]
  private def invalidArg = throw new XPathException(getLine(), getColumn(), "Missing function argument")

  /**
   * Adds a safe get(Int) : Maybe[T] method to any Array[T] object
   */
  private implicit class MyArray[T](val array: Array[T]) {
    def get(idx: Int): Maybe[T] = fromOption(scala.util.Try(array(idx)).toOption)
  }

  /**
   * Extract a single argument
   *
   * @param idx The index of the argument
   * @param iidx If the argument is a sequence, then the item index within the sequence.
   *       Defaults to 0 which is suitable for an atomic item
   */
  private def arg[T <: Item](args: Array[Sequence])(idx: Int, iidx: Int = 0) : Maybe[T] = args.get(idx).map(_.itemAt(iidx).asInstanceOf[T])

  /**
   * Extract a single string value argument
   *
   * @param idx The index of the argument
   * @param iidx If the argument is a sequence, then the item index within the sequence.
   *       Defaults to 0 which is suitable for an atomic item
   */
  private def sarg(args: Array[Sequence])(idx: Int, iidx: Int = 0) : Maybe[StringValue] = arg[StringValue](args)(idx, iidx)

  /**
   * Extract a single boolean value argument
   *
   * @param idx The index of the argument
   * @param iidx If the argument is a sequence, then the item index within the sequence.
   *       Defaults to 0 which is suitable for an atomic item
   */
  private def barg(args: Array[Sequence])(idx: Int, iidx: Int = 0) : Maybe[BooleanValue] = arg[BooleanValue](args)(idx, iidx)

  /**
   * Extract a single integer value argument
   *
   * @param idx The index of the argument
   * @param iidx If the argument is a sequence, then the item index within the sequence.
   *       Defaults to 0 which is suitable for an atomic item
   */
  private def iarg(args: Array[Sequence])(idx: Int, iidx: Int = 0) : Maybe[IntegerValue] = arg[IntegerValue](args)(idx, iidx)

  /**
   * Extracts a value from the right or throws
   * an XPathException for the error on the left
   */
  @throws[XPathException]
  private implicit def getOrThrow[T](value: \/[FileModuleError, T]): T = value.valueOr(throwIt(_))

  /**
   * Throws an XPathException from a FileModuleError
   */
  @throws[XPathException]
  private def throwIt(fme: FileModuleError) = {
    val errorCode = new ErrorCode(new QName(fme.name, FileModule.NAMESPACE), fme.description)    
    throw fme.exception.map(e => new XPathException(this, errorCode, fme.description, null, e)) | new XPathException(errorCode, getLine, getColumn)
  }

  /**
   * If there is an error value then
   * throw an appropriate XPathException
   * otherwise return an empty Sequence
   *
   * @return An empty sequence
   */
  @throws[XPathException]("If there is an error value")
  private implicit def emptyOrThrow(value: Maybe[FileModuleError]) : Sequence = {
    value match {
      case Just(fme) =>
        throwIt(fme)
      case empty =>
        Sequence.EMPTY_SEQUENCE
    }
  }

  /**
   * Flattens out a result to extract an
   * either error or value. Exceptions are
   * converted to FileModuleErrors.IoError
   */
  private def valueOrError[T](v: \/[FileModuleError, \/[Throwable, T]]) : \/[FileModuleError, T] = v.map(_.leftMap(FileModuleErrors.IoError))

  private implicit def sv2s(value: Maybe[StringValue]) : Maybe[String] = value.map(_.getStringValue)
  private implicit def bv2b(value: Maybe[BooleanValue]) : Maybe[Boolean] = value.map(_.getValue)
  private implicit def iv2i(value: Maybe[IntegerValue]) : Maybe[Int] = value.map(_.getInt)
}
