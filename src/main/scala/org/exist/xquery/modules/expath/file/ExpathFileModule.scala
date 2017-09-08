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
import org.exist.xquery.{AbstractInternalModule, BasicFunction, FunctionDef, FunctionSignature, XPathException, XQueryContext}
import org.exist.xquery.util.SerializerUtils
import org.exist.xquery.value.{Base64BinaryValueType, BinaryValue, BinaryValueFromInputStream, BooleanValue, IntegerValue, Item, NodeValue, Sequence, StringValue, ValueSequence}
import org.exquery.expath.module.file.{FileModule, FileModuleErrors, FileModuleException}
import FileModule._
import java.io.{ByteArrayInputStream, InputStream, ByteArrayOutputStream => JByteArrayOutputStream, OutputStreamWriter => JOutputStreamWriter}
import java.net.URI
import java.util.{List => JList, Map => JMap, Properties => JProperties}

import fs2.{Stream, io}
import cats.effect.{IO, Sync}


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

  private def signatures(name: String, description: String, multiParameters: Seq[Seq[Parameter]], resultType: Option[ResultType]) : Seq[FunctionSignature] = org.exist.xdm.Function.signatures(new QName(name, FileModule.NAMESPACE, FileModule.PREFIX), description, multiParameters, resultType)
  private def functionDefs(signatures: Seq[FunctionSignature]) = signatures.map(new FunctionDef(_, classOf[ExpathFileFunctions]))

  val functions = Seq(
    functionDefs(signatures("exists", "Tests if the file or directory pointed by $path exists.", Seq(Seq(pathParam)), Some(ResultType(Type.boolean, "true if the file exists.")))),
    functionDefs(signatures("is-dir", "Tests if $path points to a directory. On UNIX-based systems the root and the volume roots are considered directories.", Seq(Seq(pathParam)), Some(ResultType(Type.boolean, "true if the path is a directory.")))),
    functionDefs(signatures("is-file", "Tests if $path points to a file.", Seq(Seq(pathParam)), Some(ResultType(Type.boolean, "true if the path is a file.")))),
    functionDefs(signatures("last-modified", "Returns the last modification time of a file or directory.", Seq(Seq(pathParam)), Some(ResultType(Type.dateTime, "The last modification time of the directory or file.")))),
    functionDefs(signatures("size", "Returns the byte size of a file, or the value 0 for directories.", Seq(Seq(fileParam)), Some(ResultType(Type.integer, "The size in bytes of a file, or 0 if a directory.")))),
    functionDefs(signatures("append", "Appends a sequence of items to a file. If the file pointed by $file does not exist, a new file will be created.", Seq(Seq(fileParam, itemsParam), Seq(fileParam, itemsParam, paramsParam)), None)),
    functionDefs(signatures("append-binary", "Appends a Base64 item as binary to a file. If the file pointed by $file does not exist, a new file will be created.", Seq(Seq(fileParam, binaryValueParam)), None)),
    functionDefs(signatures("append-text", "Appends a string to a file. If the file pointed by $file does not exist, a new file will be created. Encoding is assumed to be UTF-8 if not specified.", Seq(Seq(fileParam, valueParam), Seq(fileParam, valueParam, encodingParam)), None)),
    functionDefs(signatures("append-text-lines", "Appends a sequence of strings to a file, each followed by the system-dependent newline character. If the file pointed by $file does not exist, a new file will be created. Encoding is assumed to be UTF-8 if not specified", Seq(Seq(fileParam, linesParam), Seq(fileParam, linesParam, encodingParam)), None)),
    functionDefs(signatures("copy", "Copies a file or a directory given a source and a target path/URI.", Seq(Seq(Parameter("source", Type.string, "The path to the file or directory to copy"), Parameter("target", Type.string, "The path to the target file or directory for the copy"))), None)),
    functionDefs(signatures("create-dir", "Creates a directory, or does nothing if the directory already exists. The operation will create all non-existing parent directories.", Seq(Seq(dirParam())), None)),
    functionDefs(signatures("create-temp-dir", "Creates a temporary directory and all non-existing parent directories.", Seq(Seq(prefixParam, suffixParam), Seq(prefixParam, suffixParam, dirParam("A directory in which to create the temporary directory"))), Some(pathResultType))),
    functionDefs(signatures("create-temp-file", "Creates a temporary file and all non-existing parent directories.", Seq(Seq(prefixParam, suffixParam), Seq(prefixParam, suffixParam, dirParam("A directory in which to create the temporary file"))), Some(pathResultType))),
    functionDefs(signatures("delete", "Deletes a file or a directory from the file system.", Seq(Seq(pathParam), Seq(pathParam, recursiveParam)), None)),
    functionDefs(signatures("list", """Lists all files and directories in a given directory. The order of the items in the resulting sequence is not defined. The "." and ".." items are never returned. The returned paths are relative to the provided directory $dir.""", Seq(Seq(dirParam()), Seq(dirParam(), recursiveParam), Seq(dirParam(), recursiveParam, Parameter("pattern", Type.string, "Defines a name pattern in the glob syntax. Only the paths of the files and directories whose names are matching the pattern will be returned."))), None)),
    functionDefs(signatures("move", "Moves a file or a directory given a source and a target path/URI.", Seq(Seq(Parameter("source", Type.string, "The path to the file or directory to move"), Parameter("target", Type.string, "The path to the target file or directory for the move"))), None)),
    functionDefs(signatures("read-binary", "Returns the content of a file in its Base64 representation.", Seq(Seq(fileParam), Seq(fileParam, offsetParam), Seq(fileParam, offsetParam, lengthParam)), Some(binaryResultType))),
    functionDefs(signatures("read-text", "Returns the content of a file in its string representation. Encoding is assumed to be UTF-8 if not specified.", Seq(Seq(fileParam), Seq(fileParam, encodingParam)), Some(textResultType))),
    functionDefs(signatures("read-text-lines", "Returns the contents of a file as a sequence of strings, separated at newline boundaries. Encoding is assumed to be UTF-8 if not specified.", Seq(Seq(fileParam), Seq(fileParam, encodingParam)), Some(textResultType))),
    functionDefs(signatures("write", "Writes a sequence of items to a file. If the file pointed to by $file already exists, it will be overwritten.", Seq(Seq(fileParam, itemsParam), Seq(fileParam, itemsParam, paramsParam)), None)),
    functionDefs(signatures("write-binary", "Writes a Base64 item as binary to a file. If the file pointed to by $file already exists, it will be overwritten.", Seq(Seq(fileParam, binaryValueParam), Seq(fileParam, binaryValueParam, offsetParam)), None)),
    functionDefs(signatures("write-text", "Writes a string to a file. If the file pointed to by $file already exists, it will be overwritten. Encoding is assumed to be UTF-8.", Seq(Seq(fileParam, valueParam), Seq(fileParam, valueParam, encodingParam)), None)),
    functionDefs(signatures("write-text-lines", "Writes a sequence of strings to a file, each followed by the system-dependent newline character. If the file pointed to by $file already exists, it will be overwritten. Encoding is assumed to be UTF-8 if bit specified.", Seq(Seq(fileParam, valuesParam), Seq(fileParam, valuesParam, encodingParam)), None)),
    functionDefs(signatures("name", "Returns the name of a file or directory.", Seq(Seq(pathParam)), Some(ResultType(Type.string, "The name of the directory or file.")))),
    functionDefs(signatures("parent", "Transforms the given path into an absolute path, as specified by file:resolve-path, and returns the parent directory.", Seq(Seq(pathParam)), Some(ResultType(Type.string_?, "The name of the parent or the empty-sequence if the parent is a filesystem root.")))),
    functionDefs(signatures("path-to-native", "Transforms a URI, an absolute path, or relative path to a canonical, system-dependent path representation. A canonical path is both absolute and unique and thus contains no redirections such as references to parent directories or symbolic links.", Seq(Seq(pathParam)), Some(ResultType(Type.string, "The resulting native path.")))),
    functionDefs(signatures("path-to-uri", "Transforms a file system path into a URI with the file:// scheme. If the path is relative, it is first resolved against the current working directory.", Seq(Seq(pathParam)), Some(ResultType(Type.uri, "The resulting path URI.")))),
    functionDefs(signatures("resolve-path", "Transforms a relative path into an absolute operating system path by resolving it against the current working directory. If the resulting path points to a directory, it will be suffixed with the system-specific directory separator.", Seq(Seq(pathParam)), Some(ResultType(Type.string, "The absolute filesystem path.")))),
    functionDefs(signatures("dir-separator", """Returns the value of the operating system-specific directory separator, which usually is / on UNIX-based systems and \ on Windows systems.""", Seq.empty, Some(ResultType(Type.string, "The directory separator")))),
    functionDefs(signatures("line-separator", "Returns the value of the operating system-specific line separator, which usually is &#10; on UNIX-based systems, &#13;&#10; on Windows systems and &#13; on Mac systems.", Seq.empty, Some(ResultType(Type.string, "The line separator")))),
    functionDefs(signatures("path-separator", "Returns the value of the operating system-specific path separator, which usually is : on UNIX-based systems and ; on Windows systems.", Seq.empty, Some(ResultType(Type.string, "The path separator")))),
    functionDefs(signatures("temp-dir", "Returns the path to the default temporary-file directory of an operating system.", Seq.empty, Some(ResultType(Type.string, "The path of the temporary directory.")))),
    functionDefs(signatures("base-dir", "Returns the parent directory of the static base URI. If the Base URI property is undefined, the empty sequence is returned.", Seq.empty, Some(ResultType(Type.string_?, "The parent directory of the static base URI.")))),
    functionDefs(signatures("current-dir", "Returns the current working directory.", Seq.empty, Some(ResultType(Type.string, "The current working directory."))))
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
        getOrThrow[Boolean](valueOrError(
          fileProperty(args)(fm.exists[IO]).attempt.unsafeRunSync()
        ))

      case "is-dir" =>
        getOrThrow[Boolean](valueOrError(
          fileProperty(args)(fm.isDir[IO]).attempt.unsafeRunSync()
        ))

      case "is-file" =>
        getOrThrow[Boolean](valueOrError(
          fileProperty(args)(fm.isFile[IO]).attempt.unsafeRunSync()
        ))

      case "last-modified" =>
        getOrThrow(valueOrError(
          fileProperty(args)(fm.lastModified[IO]).map(LongToXdmDateTime).attempt.unsafeRunSync()
        ))

      case "size" =>
        getOrThrow(valueOrError(
          fileProperty(args)(fm.fileSize[IO]).map(LongToXdmInteger).attempt.unsafeRunSync()
        ))

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

        zip(source, target) match {
          case Some((source, target)) =>
            getOrThrow(valueOrError(
              fm.copy[IO](source, target)
                .map(_ => Sequence.EMPTY_SEQUENCE)
                .attempt.unsafeRunSync()
            ))
          case None =>
            invalidArg
        }

      case "create-dir" =>
        sarg(args)(0) match {
          case Some(dir) =>
            getOrThrow(valueOrError(
              fm.createDir[IO](dir)
                .map(_ => Sequence.EMPTY_SEQUENCE)
                .attempt.unsafeRunSync()
            ))
          case None =>
            invalidArg
        }

      case "create-temp-dir" =>
        val prefix = sarg(args)(0)
        val suffix = sarg(args)(1)
        val dir = sarg(args)(2)

        zip(prefix, suffix).map {
          case (prefix, suffix) =>
            getOrThrow(valueOrError(
              fm.createTempDir[IO](prefix, suffix, dir).map(StringToXdmString(_)).attempt.unsafeRunSync()
            ))
        }.getOrElse(invalidArg)

      case "create-temp-file" =>
        val prefix = sarg(args)(0)
        val suffix = sarg(args)(1)
        val dir = sarg(args)(2)

        zip(prefix, suffix).map {
          case (prefix, suffix) =>
            getOrThrow(valueOrError(
              fm.createTempFile[IO](prefix, suffix, dir).map(StringToXdmString(_)).attempt.unsafeRunSync()
            ))
        }.getOrElse(invalidArg)

      case "delete" =>
        val path = sarg(args)(0)
        val recursive: Boolean = bv2b(barg(args)(1)).getOrElse(false)
        path match {
          case Some(path) =>
            getOrThrow(valueOrError(
              fm.delete[IO](path, recursive)
                .map(_ => Sequence.EMPTY_SEQUENCE)
                .attempt.unsafeRunSync()
            ))
          case None =>
            invalidArg
        }

      case "list" =>
        val dir = sarg(args)(0)
        val recursive = bv2b(barg(args)(1)).getOrElse(false)
        val pattern = sarg(args)(0)

        dir.map {
          dir =>
            getOrThrow(valueOrError(
              fm.list[IO](dir, recursive, pattern)
                .map { paths =>
                  val seq = new ValueSequence()
                  paths.map(path => seq.add(path))
                  seq
                }.attempt.unsafeRunSync()
            ))
        }.getOrElse(invalidArg)

      case "move" =>
        val source = sarg(args)(0)
        val target = sarg(args)(1)

        zip(source, target) match {
          case Some((source, target)) =>
            getOrThrow(valueOrError(
              fm.move[IO](source, target)
                .map(_ => Sequence.EMPTY_SEQUENCE)
                .attempt.unsafeRunSync()
            ))
          case None =>
            invalidArg
        }

      case "read-binary" =>
        val file = sarg(args)(0)
        val offset = iv2i(iarg(args)(1)).getOrElse(0)
        val length = iarg(args)(2)

        file.map {
          file =>
            // TODO must be a better way to go from fm.readBinary to BinaryValueFromInputStream -- without buffering in memory?
            // what about --> stream.through(io.toInputStream[IO])

            getOrThrow(valueOrError(
              fm.readBinary[IO](file, offset, length).flatMap { stream =>
                stream
                .runFold(new JByteArrayOutputStream()){ case (buf, b) => buf.write(b); buf }
                .map(_.toByteArray)
                .map(inBuf => BinaryValueFromInputStream.getInstance(context, new Base64BinaryValueType(), new ByteArrayInputStream(inBuf)))
              }.attempt.unsafeRunSync()
          ))
        }.getOrElse(invalidArg)

      case "read-text" =>
        val file = sarg(args)(0)
        val encoding = sv2s(sarg(args)(1)).getOrElse(FileModule.DEFAULT_CHAR_ENCODING)

        file.map {
          file =>

            getOrThrow(valueOrError(
              fm.readText[IO](file, encoding).flatMap { stream =>
                stream
                  .runFold(new StringBuilder()){ case (buf, str) => buf.append(str)}
                  .map(_.toString())
              }.attempt.unsafeRunSync()
            ))
        }.getOrElse(invalidArg)

      case "read-text-lines" =>
        val file = sarg(args)(0)
        val encoding = sv2s(sarg(args)(1)).getOrElse(FileModule.DEFAULT_CHAR_ENCODING)

        file.map {
          file =>
            getOrThrow(valueOrError(
              fm.readText[IO](file, encoding).flatMap { stream =>
                stream
                  .runFold(new ValueSequence()){ case (buf, str) => buf.add(StringToXdmString(str)); buf}
              }.attempt.unsafeRunSync()
            ))
        }.getOrElse(invalidArg)

      case "write" =>
        appendOrWrite(args, append = false)

      case "write-binary" =>
        writeBinary(args)

      case "write-text" =>
        appendOrWriteText(args, append = false)

      case "write-text-lines" =>
        appendOrWriteTextLines(args, append = false)

      case "name" =>
        getOrThrow[String](valueOrError(
          fileProperty(args)(fm.name[IO]).attempt.unsafeRunSync()
        ))

      case "parent" =>
        getOrThrow[Option[String]](valueOrError(
          fileProperty(args)(fm.parent[IO]).attempt.unsafeRunSync()
        ))

      // case "children" =>
      //   val path = sarg(args)(0)

      //   path.map {
      //     path =>
      //       val seq = new ValueSequence()
      //       valueOrError(fm.children(path).map(_.to(seqSink(seq)).run.attemptRun)).map(_ => seq)
      //   } | invalidArg

      case "path-to-native" =>
        getOrThrow[String](valueOrError(
          fileProperty(args)(fm.pathToNative[IO]).attempt.unsafeRunSync()
        ))

      case "path-to-uri" =>
        getOrThrow[URI](valueOrError(
          fileProperty(args)(fm.pathToUri[IO]).attempt.unsafeRunSync()
        ))

      case "resolve-path" =>
        getOrThrow[String](valueOrError(
          fileProperty(args)(fm.resolvePath[IO]).attempt.unsafeRunSync()
        ))

      case "dir-separator" =>
        fm.dirSeparator

      case "line-separator" =>
        fm.lineSeparator

      case "path-separator" =>
        fm.pathSeparator

      case "temp-dir" =>
        fm.tempDir

      case "base-dir" =>
        getOrThrow[Option[String]](valueOrError(
          fm.parent[IO](context.getBaseURI.toString).attempt.unsafeRunSync()
        ))

      case "current-dir" =>
        fm.currentDir

      case _ =>
        throw new XPathException(this, "Unknown function signature")
    }
  }

  /**
   * Zips two Options together if both are Some
   */
  private def zip[A, B](oa: Option[A], ob: Option[B]) : Option[(A, B)] = oa.flatMap(a => ob.map(b => (a, b)))

   /**
    * Zips an Option and a value together if the first is Some
    */
  private def zip[A, B](oa: Option[A], b: B) : Option[(A, B)] = oa.map(a => (a, b))

  @throws[XPathException]
  private def appendOrWrite(args: Array[Sequence], append: Boolean) : Sequence = {
    val file = sarg(args)(0)
    val items : Option[Sequence] = args.get(1)
    val outputProperties = new JProperties()
    arg[NodeValue](args)(2).map {
      params =>
        SerializerUtils.getSerializationOptions(this, params, outputProperties)
    }

    zip(file, items).map {
      case (file, items) =>
        getOrThrow(valueOrError(
          fm.writeBinary[IO](file, append).flatMap { writer =>
            serializeSequence[IO](items, outputProperties)
              .to(writer)
              .run
          }.map(_ => Sequence.EMPTY_SEQUENCE).attempt.unsafeRunSync()
        ))
    }.getOrElse(invalidArg)
  }

  @throws[XPathException]
  private def appendBinary(args: Array[Sequence]) : Sequence = appendOrWriteBinary(args, append = true)

  @throws[XPathException]
  private def writeBinary(args: Array[Sequence]) : Sequence = {
    val file = sarg(args)(0)
    val value = args.get(1).map(_.itemAt(0).asInstanceOf[BinaryValue])
    val offset = iarg(args)(2).map(_.getInt).getOrElse(0)

    zip(file, value).map {
      case (file, value) =>
        getOrThrow(valueOrError(
          fm.writeBinary[IO](file, offset).flatMap { writer =>
            io.readInputStream(IO(value.getInputStream), DEFAULT_BUF_SIZE, true)
              .to(writer)
              .run
          }.map(_ => Sequence.EMPTY_SEQUENCE).attempt.unsafeRunSync()
        ))
    }.getOrElse(invalidArg)
  }

  @throws[XPathException]
  private def appendOrWriteBinary[F[_]](args : Array[Sequence], append: Boolean) : Sequence = {
    val file = sarg(args)(0)
    val value = args.get(1).map(_.itemAt(0).asInstanceOf[BinaryValue])

    zip(file, value).map {
      case (file, value) =>
        getOrThrow(valueOrError(
          fm.writeBinary[IO](file, append).flatMap { writer =>
            io.readInputStream(IO(value.getInputStream), DEFAULT_BUF_SIZE, true)
              .to(writer)
              .run
          }.map(_ => Sequence.EMPTY_SEQUENCE).attempt.unsafeRunSync()
        ))
    }.getOrElse(invalidArg)
  }

  @throws[XPathException]
  private def appendOrWriteText(args: Array[Sequence], append: Boolean) : Sequence = {
    val file = sarg(args)(0)
    val value = sarg(args)(1)
    val encoding = arg[StringValue](args)(2).map(_.getStringValue).getOrElse("UTF-8")

    zip(file, value).map {
      case (file, value) =>
        getOrThrow(valueOrError(
          fm.writeText[IO](file, append, encoding).flatMap { writer =>
            val stream: Stream[IO, String] = Stream.emit(value.getStringValue)
            stream
              .to(writer)
              .run
          }.map(_ => Sequence.EMPTY_SEQUENCE).attempt.unsafeRunSync()
        ))
    }.getOrElse(invalidArg)
  }

  @throws[XPathException]
  private def appendOrWriteTextLines(args: Array[Sequence], append: Boolean) : Sequence = {
    val file = sarg(args)(0)
    val lines: Option[Sequence] = args.get(1)
    val encoding = arg[StringValue](args)(2).map(_.getStringValue).getOrElse("UTF-8")

    zip(file, lines).map {
      case (file, lines) if (!lines.isEmpty) =>
        getOrThrow(valueOrError(
          fm.writeText[IO](file, append, encoding).flatMap { writer =>
          val stream: Stream[IO, String] = Stream.unfold(lines.iterate){ it =>
            if(it.hasNext) {
              Some((it.nextItem(), it))
            } else {
              None
            }
          }.map(_.asInstanceOf[StringValue].getStringValue)
          stream
              .to(writer)
              .run
          }.map(_ => Sequence.EMPTY_SEQUENCE).attempt.unsafeRunSync()
      ))
    }.getOrElse(invalidArg)
  }

  /**
   * Helper function for FileModule functions that operate on a single
   * string parameter... typically a path.
   */
  @throws[XPathException]
  private def fileProperty[T](args: Array[Sequence])(fn: (String) => T) = sarg(args)(0).map(fn(_)).getOrElse(invalidArg)
  
  /**
   * Serializes a sequence
   *
   * @param outputProperties Any output properties to set on the serializer
   * @param seq The sequence of items to serialize
   * 
   * @return The serialized items
   */
  @throws[RuntimeException]
  private def serializeSequence[F[_]](seq: Sequence, outputProperties: JProperties)(implicit F: Sync[F]) : Stream[F, Byte] = {
    val in : F[InputStream] = F.delay {
      val baos = new JByteArrayOutputStream()
      try {
        val writer = new JOutputStreamWriter(baos)
        try {
          val xqSerializer = new XQuerySerializer(context.getBroker(), outputProperties, writer)
          xqSerializer.serialize(seq)
          new ByteArrayInputStream(baos.toByteArray)
        } finally {
          writer.close()
        }
      } finally {
        baos.close()
      }
    }

    fs2.io.readInputStream(in, FileModule.DEFAULT_BUF_SIZE, true)
  }

  /**
   * Throw a standard invalid argument XPathException
   */
  @throws[XPathException]
  private def invalidArg = throw new XPathException(getLine(), getColumn(), "Missing function argument")

  /**
   * Adds a safe get(Int) : Maybe[T] method to any Array[T] object
   */
  private implicit class MyArray[T](val array: Array[T]) {
    def get(idx: Int): Option[T] = scala.util.Try(array(idx)).toOption
  }

  /**
   * Extract a single argument
   *
   * @param idx The index of the argument
   * @param iidx If the argument is a sequence, then the item index within the sequence.
   *       Defaults to 0 which is suitable for an atomic item
   */
  private def arg[T <: Item](args: Array[Sequence])(idx: Int, iidx: Int = 0) : Option[T] = args.get(idx).map(_.itemAt(iidx).asInstanceOf[T])

  /**
   * Extract a single string value argument
   *
   * @param idx The index of the argument
   * @param iidx If the argument is a sequence, then the item index within the sequence.
   *       Defaults to 0 which is suitable for an atomic item
   */
  private def sarg(args: Array[Sequence])(idx: Int, iidx: Int = 0) : Option[StringValue] = arg[StringValue](args)(idx, iidx)

  /**
   * Extract a single boolean value argument
   *
   * @param idx The index of the argument
   * @param iidx If the argument is a sequence, then the item index within the sequence.
   *       Defaults to 0 which is suitable for an atomic item
   */
  private def barg(args: Array[Sequence])(idx: Int, iidx: Int = 0) : Option[BooleanValue] = arg[BooleanValue](args)(idx, iidx)

  /**
   * Extract a single integer value argument
   *
   * @param idx The index of the argument
   * @param iidx If the argument is a sequence, then the item index within the sequence.
   *       Defaults to 0 which is suitable for an atomic item
   */
  private def iarg(args: Array[Sequence])(idx: Int, iidx: Int = 0) : Option[IntegerValue] = arg[IntegerValue](args)(idx, iidx)

  /**
   * Extracts a value from the right or throws
   * an XPathException for the error on the left
   */
  @throws[XPathException]
  private implicit def getOrThrow[T](value: Either[FileModuleException, T]): T = {
    value match {
      case Right(v) => v
      case Left(fme) => throw toXPathException(fme)
    }
  }

  /**
   * Creates an XPathException from a FileModuleException
   */
  private def toXPathException(fme: FileModuleException): XPathException = {
    val errorCode = new ErrorCode(new QName(fme.fileModuleError.name, FileModule.NAMESPACE), fme.fileModuleError.description)
    new XPathException(this, errorCode, fme.fileModuleError.description, null, fme)
  }

  /**
   * Flattens out a result to extract an
   * either error or value. Exceptions are
   * converted to FileModuleErrors.IoError
   */
  private def valueOrError[T](v: Either[Throwable, T]) : Either[FileModuleException, T] = {
    v match {
      case Right(r) => Right(r)
      case Left(fileModuleEx: FileModuleException) => Left(fileModuleEx)
      case Left(t) => Left(new FileModuleException(FileModuleErrors.IoError, t))
    }
  }

  private implicit def sv2s(value: Option[StringValue]) : Option[String] = value.map(_.getStringValue)
  private implicit def bv2b(value: Option[BooleanValue]) : Option[Boolean] = value.map(_.getValue)
  private implicit def iv2i(value: Option[IntegerValue]) : Option[Int] = value.map(_.getInt)
}
