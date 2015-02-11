/**
 * Copyright © 2015, Adam Retter
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
package org.exist.expath.module.file

import scalaz.concurrent.Task
import scalaz.stream.Cause.Terminated
import scalaz.stream._
import java.io.InputStream
import scodec.bits.ByteVector

/**
 * Code provided by @mikemckibben via scalaz-stream gitter.im 20150211
 */
private[file] class ProcessInputStream(p: Process[Task, ByteVector]) extends InputStream {

  private val step = p.toTask
  private var buf = ByteVector.empty

  override def read(): Int = {
    attemptRead(1) match {
      case b if b.nonEmpty => b.get(0)
      case _ => -1
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    attemptRead(len) match {
      case bytes if bytes.nonEmpty =>
        Array.copy(bytes.toArray, 0, b, off, bytes.length)
        bytes.length
      case _ => -1
    }
  }

  override def close(): Unit = synchronized {
    @annotation.tailrec
    def go(): Unit = readBytes().isEmpty match {
      case false => go()
      case _ =>
    }
    go()
  }

  private def readBytes(): ByteVector = {
    try {
      step.run
    }
    catch {
      case _: Terminated => ByteVector.empty
    }
  }

  private def attemptRead(n: Int): ByteVector = synchronized {
    @annotation.tailrec
    def fill(bytes: ByteVector): ByteVector = {
      if (n <= bytes.size) bytes
      else {
        val next = readBytes()
        if (next.nonEmpty) fill(bytes ++ next)
        else bytes
      }
    }

    val bytes = fill(buf)
    buf = bytes.drop(n)
    bytes.take(n)
  }
}
