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
package org.exist.xdm

import org.exist.dom.QName
import org.exist.xquery.{Cardinality, FunctionSignature}
import org.exist.xquery.value.{
  AnyURIValue,
  BooleanValue,
  DateTimeValue,
  FunctionParameterSequenceType,
  FunctionReturnSequenceType,
  IntegerValue,
  Sequence,
  SequenceType,
  StringValue,
  Type => XQType
}

import java.net.URI
import java.util.Date

import scalaz._
import Scalaz._
import Maybe._



/**
 * eXist XDM helpers
 */
trait Typed {
  def xdmType: Int
  def cardinality: Int
}

case class Type(override val xdmType: Int, override val cardinality: Int) extends Typed

object Type {
  lazy val string_? = Type(XQType.STRING, Cardinality.ZERO_OR_ONE)
  lazy val string = Type(XQType.STRING, Cardinality.EXACTLY_ONE)
  lazy val string_+ = Type(XQType.STRING, Cardinality.ONE_OR_MORE)
  lazy val string_* = Type(XQType.STRING, Cardinality.ZERO_OR_MORE)

  lazy val boolean = Type(XQType.BOOLEAN, Cardinality.EXACTLY_ONE)

  lazy val base64Binary = Type(XQType.BASE64_BINARY, Cardinality.EXACTLY_ONE)

  lazy val integer = Type(XQType.INTEGER, Cardinality.EXACTLY_ONE)

  lazy val uri = Type(XQType.ANY_URI, Cardinality.EXACTLY_ONE)

  lazy val dateTime = Type(XQType.DATE_TIME, Cardinality.ONE)

  lazy val item_* = Type(XQType.ITEM, Cardinality.ZERO_OR_MORE)

  lazy val element = Type(XQType.ELEMENT, Cardinality.ONE)
}

object XdmImplicits {
  implicit def BooleanToXdmBoolean(value: Boolean) : BooleanValue = new BooleanValue(value)
  implicit def LongToXdmDateTime(value: Long) : DateTimeValue = new DateTimeValue(new Date(value))
  implicit def LongToXdmInteger(value: Long) : IntegerValue = new IntegerValue(value)
  implicit def StringToXdmString(value: String) : StringValue = new StringValue(value)
  implicit def XdmStringToString(value: StringValue) : String = value.getStringValue
  implicit def MStringToXdmString(value: Maybe[String]) : Sequence = value.map(new StringValue(_).asInstanceOf[Sequence]) | Sequence.EMPTY_SEQUENCE
  implicit def UriToXdmAnyUri(value: URI) : Sequence = new AnyURIValue(value)
}

object Function {

  trait Named {
    def name: String
  }

  trait Described {
    def description: String
  }

  case class Parameter(override val name: String, xdmType: Type, override val description: String) extends Named with Described
  case class ResultType(xdmType: Typed, override val description: String) extends Described

  def signatures(name: QName, description: String, multiParameters: Seq[Seq[Parameter]], resultType: Maybe[ResultType]): Seq[FunctionSignature] = {
    multiParameters.map {
      parameters =>
        new FunctionSignature(
          name,
          description,
          parameters.map(p => new FunctionParameterSequenceType(p.name, p.xdmType.xdmType, p.xdmType.cardinality, p.description)).toArray[SequenceType],
          resultType.map(rt => new FunctionReturnSequenceType(rt.xdmType.xdmType, rt.xdmType.cardinality, rt.description).asInstanceOf[SequenceType]) | new SequenceType(XQType.ITEM, Cardinality.EMPTY)
        )
    }
  }
}
