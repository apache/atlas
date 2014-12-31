/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metadata.tools.thrift

import scala.util.parsing.combinator.{ImplicitConversions, PackratParsers}
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

import scala.util.parsing.input.CharArrayReader._

object BASE_TYPES extends Enumeration {
  val STRING = Value("string")
  val BINARY = Value("binary")
  val SLIST = Value("slist")
  val BOOLEAN = Value("bool")
  val BYTE = Value("byte")
  val I16 = Value("i16")
  val I32 = Value("i32")
  val I64 = Value("i64")
  val DOUBLE = Value("double")
}

object THRIFT_LANG extends Enumeration {
  val CPP       = Value("cpp")
  val PHP       = Value("php")
  val PY        = Value("py")
  val PERL      = Value("perl")
  val RUBY      = Value("ruby")
  val SMLTK_CAT = Value("smalltalk.category")
  val SMLTK_PRE = Value("smalltalk.prefix")
  val JAVA      = Value("java")
  val COCOA     = Value("cocoa")
  val XSD       = Value("xsd")
  val CSHARP    = Value("csharp")
  val STAR      = Value("*")
  val OTHER     = Value("")
}

case class TypeAnnotation(name : String, value : String)
case class CPPType(name : String)
sealed trait FunctionType
case class VoidType() extends FunctionType
sealed trait FieldType extends FunctionType
case class IdentifierType(name : String) extends FieldType
case class BaseType(typ : BASE_TYPES.Value, typAnnotations :Option[List[TypeAnnotation]]) extends FieldType
sealed trait ContainerType extends FieldType {
  def typAnnotations :Option[List[TypeAnnotation]]
}
case class MapType(keyType : FieldType, valueType : FieldType,
                   cppType : Option[CPPType],
                   typAnnotations :Option[List[TypeAnnotation]]) extends ContainerType
case class SetType(elemType : FieldType,
                   cppType : Option[CPPType],
                   typAnnotations :Option[List[TypeAnnotation]]) extends ContainerType
case class ListType(elemType : FieldType,
                   cppType : Option[CPPType],
                   typAnnotations :Option[List[TypeAnnotation]]) extends ContainerType

sealed trait ConstValue
case class IntConstant(value : Int) extends ConstValue
case class DoubleConstant(value : Double) extends ConstValue
case class StringConstant(value : String) extends ConstValue
case class IdConstant(value : String) extends ConstValue
case class ConstantList(value : List[ConstValue]) extends ConstValue
case class ConstantValuePair(first : ConstValue, second : ConstValue)
case class ConstantMap(value : List[ConstantValuePair]) extends ConstValue

case class ConstDef(fieldType : FieldType, id : String, value : ConstValue)

case class TypeDef(name : String, fieldType : FieldType,
                   typAnnotations :Option[List[TypeAnnotation]])
case class EnumValueDef(value : String, id : Option[IntConstant], typAnnotations :Option[List[TypeAnnotation]])
case class EnumDef(name : String, enumValues : List[EnumValueDef], typAnnotations :Option[List[TypeAnnotation]])

case class SEnumDef(name : String, enumValues : List[String], typAnnotations :Option[List[TypeAnnotation]])

case class FieldDef(id : Option[IntConstant], requiredNess : Boolean, fieldType : FieldType, name : String,
                     fieldValue : Option[ConstValue], xsdOptional : Boolean, xsdNillable : Boolean,
                     xsdAttributes: Option[XsdAttributes],
                     typAnnotations :Option[List[TypeAnnotation]])

case class XsdAttributes(fields : List[FieldDef])

case class StructDef(name : String, xsdAll : Boolean, fields : List[FieldDef],
                     typAnnotations :Option[List[TypeAnnotation]])

case class UnionDef(val name : String, val xsdAll : Boolean,
                    val fields : List[FieldDef],
                    val typAnnotations :Option[List[TypeAnnotation]])

case class ExceptionDef(val name : String,
                        val fields : List[FieldDef],
                        val typAnnotations :Option[List[TypeAnnotation]])

case class FunctionDef(oneway : Boolean, returnType : FunctionType, name : String, parameters : List[FieldDef],
                        throwFields : Option[List[FieldDef]], typAnnotations :Option[List[TypeAnnotation]])

case class ServiceDef(name : String, superName : Option[String], functions : List[FunctionDef],
                      typAnnotations :Option[List[TypeAnnotation]])

case class IncludeDef(value : String)
case class CppIncludeDef(val value : String)
case class NamespaceDef(lang : THRIFT_LANG.Value, name : String, otherLang : Option[String] = None)

case class ThriftDef(val includes : List[IncludeDef],
                      val cppIncludes : List[CppIncludeDef],
                      val namespaces : List[NamespaceDef],
                      val constants : List[ConstDef],
                      val typedefs : List[TypeDef],
                      val enums : List[EnumDef],
                      val senums : List[SEnumDef],
                      val structs : List[StructDef],
                      val unions : List[UnionDef],
                      val xceptions : List[ExceptionDef],
                      val services : List[ServiceDef]) {

  def this() = this(List(), List(), List(), List(), List(), List(), List(),
    List(), List(), List(), List())

  def this(a : IncludeDef) = this(a :: Nil, List(), List(), List(), List(), List(), List(),
    List(), List(), List(), List())
  def this(a : CppIncludeDef) = this(List(), a :: Nil, List(), List(), List(), List(), List(), List(),
    List(), List(), List())
  def this(a : NamespaceDef) = this(List(), List(), a :: Nil, List(), List(), List(), List(), List(), List(),
    List(), List())
  def this(a : ConstDef) = this(List(), List(), List(), a :: Nil, List(), List(), List(), List(), List(), List(),
    List())
  def this(a : TypeDef) = this(List(), List(), List(), List(), a :: Nil, List(), List(), List(), List(), List(), List())
  def this(a : EnumDef) = this(List(), List(), List(), List(), List(), a :: Nil, List(), List(),
    List(), List(), List())
  def this(a : SEnumDef) = this(List(), List(), List(), List(), List(), List(), a :: Nil, List(),
    List(), List(), List())
  def this(a : StructDef) = this(List(), List(), List(), List(), List(), List(), List(), a :: Nil,
    List(), List(), List())
  def this(a : UnionDef) = this(List(), List(), List(), List(), List(), List(), List(),
    List(), a :: Nil, List(), List())
  def this(a : ExceptionDef) = this(List(), List(), List(), List(), List(), List(), List(),
    List(), List(), a :: Nil, List())
  def this(a : ServiceDef) = this(List(), List(), List(), List(), List(), List(), List(),
    List(), List(), List(), a :: Nil)


  def plus(a : IncludeDef) = ThriftDef(includes.+:(a), cppIncludes, namespaces, constants, typedefs, enums, senums,
    structs, unions, xceptions, services)
  def plus(a : CppIncludeDef) = ThriftDef(includes, cppIncludes.+:(a), namespaces, constants, typedefs, enums, senums,
    structs, unions, xceptions, services)
  def plus(a : NamespaceDef) = ThriftDef(includes, cppIncludes, namespaces.+:(a), constants, typedefs, enums, senums,
    structs, unions, xceptions, services)
  def plus(a : ConstDef) = ThriftDef(includes, cppIncludes, namespaces, constants.+:(a), typedefs, enums, senums,
    structs, unions, xceptions, services)
  def plus(a : TypeDef) = ThriftDef(includes, cppIncludes, namespaces, constants, typedefs.+:(a), enums, senums,
    structs, unions, xceptions, services)
  def plus(a : EnumDef) = ThriftDef(includes, cppIncludes, namespaces, constants, typedefs, enums.+:(a), senums,
    structs, unions, xceptions, services)
  def plus(a : SEnumDef) = ThriftDef(includes, cppIncludes, namespaces, constants, typedefs, enums, senums.+:(a),
    structs, unions, xceptions, services)
  def plus(a : StructDef) = ThriftDef(includes, cppIncludes, namespaces, constants, typedefs, enums, senums,
    structs.+:(a), unions, xceptions, services)
  def plus(a : UnionDef) = ThriftDef(includes, cppIncludes, namespaces, constants, typedefs, enums, senums,
    structs, unions.+:(a), xceptions, services)
  def plus(a : ExceptionDef) = ThriftDef(includes, cppIncludes, namespaces, constants, typedefs, enums, senums,
    structs, unions, xceptions.+:(a), services)
  def plus(a : ServiceDef) = ThriftDef(includes, cppIncludes, namespaces, constants, typedefs, enums, senums,
    structs, unions, xceptions, services.+:(a))
  def plus(a : ThriftDef) = ThriftDef(includes ::: a.includes,
    cppIncludes ::: a.cppIncludes,
    namespaces ::: a.namespaces,
    constants ::: a.constants,
    typedefs ::: a.typedefs,
    enums ::: a.enums,
    senums ::: a.senums,
    structs ::: a.structs,
    unions ::: a.unions,
    xceptions ::: a.xceptions,
    services ::: a.services)


}

trait ThriftKeywords {
  this : StandardTokenParsers =>

  import scala.language.implicitConversions

  protected case class Keyword(str: String)

  protected implicit def asParser(k: Keyword): Parser[String] = k.str

  protected val LPAREN      = Keyword("(")
  protected val RPAREN      = Keyword(")")
  protected val EQ          = Keyword("=")
  protected val CPP_TYPE    = Keyword("cpp_type")
  protected val LIST        = Keyword("list")
  protected val LT          = Keyword("<")
  protected val GT          = Keyword(">")
  protected val SET         = Keyword("set")
  protected val MAP         = Keyword("map")
  protected val STRING      = Keyword("string")
  protected val BINARY      = Keyword("binary")
  protected val SLIST       = Keyword("slist")
  protected val BOOL        = Keyword("bool")
  protected val BYTE        = Keyword("byte")
  protected val I16         = Keyword("i16")
  protected val I32         = Keyword("i32")
  protected val I64         = Keyword("i64")
  protected val DOUBLE      = Keyword("double")
  protected val VOID        = Keyword("void")
  protected val REQUIRED    = Keyword("required")
  protected val OPTIONAL    = Keyword("optional")
  protected val COLON       = Keyword(":")
  protected val THROWS      = Keyword("throws")
  protected val ONEWAY      = Keyword("oneway")
  protected val EXTENDS     = Keyword("extends")
  protected val SERVICE      = Keyword("service")
  protected val EXCEPTION   = Keyword("exception")
  protected val LBRACKET    = Keyword("{")
  protected val RBRACKET    = Keyword("}")
  protected val XSD_ATTRS   = Keyword("xsd_attributes")
  protected val XSD_NILBLE  = Keyword("xsd_nillable")
  protected val XSD_OPT     = Keyword("xsd_optional")
  protected val XSD_ALL     = Keyword("xsd_all")
  protected val UNION       = Keyword("union")
  protected val LSQBRACKET  = Keyword("[")
  protected val RSQBRACKET  = Keyword("]")
  protected val CONST       = Keyword("const")
  protected val STRUCT      = Keyword("struct")
  protected val SENUM       = Keyword("senum")
  protected val ENUM        = Keyword("enum")
  protected val COMMA       = Keyword(",")
  protected val SEMICOLON   = Keyword(";")
  protected val TYPEDEF     = Keyword("typedef")
  protected val INCLUDE     = Keyword("include")
  protected val CPP_INCL    = Keyword("cpp_include")
  protected val NAMESPACE   = Keyword("namespace")
  protected val STAR        = Keyword("*")
  protected val CPP_NS      = Keyword("cpp_namespace")
  protected val PHP_NS      = Keyword("php_namespace")
  protected val PY_NS       = Keyword("py_module")
  protected val PERL_NS     = Keyword("perl_package")
  protected val RUBY_NS     = Keyword("ruby_namespace")
  protected val SMLTK_CAT   = Keyword("smalltalk_category")
  protected val SMLTK_PRE   = Keyword("smalltalk_prefix")
  protected val JAVA_NS     = Keyword("java_package")
  protected val COCOA_NS    = Keyword("cocoa_package")
  protected val XSD_NS      = Keyword("xsd_namespace")
  protected val CSHARP_NS   = Keyword("csharp_namespace")

  def isRequired(r : Option[String]) = r match {
    case Some(REQUIRED) => true
    case _ => false
  }

  def isXsdOptional(r : Option[String]) = r match {
    case Some(XSD_OPT) => true
    case _ => false
  }

  def isXsdNillable(r : Option[String]) = r match {
    case Some(XSD_NILBLE) => true
    case _ => false
  }

  def isXsdAll(r : Option[String]) = r match {
    case Some(XSD_ALL) => true
    case _ => false
  }

  def isOneWay(r : Option[String]) = r match {
    case Some(ONEWAY) => true
    case _ => false
  }

}

trait ThriftTypeRules extends ThriftKeywords {
  this : StandardTokenParsers  =>

  def containterType : Parser[ContainerType] = mapType | setType | listType

  def setType = SET ~ cppType.? ~ LT ~ fieldType ~ GT ~ typeAnnotations.? ^^ {
    case s ~ ct ~ lt ~ t ~ gt ~ tA => SetType(t, ct, tA)
  }

  def listType = LIST ~ LT ~ fieldType ~ GT ~ cppType.? ~ typeAnnotations.? ^^ {
    case l ~ lt ~ t ~ gt ~ ct ~ tA => ListType(t, ct, tA)
  }

  def mapType = MAP ~ cppType.? ~ LT ~ fieldType ~ COMMA ~ fieldType ~ GT ~ typeAnnotations.? ^^ {
    case s ~ ct ~ lt ~ kt ~ c ~ vt ~ gt ~ tA => MapType(kt, vt, ct, tA)
  }

  def cppType : Parser[CPPType] = CPP_TYPE ~ stringLit ^^ { case c ~ s => CPPType(s)}

  def fieldType: Parser[FieldType]  = ident ^^ {case i => IdentifierType(i)} |
    baseType |
    containterType

  def baseType : Parser[BaseType] = simpleBaseType ~ typeAnnotations.? ^^ { case s ~ t => BaseType(s, t)}

  def simpleBaseType : Parser[BASE_TYPES.Value] = STRING ^^^ BASE_TYPES.STRING |
    BINARY ^^^ BASE_TYPES.BINARY |
    SLIST ^^^ BASE_TYPES.SLIST |
    BOOL ^^^ BASE_TYPES.BOOLEAN |
    BYTE ^^^ BASE_TYPES.BYTE |
    I16 ^^^ BASE_TYPES.I16 |
    I32 ^^^ BASE_TYPES.I32 |
    I64 ^^^ BASE_TYPES.I64 |
    DOUBLE ^^^ BASE_TYPES.DOUBLE

  def typeAnnotations : Parser[List[TypeAnnotation]] =
    LPAREN ~ typeAnnotation.* ~ RPAREN ^^ { case l ~ t ~ r => t.toList}

  def typeAnnotation : Parser[TypeAnnotation] =
    (ident ~ EQ ~ stringLit ~ commaOrSemicolon.?) ^^ { case i ~ e ~ s ~ c  => TypeAnnotation(i,s)}

  def commaOrSemicolon : Parser[String] = COMMA | SEMICOLON

}

/**
 * @todo extract Constant Rules into this Trait. This requires moving `hexConstant` here. But how to specify
 *       type of `HexConstant`, it is a Path dependent Type tied to lexical member of ThriftParser.
 */
trait ThriftConstantRules extends ThriftKeywords {
  this: StandardTokenParsers =>

//  def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ : Throwable => None }
//
//  def constValue : Parser[ConstValue] = numericLit ^^ {
//    case n => parseDouble(n) match {
//      case Some(d) => DoubleConstant(d)
//      case _ => IntConstant(n.toInt)
//    }
//  } |
//    hexConstant ^^ { case h => IntConstant(Integer.parseInt(h, 16))} |
//    stringLit ^^ { case s => StringConstant(s)} |
//    ident ^^ { case i => IdConstant(i)} |
//    constList |
//    constMap
//
//  def constValuePair = constValue ~ COLON ~ constValue ~ commaOrSemicolon.? ^^ {
//    case k ~ c ~ v ~ cs => ConstantValuePair(k,v)
//  }
//
//  def constList = LSQBRACKET ~ (constValue <~ commaOrSemicolon).* ~ RSQBRACKET ^^ {
//    case l ~ vs ~ r => ConstantList(vs)
//  }
//
//  def constMap = LBRACKET ~ constValuePair.* ~ RBRACKET ^^ {
//    case l ~ ps ~ r => ConstantMap(ps)
//  }
}

/**
 * A Parser for Thrift definition scripts.
 * Based on [[https://github.com/twitter/commons/blob/master/src/antlr/twitter/thrift/descriptors/AntlrThrift.g]].
 * Definition is parsed into a [[org.apache.hadoop.metadata.tools.thrift.ThriftDef ThriftDef]] structure.
 *
 *  @example {{{
 *  var p = new ThriftParser
 *  var td : Option[ThriftDef] = p("""include "share/fb303/if/fb303.thrift"
 *                namespace java org.apache.hadoop.hive.metastore.api
 *                namespace php metastore
 *                namespace cpp Apache.Hadoop.Hive
 *                \""")
 *  }}}
 *
 * @todo doesn't traverse includes directives. Includes are parsed into
 *       [[org.apache.hadoop.metadata.tools.thrift.IncludeDef IncludeDef]] structures
 *       but are not traversed.
 * @todo mixing in [[scala.util.parsing.combinator.PackratParsers PackratParsers]] is a placeholder. Need to
 *       change specific grammar rules to `lazy val` and `Parser[Elem]` to `PackratParser[Elem]`. Will do based on
 *       performance analysis.
 * @todo Error reporting
 */
class ThriftParser extends StandardTokenParsers with ThriftKeywords with ThriftTypeRules with PackratParsers {

  import scala.language.higherKinds

  private val reservedWordsDelims : Seq[String] =
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  private val (thriftreservedWords : Seq[String], thriftdelims : Seq[String]) =
    reservedWordsDelims.partition(s => s.charAt(0).isLetter)

  override val lexical = new ThriftLexer(thriftreservedWords, thriftdelims)

  import lexical.HexConstant
  /** A parser which matches a hex constant */
  def hexConstant: Parser[String] =
    elem("string literal", _.isInstanceOf[HexConstant]) ^^ (_.chars)

  def apply(input: String): Option[ThriftDef] = {
    phrase(program)(new lexical.Scanner(input)) match {
      case Success(r, x) => Some(r)
      case Failure(m, x) => {
        None
      }
      case Error(m, x) => {
        None
      }
    }
  }

  def program = headers ~ definitions ^^ { case h ~ d => h plus d}

  def headers = header.*  ^^ { case l => l.foldRight(new ThriftDef)((a,t) => t plus a)}

  def header = INCLUDE ~> stringLit ^^ { case s => new ThriftDef(IncludeDef(s))} |
    CPP_INCL ~> stringLit ^^ { case s => new ThriftDef(CppIncludeDef(s))} |
    NAMESPACE ~ ident ~ ident ^^ { case ns ~ t ~ n => new ThriftDef(NamespaceDef(THRIFT_LANG.OTHER, t, Some(n)))} |
    NAMESPACE ~ STAR ~ ident ^^ { case ns ~ s ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.STAR, i))} |
    CPP_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.CPP, i))} |
    PHP_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.PHP, i))} |
    PY_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.PY, i))} |
    PERL_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.PERL, i))} |
    RUBY_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.RUBY, i))} |
    SMLTK_CAT ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.SMLTK_CAT, i))} |
    SMLTK_PRE ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.SMLTK_PRE, i))} |
    JAVA_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.JAVA, i))} |
    COCOA_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.COCOA, i))} |
    XSD_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.XSD, i))} |
    CSHARP_NS ~ ident ^^ { case ns ~ i => new ThriftDef(NamespaceDef(THRIFT_LANG.CSHARP, i))}

  def definitions : Parser[ThriftDef] = definition.*  ^^ {
    case l => l.foldRight(new ThriftDef)((a,t) => t plus a)
  }

  def definition : Parser[ThriftDef] = const ^^ { case c => new ThriftDef(c)} |
    typeDefinition |
    service ^^ { case s => new ThriftDef(s)}


  def typeDefinition : Parser[ThriftDef] = (typedef ^^ {case t => new ThriftDef(t)} |
    enum ^^ {case e => new ThriftDef(e)} |
    senum ^^ {case e => new ThriftDef(e)} |
    struct ^^ {case e => new ThriftDef(e)} |
    union ^^ {case e => new ThriftDef(e)} |
    xception ^^ {case e => new ThriftDef(e)}
    )

  def typedef : Parser[TypeDef] = TYPEDEF ~ fieldType ~ ident ~ typeAnnotations.? ^^ {
    case t ~ f ~ i ~ tA => TypeDef(i, f, tA)
  }

  def enum : Parser[EnumDef] = ENUM ~ ident ~ LBRACKET ~ enumDef.* ~ RBRACKET ~ typeAnnotations.? ^^ {
    case e ~ i ~  l ~ ed ~ r ~ t => EnumDef(i, ed.toList, t)
  }

  def enumDef : Parser[EnumValueDef] = ident ~ EQ ~ numericLit ~ typeAnnotations.? ~ commaOrSemicolon.? ^^ {
    case i ~ e ~ n ~ t ~ c => EnumValueDef(i, Some(IntConstant(n.toInt)), t)
  }

  def senum : Parser[SEnumDef] = SENUM ~ ident ~ LBRACKET ~ senumDef.* ~ RBRACKET ~ typeAnnotations.? ^^ {
    case se ~ i ~  l ~ sed ~ r ~ t => SEnumDef(i, sed.toList, t)
  }

  def senumDef : Parser[String] = stringLit <~ commaOrSemicolon.?

  def service : Parser[ServiceDef] = SERVICE ~ ident ~ extnds.? ~ LBRACKET ~ function.* ~
    RBRACKET ~ typeAnnotations.? ^^ {
    case s ~ i ~ e ~ lb ~ fs ~ rb ~ tA => ServiceDef(i, e, fs, tA)
  }

  def extnds : Parser[String] = EXTENDS ~> ident

  def function : Parser[FunctionDef] = ONEWAY.? ~ functionType ~ ident ~ LPAREN ~ field.* ~ RPAREN ~ throwz.? ~
    typeAnnotations.? ~ commaOrSemicolon.? ^^ {
    case o ~ fT ~ i ~ lp ~ fs ~ rp ~ th ~ tA ~ cS => FunctionDef(isOneWay(o), fT, i, fs, th, tA)
  }

  def throwz : Parser[List[FieldDef]] = THROWS ~ LPAREN ~ field.* ~ RPAREN ^^ {
    case t ~ l ~ fs ~ r => fs.toList
  }

  def functionType : Parser[FunctionType] = VOID ^^^ VoidType() | fieldType

  def xception : Parser[ExceptionDef] = EXCEPTION ~ ident ~ LBRACKET ~ field.* ~ RBRACKET ~ typeAnnotations.? ^^ {
    case s ~ i ~ lb ~ fs ~ rb ~ tA => ExceptionDef(i, fs.toList, tA)
  }

  def union : Parser[UnionDef] = UNION ~ ident ~ XSD_ALL.? ~ LBRACKET ~ field.* ~ RBRACKET ~ typeAnnotations.? ^^ {
    case s ~ i ~ xA ~ lb ~ fs ~ rb ~ tA => UnionDef(i, isXsdAll(xA), fs.toList, tA)
  }

  def struct : Parser[StructDef] = STRUCT ~ ident ~ XSD_ALL.? ~ LBRACKET ~ field.* ~ RBRACKET ~ typeAnnotations.? ^^ {
    case s ~ i ~ xA ~ lb ~ fs ~ rb ~ tA => StructDef(i, isXsdAll(xA), fs.toList, tA)
  }

  def field : Parser[FieldDef] = fieldIdentifier.? ~ fieldRequiredness.? ~ fieldType ~ ident ~ fieldValue.? ~
    XSD_OPT.? ~ XSD_NILBLE.? ~ xsdAttributes.? ~ typeAnnotations.? ~ commaOrSemicolon.? ^^ {
    case fi ~ fr ~ ft ~id ~ fv ~ xo ~ xn ~ xa ~ tA ~ cS => FieldDef(
    fi,
    isRequired(fr),
    ft,
    id,
    fv,
    isXsdOptional(xo),
    isXsdNillable(xn),
    xa,
    tA
    )
  }

  def xsdAttributes : Parser[XsdAttributes] = XSD_ATTRS ~ LBRACKET ~ field.* ~ RBRACKET ^^ {
    case x ~ l ~ f ~ r => XsdAttributes(f)
  }

  def fieldValue = EQ ~> constValue

  def fieldRequiredness : Parser[String] = REQUIRED | OPTIONAL

  def fieldIdentifier : Parser[IntConstant] = numericLit <~ COLON ^^ {
    case n => IntConstant(n.toInt)
  }

  def const : Parser[ConstDef] = CONST ~ fieldType ~ ident ~ EQ ~ constValue ~ commaOrSemicolon.? ^^ {
    case c ~ fT ~ i ~ e ~ cV ~ cS => ConstDef(fT, i, cV)
  }

  def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ : Throwable => None }

  def constValue : Parser[ConstValue] = numericLit ^^ {
    case n => parseDouble(n) match {
      case Some(d) => DoubleConstant(d)
      case _ => IntConstant(n.toInt)
    }
  } |
  hexConstant ^^ { case h => IntConstant(Integer.parseInt(h, 16))} |
  stringLit ^^ { case s => StringConstant(s)} |
  ident ^^ { case i => IdConstant(i)} |
  constList |
  constMap

  def constValuePair = constValue ~ COLON ~ constValue ~ commaOrSemicolon.? ^^ {
    case k ~ c ~ v ~ cs => ConstantValuePair(k,v)
  }

  def constList = LSQBRACKET ~ (constValue <~ commaOrSemicolon).* ~ RSQBRACKET ^^ {
    case l ~ vs ~ r => ConstantList(vs)
  }

  def constMap = LBRACKET ~ constValuePair.* ~ RBRACKET ^^ {
    case l ~ ps ~ r => ConstantMap(ps)
  }
}

class ThriftLexer(val keywords: Seq[String], val delims : Seq[String]) extends StdLexical with ImplicitConversions {

  case class HexConstant(chars: String) extends Token {
    override def toString = chars
  }

  case class StIdentifier(chars: String) extends Token {
    override def toString = chars
  }

  reserved ++= keywords

  delimiters ++= delims

  override lazy val token: Parser[Token] =
    ( intConstant ^^ NumericLit
      | hexConstant ^^ HexConstant
      | dubConstant ^^ NumericLit
      | identifier ^^ processIdent
      | st_identifier ^^ StIdentifier
      | string ^^ StringLit
      | EofCh ^^^ EOF
      | '\'' ~> failure("unclosed string literal")
      | '"' ~> failure("unclosed string literal")
      | delim
      | failure("illegal character")
      )

  override def identChar = letter | elem('_')

  def identifier = identChar ~ (identChar | digit | '.' ).* ^^
    { case first ~ rest => (first :: rest).mkString }

  def st_identChar = letter | elem('-')
  def st_identifier = st_identChar ~ (st_identChar | digit | '.' | '_').* ^^
    { case first ~ rest => (first :: rest).mkString("")}

  override def whitespace: Parser[Any] =
    ( whitespaceChar
      | '/' ~ '*' ~ comment
      | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
      | '#' ~ chrExcept(EofCh, '\n').*
      | '/' ~ '*' ~ failure("unclosed comment")
      ).*

  protected override def comment: Parser[Any] = (
    commentChar.* ~ '*' ~ '/'
    )

  protected def commentChar = chrExcept(EofCh, '*') | '*' ~ not('/')

  def string = '\"' ~> chrExcept('\"', '\n', EofCh).* <~ '\"' ^^ { _ mkString "" } |
    '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^ { _ mkString "" }

  def zero: Parser[String] = '0' ^^^ "0"
  def nonzero = elem("nonzero digit", d => d.isDigit && d != '0')
  def sign = elem("sign character", d => d == '-' || d == '+')
  def exponent = elem("exponent character", d => d == 'e' || d == 'E')


  def intConstant = opt(sign) ~> zero | intList
  def intList = opt(sign) ~ nonzero ~ rep(digit) ^^ {case s ~ x ~ y =>  (optString("", s) :: x :: y) mkString ""}
  def fracPart = '.' ~> rep(digit) ^^ { "." + _ mkString "" }
  def expPart = exponent ~ opt(sign) ~ rep1(digit) ^^ { case e ~ s ~ d =>
    e.toString + optString("", s) + d.mkString("")
  }

  def dubConstant = opt(sign) ~ digit.* ~ fracPart ~ opt(expPart) ^^ { case s ~ i ~ f ~ e =>
    optString("", s) + i + f + optString("", e)
  }

  val hexDigits = Set[Char]() ++ "0123456789abcdefABCDEF".toArray
  def hexDigit = elem("hex digit", hexDigits.contains(_))

  def hexConstant = '0' ~> 'x' ~> hexDigit.+ ^^ {case h => h.mkString("")}


  private def optString[A](pre: String, a: Option[A]) = a match {
    case Some(x) => pre + x.toString
    case None => ""
  }

}
