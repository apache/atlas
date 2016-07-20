/*
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

package org.apache.atlas.query

import com.google.common.collect.ImmutableCollection
import org.apache.atlas.AtlasException
import org.apache.atlas.typesystem.types.DataTypes.{ArrayType, PrimitiveType, TypeCategory}
import org.apache.atlas.typesystem.types._

object Expressions {

    import TypeUtils._

    class ExpressionException(val e: Expression, message: String, cause: Throwable, enableSuppression: Boolean,
                              writableStackTrace: Boolean)
        extends AtlasException(message, cause, enableSuppression, writableStackTrace) {

        def this(e: Expression, message: String) {
            this(e, message, null, false, true)
        }

        def this(e: Expression, message: String, cause: Throwable) {
            this(e, message, cause, false, true)
        }

        def this(e: Expression, cause: Throwable) {
            this(e, null, cause, false, true)
        }

        override def getMessage: String = {
            val eString = e.toString
            s"${super.getMessage}, expression:${if (eString contains "\n") "\n" else " "}$e"
        }

    }

    class UnresolvedException(expr: Expression, function: String) extends
    ExpressionException(expr, s"Unresolved $function")

    def attachExpression[A](e: Expression, msg: String = "")(f: => A): A = {
        try f catch {
            case eex: ExpressionException => throw eex
            case ex: Exception => throw new ExpressionException(e, msg, ex)
        }
    }

    trait Expression {
        self: Product =>

        def children: Seq[Expression]

        /**
         * Returns `true` if the schema for this expression and all its children have been resolved.
         * The default logic is that an Expression is resolve if all its children are resolved.
         */
        lazy val resolved: Boolean = childrenResolved

        /**
         * Returns the output [[IDataType[_]] of this expression.  Expressions that are unresolved will
         * throw if this method is invoked.
         */
        def dataType: IDataType[_]

        /**
         * Returns true if  all the children have been resolved.
         */
        def childrenResolved = !children.exists(!_.resolved)


        /**
         * the aliases that are present in this Expression Tree
         */
        def namedExpressions: Map[String, Expression] = Map()

        def fastEquals(other: Expression): Boolean = {
            this.eq(other) || this == other
        }

        def makeCopy(newArgs: Array[AnyRef]): this.type = attachExpression(this, "makeCopy") {
            try {
                val defaultCtor = getClass.getConstructors.find(_.getParameterTypes.size != 0).head
                defaultCtor.newInstance(newArgs: _*).asInstanceOf[this.type]
            } catch {
                case e: java.lang.IllegalArgumentException =>
                    throw new ExpressionException(
                        this, s"Failed to copy node. Reason: ${e.getMessage}.")
            }
        }

        def transformChildrenDown(rule: PartialFunction[Expression, Expression]): this.type = {
            var changed = false
            val newArgs = productIterator.map {
                case arg: Expression if children contains arg =>
                    val newChild = arg.asInstanceOf[Expression].transformDown(rule)
                    if (!(newChild fastEquals arg)) {
                        changed = true
                        newChild
                    } else {
                        arg
                    }
                case Some(arg: Expression) if children contains arg =>
                    val newChild = arg.asInstanceOf[Expression].transformDown(rule)
                    if (!(newChild fastEquals arg)) {
                        changed = true
                        Some(newChild)
                    } else {
                        Some(arg)
                    }
                case m: Map[_, _] => m
                case args: Traversable[_] => args.map {
                    case arg: Expression if children contains arg =>
                        val newChild = arg.asInstanceOf[Expression].transformDown(rule)
                        if (!(newChild fastEquals arg)) {
                            changed = true
                            newChild
                        } else {
                            arg
                        }
                    case other => other
                }
                case nonChild: AnyRef => nonChild
                case null => null
            }.toArray
            if (changed) makeCopy(newArgs) else this
        }

        def transformDown(rule: PartialFunction[Expression, Expression]): Expression = {
            val afterRule = rule.applyOrElse(this, identity[Expression])
            // Check if unchanged and then possibly return old copy to avoid gc churn.
            if (this fastEquals afterRule) {
                transformChildrenDown(rule)
            } else {
                afterRule.transformChildrenDown(rule)
            }
        }

        def traverseChildren(traverseFunc: (Expression, PartialFunction[Expression, Unit]) => Unit)
                            (rule: PartialFunction[Expression, Unit]): Unit = {
            productIterator.foreach {
                case arg: Expression if children contains arg =>
                    traverseFunc(arg.asInstanceOf[Expression], rule)
                case Some(arg: Expression) if children contains arg =>
                    traverseFunc(arg.asInstanceOf[Expression], rule)
                case m: Map[_, _] => m
                case args: Traversable[_] => args.map {
                    case arg: Expression if children contains arg =>
                        traverseFunc(arg.asInstanceOf[Expression], rule)
                    case other => other
                }
                case nonChild: AnyRef => nonChild
                case null => null
            }
        }

        def traverseChildrenDown = traverseChildren(_traverseDown) _

        private def _traverseDown(e: Expression, rule: PartialFunction[Expression, Unit]): Unit = {
            if (rule.isDefinedAt(e)) {
                rule.apply(e)
            }
            e.traverseChildrenDown(rule)
        }

        def traverseDown(rule: PartialFunction[Expression, Unit]): Unit = {
            _traverseDown(this, rule)
        }

        def traverseChildrenUp = traverseChildren(_traverseUp) _

        private def _traverseUp(e: Expression, rule: PartialFunction[Expression, Unit]): Unit = {
            e.traverseChildrenUp(rule)
            if (rule.isDefinedAt(e)) {
                rule.apply(e)
            }
        }

        def traverseUp(rule: PartialFunction[Expression, Unit]): Unit = {
            _traverseUp(this, rule)
        }

        def transformUp(rule: PartialFunction[Expression, Expression]): Expression = {
            val afterRuleOnChildren = transformChildrenUp(rule);
            if (this fastEquals afterRuleOnChildren) {
                rule.applyOrElse(this, identity[Expression])
            } else {
                rule.applyOrElse(afterRuleOnChildren, identity[Expression])
            }
        }

        def transformChildrenUp(rule: PartialFunction[Expression, Expression]): this.type = {
            var changed = false
            val newArgs = productIterator.map {
                case arg: Expression if children contains arg =>
                    val newChild = arg.asInstanceOf[Expression].transformUp(rule)
                    if (!(newChild fastEquals arg)) {
                        changed = true
                        newChild
                    } else {
                        arg
                    }
                case Some(arg: Expression) if children contains arg =>
                    val newChild = arg.asInstanceOf[Expression].transformUp(rule)
                    if (!(newChild fastEquals arg)) {
                        changed = true
                        Some(newChild)
                    } else {
                        Some(arg)
                    }
                case m: Map[_, _] => m
                case args: Traversable[_] => args.map {
                    case arg: Expression if children contains arg =>
                        val newChild = arg.asInstanceOf[Expression].transformUp(rule)
                        if (!(newChild fastEquals arg)) {
                            changed = true
                            newChild
                        } else {
                            arg
                        }
                    case other => other
                }
                case nonChild: AnyRef => nonChild
                case null => null
            }.toArray
            if (changed) makeCopy(newArgs) else this
        }

        /*
         * treeString methods
         */
        def nodeName = getClass.getSimpleName

        def argString: String = productIterator.flatMap {
            case e: Expression if children contains e => Nil
            case e: Expression if e.toString contains "\n" => s"(${e.simpleString})" :: Nil
            case seq: Seq[_] => seq.mkString("[", ",", "]") :: Nil
            case set: Set[_] => set.mkString("{", ",", "}") :: Nil
            case f: IDataType[_] => f.getName :: Nil
            case other => other :: Nil
        }.mkString(", ")

        /** String representation of this node without any children */
        def simpleString = s"$nodeName $argString"

        protected def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
            builder.append(" " * depth)
            builder.append(simpleString)
            builder.append("\n")
            children.foreach(_.generateTreeString(depth + 1, builder))
            builder
        }

        def treeString = generateTreeString(0, new StringBuilder).toString

        /*
         * Fluent API methods
         */
        def field(fieldName: String) = new UnresolvedFieldExpression(this, fieldName)

        def join(fieldName: String) = field(fieldName)

        def `.`(fieldName: String) = field(fieldName)

        def as(alias: String) = new AliasExpression(this, alias)

        def arith(op: String)(rightExpr: Expression) = new ArithmeticExpression(op, this, rightExpr)

        def + = arith("+") _

        def - = arith("-") _

        def * = arith("*") _

        def / = arith("/") _

        def % = arith("%") _

        def isTrait(name: String) = new isTraitUnaryExpression(name, this)

        def hasField(name: String) = new hasFieldUnaryExpression(name, this)

        def compareOp(op: String)(rightExpr: Expression) = new ComparisonExpression(op, this, rightExpr)

        def `=` = compareOp("=") _

        def `!=` = compareOp("!=") _

        def `>` = compareOp(">") _

        def `>=` = compareOp(">=") _

        def `<` = compareOp("<") _

        def `<=` = compareOp("=") _

        def logicalOp(op: String)(rightExpr: Expression) = new LogicalExpression(op, List(this, rightExpr))

        def and = logicalOp("and") _

        def or = logicalOp("or") _

        def where(condExpr: Expression) = new FilterExpression(this, condExpr)

        def select(selectList: Expression*) = new SelectExpression(this, selectList.toList)

        def loop(loopingExpr: Expression) = new LoopExpression(this, loopingExpr, None)

        def loop(loopingExpr: Expression, times: Literal[Integer]) =
            new LoopExpression(this, loopingExpr, Some(times))

        def traitInstance() = new TraitInstanceExpression(this)
        def instance() = new InstanceExpression(this)

        def path() = new PathExpression(this)

        def limit(lmt: Literal[Integer], offset : Literal[Integer]) = new LimitExpression(this, lmt, offset)

        def order(odr: Expression, asc: Boolean) = new OrderExpression(this, odr, asc)
    }

    trait BinaryNode {
        self: Expression =>
        def left: Expression

        def right: Expression

        def children = Seq(left, right)

        override def namedExpressions = left.namedExpressions ++ right.namedExpressions
    }

    trait LeafNode {
        def children = Nil
    }

    trait UnaryNode {
        self: Expression =>
        def child: Expression

        override def namedExpressions = child.namedExpressions

        def children = child :: Nil
    }

    abstract class BinaryExpression extends Expression with BinaryNode {
        self: Product =>
        def symbol: String

        override def toString = s"($left $symbol $right)"
    }

    case class ClassExpression(clsName: String) extends Expression with LeafNode {
        val dataType = typSystem.getDataType(classOf[ClassType], clsName)

        override def toString = clsName
    }

    def _class(name: String): Expression = new ClassExpression(name)

    case class TraitExpression(traitName: String) extends Expression with LeafNode {
        val dataType = typSystem.getDataType(classOf[TraitType], traitName)

        override def toString = traitName
    }

    def _trait(name: String) = new TraitExpression(name)

    case class IdExpression(name: String) extends Expression with LeafNode {
        override def toString = name

        override lazy val resolved = false

        override def dataType = throw new UnresolvedException(this, "id")
    }

    def id(name: String) = new IdExpression(name)

    case class UnresolvedFieldExpression(child: Expression, fieldName: String) extends Expression
    with UnaryNode {
        override def toString = s"${child}.$fieldName"

        override lazy val resolved = false

        override def dataType = throw new UnresolvedException(this, "field")
    }

    case class FieldExpression(fieldName: String, fieldInfo: FieldInfo, child: Option[Expression])
        extends Expression {

        def elemType(t: IDataType[_]): IDataType[_] = {
            if (t.getTypeCategory == TypeCategory.ARRAY) {
                val aT = t.asInstanceOf[ArrayType]
                if (aT.getElemType.getTypeCategory == TypeCategory.CLASS ||
                    aT.getElemType.getTypeCategory == TypeCategory.STRUCT) {
                    return aT.getElemType
                }
            }
            t
        }

        val children = if (child.isDefined) List(child.get) else Nil
        import scala.language.existentials
        lazy val dataType = {
            val t = {
              if (fieldInfo.traitName != null ) {
                typSystem.getDataType(classOf[TraitType], fieldInfo.traitName)
              } else if (!fieldInfo.isReverse) {
                fieldInfo.attrInfo.dataType()
              } else {
                fieldInfo.reverseDataType
              }
            }
            elemType(t)
        }
        override lazy val resolved: Boolean = true

        override def namedExpressions = if (child.isDefined) child.get.namedExpressions else Map()

        override def toString = {
            if (child.isDefined) {
                val sep = if (dataType.isInstanceOf[ClassType]) " " else "."
                s"${child.get}${sep}$fieldName"
            } else {
                fieldName
            }
        }
    }

    case class AliasExpression(child: Expression, alias: String) extends Expression with UnaryNode {
        override def namedExpressions = child.namedExpressions + (alias -> child)

        override def toString = s"$child as $alias"

        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved child")
            }
            child.dataType
        }
    }

    case class BackReference(alias: String, reference: Expression, child: Option[Expression]) extends Expression {
        val children = if (child.isDefined) List(child.get) else Nil
        val dataType = reference.dataType

        override def namedExpressions = if (child.isDefined) child.get.namedExpressions else Map()

        override def toString = if (child.isDefined) s"${child.get} $alias" else alias
    }

    case class Literal[T](dataType: PrimitiveType[T], rawValue: Any) extends Expression with LeafNode {
        val value = if (rawValue == null) dataType.nullValue() else dataType.convert(rawValue, Multiplicity.REQUIRED)

        override def toString = value match {
            case s: String => s""""$s""""
            case x => x.toString
        }
    }

    import scala.collection.JavaConversions._
    case class ListLiteral[_](dataType: ArrayType, rawValue: List[Expressions.Literal[_]]) extends Expression with LeafNode {

        val lc : java.util.List[Expressions.Literal[_]] = rawValue
        val value = if (rawValue != null) dataType.convert(lc, Multiplicity.REQUIRED)

        override def toString = value match {
            case l: Seq[_]
               => l.mkString("[",",","]")
            case c: ImmutableCollection[_] =>
                c.asList.mkString("[",",","]")
            case x =>
                x.toString
        }
    }

    def literal[T](typ: PrimitiveType[T], rawValue: Any) = new Literal[T](typ, rawValue)

    def boolean(rawValue: Any) = literal(DataTypes.BOOLEAN_TYPE, rawValue)

    def byte(rawValue: Any) = literal(DataTypes.BYTE_TYPE, rawValue)

    def short(rawValue: Any) = literal(DataTypes.SHORT_TYPE, rawValue)

    def int(rawValue: Any) = literal(DataTypes.INT_TYPE, rawValue)

    def long(rawValue: Any) = literal(DataTypes.LONG_TYPE, rawValue)

    def float(rawValue: Any) = literal(DataTypes.FLOAT_TYPE, rawValue)

    def double(rawValue: Any) = literal(DataTypes.DOUBLE_TYPE, rawValue)

    def bigint(rawValue: Any) = literal(DataTypes.BIGINTEGER_TYPE, rawValue)

    def bigdecimal(rawValue: Any) = literal(DataTypes.BIGDECIMAL_TYPE, rawValue)

    def string(rawValue: Any) = literal(DataTypes.STRING_TYPE, rawValue)

    def date(rawValue: Any) = literal(DataTypes.DATE_TYPE, rawValue)

    def list[_ <: PrimitiveType[_]](listElements: List[Expressions.Literal[_]]) =  {
        listLiteral(TypeSystem.getInstance().defineArrayType(listElements.head.dataType), listElements)
    }

    def listLiteral[_ <: PrimitiveType[_]](typ: ArrayType, rawValue: List[Expressions.Literal[_]]) = new ListLiteral(typ, rawValue)

    case class ArithmeticExpression(symbol: String,
                                    left: Expression,
                                    right: Expression)
        extends BinaryExpression {

        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved children")
            }
            TypeUtils.combinedType(left.dataType, right.dataType)
        }
    }

    case class isTraitLeafExpression(traitName: String, classExpression: Option[Expression] = None)
        extends Expression with LeafNode {
        // validate TraitName
        try {
            typSystem.getDataType(classOf[TraitType], traitName)
        } catch {
            case me: AtlasException => throw new ExpressionException(this, "not a TraitType", me)
        }

        override lazy val resolved = classExpression.isDefined
        lazy val dataType = {

            if (!resolved) {
                throw new UnresolvedException(this,
                    s"cannot resolve isTrait application")
            }

            if (!classExpression.get.dataType.isInstanceOf[ClassType]) {
                throw new ExpressionException(this,
                    s"Cannot apply isTrait on ${classExpression.get.dataType.getName}, it is not a ClassType")
            }
            DataTypes.BOOLEAN_TYPE
        }

        override def toString = s"${classExpression.getOrElse("")} is $traitName"
    }

    def isTrait(name: String) = new isTraitLeafExpression(name)

    case class isTraitUnaryExpression(traitName: String, child: Expression)
        extends Expression with UnaryNode {
        // validate TraitName
        typSystem.getDataType(classOf[TraitType], traitName)
        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved child")
            }
            if (!child.dataType.isInstanceOf[ClassType]) {
                throw new ExpressionException(this,
                    s"Cannot apply isTrait on ${child.dataType.getName}, it is not a ClassType")
            }
            DataTypes.BOOLEAN_TYPE
        }

        override def toString = s"$child is $traitName"
    }

    case class hasFieldLeafExpression(fieldName: String, classExpression: Option[Expression] = None)
        extends Expression with LeafNode {

        override lazy val resolved = classExpression.isDefined
        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"Cannot apply hasField on ${classExpression.get.dataType.getName}, it is not a ClassType")
            }
            if (classExpression.isDefined && !TypeUtils.fieldMapping(classExpression.get.dataType).isDefined) {
                throw new ExpressionException(this, s"Cannot apply hasField on ${classExpression.get.dataType.getName}")
            }
            DataTypes.BOOLEAN_TYPE
        }

        override def toString = s"${classExpression.getOrElse("")} has $fieldName"
    }

    def hasField(name: String) = new hasFieldLeafExpression(name)

    case class hasFieldUnaryExpression(fieldName: String, child: Expression)
        extends Expression with UnaryNode {
        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved child")
            }
            if (!TypeUtils.fieldMapping(child.dataType).isDefined) {
                throw new AtlasException(s"Cannot apply hasField on ${child.dataType.getName}")
            }
            DataTypes.BOOLEAN_TYPE
        }

        override def toString = s"$child has $fieldName"
    }

    case class ComparisonExpression(symbol: String,
                                    left: Expression,
                                    right: Expression)
        extends BinaryExpression {

        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved children")
            }

            if(left.dataType.getName.startsWith(DataTypes.ARRAY_TYPE_PREFIX)) {
                left.dataType;
            } else if(left.dataType == DataTypes.DATE_TYPE) {
                DataTypes.DATE_TYPE
            }
            else if(left.dataType == DataTypes.BOOLEAN_TYPE) {
                DataTypes.BOOLEAN_TYPE;
            }
            else if (left.dataType != DataTypes.STRING_TYPE || right.dataType != DataTypes.STRING_TYPE) {
                TypeUtils.combinedType(left.dataType, right.dataType)
            }
            DataTypes.BOOLEAN_TYPE
        }
    }

    case class LogicalExpression(symbol: String, children: List[Expression])
        extends Expression {
        assert(children.size > 0)
        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved children")
            }
            children.foreach { childExpr =>
                if (childExpr.dataType != DataTypes.BOOLEAN_TYPE) {
                    throw new AtlasException(
                        s"Cannot apply logical operator '$symbol' on input of type '${childExpr.dataType}")
                }
            }
            DataTypes.BOOLEAN_TYPE
        }

        override def toString = children.mkString("", s" $symbol ", "")
    }

    case class FilterExpression(val child: Expression, val condExpr: Expression) extends Expression {
        val children = List(child, condExpr)
        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved children")
            }
            if (condExpr.dataType != DataTypes.BOOLEAN_TYPE) {
                throw new ExpressionException(this, s"Filter condition '$condExpr' is not a boolean expression")
            }
            child.dataType
        }

        override def namedExpressions = child.namedExpressions ++ condExpr.namedExpressions

        override def toString = s"$child where $condExpr"
    }

    case class SelectExpression(child: Expression, selectList: List[Expression]) extends Expression {
        val children = List(child) ::: selectList
        lazy val selectListWithAlias = selectList.zipWithIndex map {
            case (s: AliasExpression, _) => s
            case (x, i) => new AliasExpression(x, s"${x}")
        }

        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved children")
            }
            TypeUtils.createStructType(selectListWithAlias)
        }

        override def namedExpressions = child.namedExpressions ++ (selectList.flatMap(_.namedExpressions))

        override def toString = s"""$child select ${selectListWithAlias.mkString("", ", ", "")}"""
    }

    case class LoopExpression(val input: Expression, val loopingExpression: Expression,
                              val times: Option[Literal[Integer]]) extends Expression {
        val children = List(input, loopingExpression)
        lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved children")
            }
            if (input.dataType.getTypeCategory != TypeCategory.CLASS) {
                throw new ExpressionException(this, s"Loop Expression applied to type : '${input.dataType.getName}';" +
                    " loop can only be applied to Class Expressions")
            }
            if (input.dataType != loopingExpression.dataType) {
                throw new ExpressionException(this,
                    s"Invalid Loop Expression; input and loopExpression dataTypes don't match: " +
                        s"(${input.dataType.getName},${loopingExpression.dataType.getName}})")
            }
            input.dataType
        }

        override def namedExpressions = input.namedExpressions

        override def toString = {
            if (times.isDefined) s"$input loop ($loopingExpression) times ${times.get.value}"
            else s"$input loop ($loopingExpression)"
        }
    }

    case class TraitInstanceExpression(child: Expression)
      extends Expression with UnaryNode {
      lazy val dataType = {
        if (!resolved) {
          throw new UnresolvedException(this,
            s"datatype. Can not resolve due to unresolved child")
        }
        if (!child.dataType.isInstanceOf[TraitType]) {
          throw new ExpressionException(this,
            s"Cannot apply instance on ${child.dataType.getName}, it is not a TraitType")
        }
        typSystem.getIdType.getStructType
      }

      override def toString = s"$child traitInstance"
    }

  case class InstanceExpression(child: Expression)
    extends Expression with UnaryNode {
    lazy val dataType = {
      if (!resolved) {
        throw new UnresolvedException(this,
          s"datatype. Can not resolve due to unresolved child")
      }
      typSystem.getIdType.getStructType
    }

    override def toString = s"$child instance"
  }

  case class PathExpression(child: Expression)
    extends Expression with UnaryNode {
    lazy val dataType = {
      if (!resolved) {
        throw new UnresolvedException(this,
          s"datatype. Can not resolve due to unresolved child")
      }
      TypeUtils.ResultWithPathStruct.createType(this, child.dataType)
    }

    override def toString = s"$child withPath"
  }

  case class LimitExpression(child: Expression, limit: Literal[Integer], offset: Literal[Integer]) extends Expression with UnaryNode {

    override def toString = s"$child limit $limit offset $offset "

    lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved children")
            }
            child.dataType
    }
  }

  case class OrderExpression(child: Expression, odr: Expression, asc: Boolean) extends Expression with UnaryNode {

    override def toString = s"$child orderby $odr asc $asc"

    lazy val dataType = {
            if (!resolved) {
                throw new UnresolvedException(this,
                    s"datatype. Can not resolve due to unresolved children")
            }
            child.dataType
    }
  }
}
