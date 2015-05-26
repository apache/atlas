package org.apache.hadoop.metadata.typesystem.builders

import org.apache.hadoop.metadata.MetadataException
import org.apache.hadoop.metadata.typesystem.types.{Multiplicity, ClassType, TypeSystem}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class MultiplicityTest extends FunSuite with BeforeAndAfterAll {

  override  def beforeAll() = {
    TypeSystem.getInstance().reset()

    val b = new TypesBuilder
    import b._

    val tDef = types {

      _trait("Dimension") {}
      _trait("PII") {}
      _trait("Metric") {}
      _trait("ETL") {}
      _trait("JdbcAccess") {}

      _class("DB") {
        "name" ~ (string, required, indexed, unique)
        "owner" ~ (string)
        "createTime" ~ (int)
      }

      _class("StorageDesc") {
        "inputFormat" ~ (string, required)
        "outputFormat" ~ (string, required)
      }

      _class("Column") {
        "name" ~ (string, required)
        "dataType" ~ (string, required)
        "sd" ~ ("StorageDesc", required)
      }

      _class("Table", List()) {
        "name" ~ (string,  required,  indexed)
        "db" ~ ("DB", required)
        "sd" ~ ("StorageDesc", required)
      }

      _class("LoadProcess") {
        "name" ~ (string, required)
        "inputTables" ~ (array("Table"), collection)
        "outputTable" ~ ("Table", required)

      }

      _class("View") {
        "name" ~ (string, required)
        "inputTables" ~ (array("Table"), collection)
      }

      _class("AT") {
        "name" ~ (string, required)
        "stringSet" ~ (array("string"), multiplicty(0, Int.MaxValue, true))
      }
    }

    TypeSystem.getInstance().defineTypes(tDef)
  }

  test("test1") {

    val b = new InstanceBuilder
    import b._

    val instances = b create {
      val a = instance("AT") {  // use instance to create Referenceables. use closure to
        // set attributes of instance
        'name ~ "A1"                  // use '~' to set attributes. Use a Symbol (names starting with ') for
        'stringSet ~ Seq("a", "a")
      }
    }

    val ts = TypeSystem.getInstance()
    import scala.collection.JavaConversions._
    val typedInstances = instances.map { i =>
      val iTyp = ts.getDataType(classOf[ClassType], i.getTypeName)
      iTyp.convert(i, Multiplicity.REQUIRED)
    }

    typedInstances.foreach { i =>
      println(i)
    }
  }

  test("WrongMultiplicity") {
    val b = new TypesBuilder
    import b._
    val tDef = types {
      _class("Wrong") {
        "name" ~ (string, required)
        "stringSet" ~ (string, multiplicty(0, Int.MaxValue, true))
      }
    }
    val me = intercept[MetadataException] {
      TypeSystem.getInstance().defineTypes(tDef)
    }
    assert("A multiplicty of more than one requires a collection type for attribute 'stringSet'" == me.getMessage)
  }
}
