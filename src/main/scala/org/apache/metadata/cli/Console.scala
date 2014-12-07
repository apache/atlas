package org.apache.metadata.cli

import org.apache.metadata.MetadataService
import org.apache.metadata.MetadataService
import org.apache.metadata.storage.memory.MemRepository
import org.apache.metadata.types.TypeSystem

import scala.reflect.io.File
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{IMain, ILoop}

object Console extends App {
  val settings = new Settings
  settings.usejavacp.value = true
  settings.deprecation.value = true
  settings.bootclasspath.value += """/Users/hbutani/.m2/repository/org/apache/metadata/1.0-SNAPSHOT/metadata-1.0-SNAPSHOT.jar:/Users/hbutani/.m2/repository/org/scala-lang/scala-compiler/2.10.4/scala-compiler-2.10.4.jar:/Users/hbutani/.m2/repository/org/scala-lang/scala-reflect/2.10.4/scala-reflect-2.10.4.jar:/Users/hbutani/.m2/repository/org/scala-lang/jline/2.10.4/jline-2.10.4.jar:/Users/hbutani/.m2/repository/org/fusesource/jansi/jansi/1.4/jansi-1.4.jar:/Users/hbutani/.m2/repository/org/scala-lang/scala-library/2.10.4/scala-library-2.10.4.jar:/Users/hbutani/.m2/repository/org/scala-lang/scala-actors/2.10.4/scala-actors-2.10.4.jar:/Users/hbutani/.m2/repository/org/scala-lang/scalap/2.10.4/scalap-2.10.4.jar:/Users/hbutani/.m2/repository/org/scalatest/scalatest_2.10/2.2.0/scalatest_2.10-2.2.0.jar:/Users/hbutani/.m2/repository/org/scalamacros/quasiquotes_2.10/2.0.1/quasiquotes_2.10-2.0.1.jar:/Users/hbutani/.m2/repository/org/json4s/json4s-native_2.10/3.2.11/json4s-native_2.10-3.2.11.jar:/Users/hbutani/.m2/repository/org/json4s/json4s-core_2.10/3.2.11/json4s-core_2.10-3.2.11.jar:/Users/hbutani/.m2/repository/org/json4s/json4s-ast_2.10/3.2.11/json4s-ast_2.10-3.2.11.jar:/Users/hbutani/.m2/repository/com/thoughtworks/paranamer/paranamer/2.6/paranamer-2.6.jar:/Users/hbutani/.m2/repository/com/github/nscala-time/nscala-time_2.10/1.6.0/nscala-time_2.10-1.6.0.jar:/Users/hbutani/.m2/repository/joda-time/joda-time/2.5/joda-time-2.5.jar:/Users/hbutani/.m2/repository/org/joda/joda-convert/1.2/joda-convert-1.2.jar:/Users/hbutani/.m2/repository/com/typesafe/config/1.2.1/config-1.2.1.jar:/Users/hbutani/.m2/repository/com/typesafe/akka/akka-actor_2.10/2.3.7/akka-actor_2.10-2.3.7.jar:/Users/hbutani/.m2/repository/com/typesafe/akka/akka-testkit_2.10/2.3.7/akka-testkit_2.10-2.3.7.jar:/Users/hbutani/.m2/repository/com/typesafe/akka/akka-slf4j_2.10/2.3.7/akka-slf4j_2.10-2.3.7.jar:/Users/hbutani/.m2/repository/org/slf4j/slf4j-api/1.7.5/slf4j-api-1.7.5.jar:/Users/hbutani/.m2/repository/io/spray/spray-routing/1.3.1/spray-routing-1.3.1.jar:/Users/hbutani/.m2/repository/io/spray/spray-http/1.3.1/spray-http-1.3.1.jar:/Users/hbutani/.m2/repository/org/parboiled/parboiled-scala_2.10/1.1.6/parboiled-scala_2.10-1.1.6.jar:/Users/hbutani/.m2/repository/org/parboiled/parboiled-core/1.1.6/parboiled-core-1.1.6.jar:/Users/hbutani/.m2/repository/io/spray/spray-util/1.3.1/spray-util-1.3.1.jar:/Users/hbutani/.m2/repository/com/chuusai/shapeless_2.10/1.2.4/shapeless_2.10-1.2.4.jar:/Users/hbutani/.m2/repository/io/spray/spray-can/1.3.1/spray-can-1.3.1.jar:/Users/hbutani/.m2/repository/io/spray/spray-io/1.3.1/spray-io-1.3.1.jar:/Users/hbutani/.m2/repository/io/spray/spray-httpx/1.3.1/spray-httpx-1.3.1.jar:/Users/hbutani/.m2/repository/org/jvnet/mimepull/mimepull/1.9.4/mimepull-1.9.4.jar:/Users/hbutani/.m2/repository/io/spray/spray-testkit/1.3.1/spray-testkit-1.3.1.jar:/Users/hbutani/.m2/repository/com/google/guava/guava/11.0.2/guava-11.0.2.jar:/Users/hbutani/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:/Users/hbutani/.m2/repository/junit/junit/4.10/junit-4.10.jar:/Users/hbutani/.m2/repository/org/hamcrest/hamcrest-core/1.1/hamcrest-core-1.1.jar"""

  val in = new IMain(settings){
    override protected def parentClassLoader = settings.getClass.getClassLoader()
  }

  new SampleILoop().process(settings)
}

class SampleILoop extends ILoop {
  override def prompt = "==> "

  //intp = Console.in

  val ts: TypeSystem = new TypeSystem
  val mr: MemRepository = new MemRepository
  val ms : MetadataService = new MetadataService(mr, ts)
  MetadataService.setCurrentService(ms)

  addThunk {
    intp.beQuietDuring {
      intp.addImports("java.lang.Math._")
      intp.addImports("org.json4s.native.Serialization.{read, write => swrite}")
      intp.addImports("org.json4s._")
      intp.addImports("org.json4s.native.JsonMethods._")
      intp.addImports("org.apache.metadata.dsl._")
      intp.bindValue("service", ms)
      //intp.bindValue("cp", intp.compilerClasspath)
    }
  }

  override def printWelcome() {
    echo("\n" +
      "         \\,,,/\n" +
      "         (o o)\n" +
      "-----oOOo-(_)-oOOo-----")
  }

}
