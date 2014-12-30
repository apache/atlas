package org.apache.hadoop.metadata.tools.thrift

import org.junit.Test
import org.junit.Assert

class ThriftParserTest {

  @Test def testSimple {
    var p = new ThriftParser

    var td : Option[ThriftDef] = p("""include "share/fb303/if/fb303.thrift"

                             namespace java org.apache.hadoop.hive.metastore.api
                             namespace php metastore
                             namespace cpp Apache.Hadoop.Hive
                                   """)

    Assert.assertEquals(td.get.toString, """ThriftDef(List(IncludeDef(share/fb303/if/fb303.thrift)),List(),List(NamespaceDef(,cpp,Some(Apache.Hadoop.Hive)), NamespaceDef(,php,Some(metastore)), NamespaceDef(,java,Some(org.apache.hadoop.hive.metastore.api))),List(),List(),List(),List(),List(),List(),List(),List())""")

  }
}
