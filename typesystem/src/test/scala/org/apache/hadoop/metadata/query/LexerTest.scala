package org.apache.hadoop.metadata.query

import org.junit.{Assert, Test}

import scala.util.parsing.input.CharArrayReader

class LexerTest {

  def scan(p : QueryParser, str : String) : p.lexical.ParseResult[_] = {
    val l = p.lexical
    var s: l.Input = new CharArrayReader(str.toCharArray)
    var r = (l.whitespace.? ~ l.token)(s)
    s = r.next

    while (r.successful && !s.atEnd) {
      s = r.next
      if (!s.atEnd) {
        r = (l.whitespace.? ~ l.token)(s)
      }
    }
    r.asInstanceOf[p.lexical.ParseResult[_]]
  }

  @Test def testSimple {
    val p = new QueryParser
    val r = scan(p, """DB where db1.name""")
    Assert.assertTrue(r.successful)

  }
}
