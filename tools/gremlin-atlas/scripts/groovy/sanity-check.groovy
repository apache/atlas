// Sanity check: verify graph connectivity and show sample vertices.
// READ-ONLY — no data is modified.

import org.janusgraph.core.JanusGraphFactory

graph = JanusGraphFactory.open('/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties')
g = graph.traversal()

println "=== JanusGraph Sanity Check ==="
println ""

count = g.V().limit(10).count().next()
println "First 10 vertices accessible: ${count} found"

if (count > 0) {
    println ""
    println "Sample vertices:"
    g.V().limit(5).valueMap('__typeName', 'qualifiedName', '__guid').each { props ->
        println "  type=${props['__typeName']}, qn=${props['qualifiedName']}, guid=${props['__guid']}"
    }
}

println ""
println "Graph open: ${graph.isOpen()}"

graph.close()
println "Done."
