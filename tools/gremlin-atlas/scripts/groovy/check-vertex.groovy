// Look up a vertex by GUID and print all its properties.
// READ-ONLY — no data is modified.
//
// Usage: pass -Dguid=<GUID> as a JVM arg, or edit the default below.

import org.janusgraph.core.JanusGraphFactory

guid = System.getProperty('guid', 'REPLACE_WITH_GUID')

if (guid == 'REPLACE_WITH_GUID') {
    println "ERROR: Provide a GUID via -Dguid=<GUID>"
    println "Or edit this script and replace REPLACE_WITH_GUID"
    System.exit(1)
}

graph = JanusGraphFactory.open('/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties')
g = graph.traversal()

println "=== Vertex Lookup: ${guid} ==="
println ""

results = g.V().has('__guid', guid).valueMap().toList()

if (results.isEmpty()) {
    println "No vertex found with __guid = ${guid}"
} else {
    results[0].sort().each { k, v ->
        println "  ${k}: ${v}"
    }
}

graph.close()
