// Show vertex_index field status summary and list all graph indexes.
// READ-ONLY — management transaction is rolled back.

import org.janusgraph.core.JanusGraphFactory

graph = JanusGraphFactory.open('/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties')
mgmt = graph.openManagement()

println "=== vertex_index Field Status Summary ==="
println ""

idx = mgmt.getGraphIndex('vertex_index')
fields = idx.getFieldKeys()

statusMap = [:]
fields.each { pk ->
    s = idx.getIndexStatus(pk).toString()
    statusMap[s] = (statusMap[s] ?: 0) + 1
}

statusMap.sort().each { status, count ->
    println "  ${status}: ${count}"
}
println ""
println "Total fields in vertex_index: ${fields.size()}"

println ""
println "=== All Graph Indexes ==="
mgmt.getGraphIndexes(org.apache.tinkerpop.gremlin.structure.Vertex.class).each { gidx ->
    println "  ${gidx.name()} (${gidx.isMixedIndex() ? 'mixed' : 'composite'}, ${gidx.getFieldKeys().size()} fields)"
}

mgmt.rollback()
graph.close()
