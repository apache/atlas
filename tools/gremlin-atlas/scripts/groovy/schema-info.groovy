// Show JanusGraph schema: property keys, vertex labels, and graph indexes.
// READ-ONLY — management transaction is rolled back.

import org.janusgraph.core.JanusGraphFactory

graph = JanusGraphFactory.open('/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties')
mgmt = graph.openManagement()

println "=== JanusGraph Schema Info ==="

// Property keys
pks = mgmt.getRelationTypes(org.janusgraph.core.PropertyKey.class).collect { it }
println ""
println "Property Keys: ${pks.size()}"
pks.sort { it.name() }.take(50).each { pk ->
    println "  ${pk.name()} (${pk.dataType().simpleName}, ${pk.cardinality()})"
}
if (pks.size() > 50) println "  ... and ${pks.size() - 50} more"

// Vertex labels
labels = mgmt.getVertexLabels().collect { it }
println ""
println "Vertex Labels: ${labels.size()}"
labels.sort { it.name() }.each { l ->
    println "  ${l.name()}"
}

// Graph indexes
println ""
println "Graph Indexes (Vertex):"
mgmt.getGraphIndexes(org.apache.tinkerpop.gremlin.structure.Vertex.class).each { idx ->
    type = idx.isMixedIndex() ? "mixed (${idx.getBackingIndex()})" : "composite"
    println "  ${idx.name()} — ${type}, ${idx.getFieldKeys().size()} fields"
}

println ""
println "Graph Indexes (Edge):"
mgmt.getGraphIndexes(org.apache.tinkerpop.gremlin.structure.Edge.class).each { idx ->
    type = idx.isMixedIndex() ? "mixed (${idx.getBackingIndex()})" : "composite"
    println "  ${idx.name()} — ${type}, ${idx.getFieldKeys().size()} fields"
}

mgmt.rollback()
graph.close()
