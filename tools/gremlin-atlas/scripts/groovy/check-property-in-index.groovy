// Check if a specific property key exists and is registered in the mixed index.
// READ-ONLY — management transaction is rolled back.
//
// Usage: pass -Dproperty=<name> as a JVM arg.
// Default: connectionQualifiedName

import org.janusgraph.core.JanusGraphFactory

propertyName = System.getProperty('property', 'connectionQualifiedName')

graph = JanusGraphFactory.open('/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties')
mgmt = graph.openManagement()

println "=== Check Property in Mixed Index ==="
println ""
println "Property: ${propertyName}"
println ""

pk = mgmt.getPropertyKey(propertyName)
if (pk == null) {
    println "Property key '${propertyName}' does NOT exist in the schema."
} else {
    println "Property key exists: dataType=${pk.dataType().simpleName}, cardinality=${pk.cardinality()}"

    idx = mgmt.getGraphIndex('vertex_index')
    try {
        status = idx.getIndexStatus(pk)
        println "In vertex_index: YES (status: ${status})"
    } catch (Exception e) {
        println "In vertex_index: NO — ${e.message}"
    }
}

mgmt.rollback()
graph.close()
