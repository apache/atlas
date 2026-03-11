// List all fields in vertex_index grouped by status.
// READ-ONLY — management transaction is rolled back.

import org.janusgraph.core.JanusGraphFactory

graph = JanusGraphFactory.open('/tmp/gremlin-100-gremlin-1/conf/janusgraph-cql-es.properties')
mgmt = graph.openManagement()

println "=== vertex_index Fields ==="
println ""

idx = mgmt.getGraphIndex('vertex_index')
fields = idx.getFieldKeys().collect { pk ->
    [name: pk.name(), status: idx.getIndexStatus(pk).toString(), dataType: pk.dataType().simpleName]
}.sort { it.name }

['ENABLED', 'REGISTERED', 'INSTALLED', 'DISABLED'].each { status ->
    group = fields.findAll { it.status == status }
    if (group) {
        println "--- ${status} (${group.size()}) ---"
        group.each { f ->
            println "  ${f.name} (${f.dataType})"
        }
        println ""
    }
}

println "Total: ${fields.size()}"

mgmt.rollback()
graph.close()
