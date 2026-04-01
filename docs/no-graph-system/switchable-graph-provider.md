Plan: Switchable Graph Provider Branch

     Context                                                                                                                                                                                                                                                          
                                                                                                                                                                                                                                                                    
     The zerograph branch successfully built a Cassandra+ES graph backend, but it deleted JanusGraph entirely. This means you can't switch between backends — it's Cassandra-only.

     The goal is a new switchable-graph-provider branch (from master) where:
     - JanusGraph stays intact and is the default (zero risk to existing deployments)
     - Cassandra backend is added as a new option
     - A config flag atlas.graphdb.backend=janus|cassandra switches between them at runtime
     - The PR is safe to land: most changes are new files, only ~8 existing files need real review

     Approach: Keep graphdb/api unchanged, add Cassandra alongside JanusGraph

     The key insight: graphdb/api already depends on TinkerPop (for AtlasGraphTraversal, AtlasGraphTraversalSource, AtlasIndexQuery). Rather than restructuring the API layer, we keep it as-is. CassandraGraph gets TinkerPop transitively through graphdb/api —
      it implements the interface methods with stubs for TinkerPop-specific ones. This means zero changes to graphdb/janus or graphdb/common — the JanusGraph path is untouched.

     What does NOT change (vs zerograph branch)

     ┌────────────────────────────────────┬─────────────────────────────────────────────────────────┐
     │             Kept as-is             │                           Why                           │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ graphdb/janus/ module              │ JanusGraph backend stays fully functional               │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ graphdb/common/ module             │ TinkerPop query helpers still used by JanusGraph        │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ graphdb/graphdb-impls/             │ Build-time graph provider selection still works         │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ common/src/.../groovy/ (23 files)  │ Groovy expression framework stays (used by Gremlin DSL) │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ repository/.../query/ (18 files)   │ Gremlin DSL query system stays (works with JanusGraph)  │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ AtlasGraphTraversal.java           │ Stays as abstract class extending DefaultGraphTraversal │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ AtlasGraphTraversalSource.java     │ Stays in graphdb/api                                    │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ GremlinVersion.java                │ Stays in graphdb/api                                    │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ Gremlin methods in AtlasGraph.java │ Stays (executeGremlinScript, etc.)                      │
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────┤
     │ Default backend                    │ Stays as janus                                          │
     └────────────────────────────────────┴─────────────────────────────────────────────────────────┘

     Implementation Steps

     Step 1: Create branch from master

     git checkout master && git checkout -b switchable-graph-provider

     Step 2: Add new modules (purely additive)

     2a. graphdb/cassandra/ — Copy entire module from zerograph branch.
     - ~27 Java source files, ~9,300 lines
     - Implements AtlasGraph, AtlasVertex, AtlasEdge, GraphDatabase interfaces
     - CassandraGraphTraversal extends AtlasGraphTraversal (gets TinkerPop base transitively)
     - CassandraGraph stubs out Gremlin-specific methods (throw UnsupportedOperationException)
     - Direct Cassandra storage (VertexRepository, EdgeRepository, IndexRepository)
     - Direct ES integration (reuses AtlasElasticsearchDatabase pattern)

     2b. graphdb/migrator/ — Copy entire module from zerograph branch.
     - ~13 Java source files + 3 serializer files
     - Standalone fat-jar tool for JanusGraph→Cassandra migration
     - Depends on JanusGraph for reading (isolated dependency)

     Step 3: Extend graphdb/api (backward compatible)

     File: graphdb/api/.../AtlasGraph.java
     - ADD 3 getEdgesForVertices() default methods (~72 lines)
     - These have default implementations that fall back to per-vertex iteration
     - CassandraGraph overrides with async parallel queries
     - JanusGraph gets the default (works correctly, just slower)

     This is the ONLY change to graphdb/api. No other API files change.

     Step 4: Wire up config switching

     File: intg/.../ApplicationProperties.java (~3 lines)
     - Add constant: GRAPHDB_BACKEND_CASSANDRA = "cassandra"
     - Keep default as GRAPHBD_BACKEND_JANUS (safe to land)

     File: repository/.../AtlasRepositoryConfiguration.java (~5 lines)
     - Add CASSANDRA_GRAPH_DATABASE_IMPLEMENTATION_CLASS constant
     - Add cassandra case to getGraphDatabaseImpl() switch

     Step 5: Add Cassandra dependency to repository + webapp

     File: repository/pom.xml
     - Add atlas-graphdb-cassandra as compile-scope dependency (alongside existing atlas-graphdb-janus)

     File: webapp/pom.xml
     - Add atlas-graphdb-cassandra as a dependency (alongside existing atlas-graphdb-impls)

     File: graphdb/pom.xml
     - Add cassandra and migrator to <modules>

     Step 6: Refactor ~6 repository files (the real review)

     These files currently use TinkerPop/Gremlin directly and must be changed to use AtlasVertex/AtlasEdge interface methods so they work with BOTH backends.

     6a. EntityGraphRetriever.java (~350 lines changed)
     - Replace graph.V(batch).project().by() Gremlin traversal with graph.getEdgesForVertices(ids, edgeLabels)
     - Replace graph.traversal().V() calls with graph.getVertices()
     - Remove TinkerPop imports (GraphTraversal, P, __, Vertex, Edge)

     6b. GlossaryService.java (~164 lines changed)
     - Replace 4 Gremlin traversals with getActiveNeighbours() helper using vertex.getEdges(direction, label)
     - Before: graph.V().has(guid).out(label).has(state, ACTIVE).order().by(name, Order.asc).range(offset, limit).toSet()
     - After: iterate edges → filter active → sort in Java → paginate
     - Remove TinkerPop imports (GraphTraversal, Order, P, __)

     6c. DeleteHandlerV1.java (~73 lines changed)
     - Replace GraphTraversal-based lineage direction check with edge iteration loop
     - hasActiveLineageDirection() → iterate vertex.getEdges(direction) and check state

     6d. GraphHelper.java (~46 lines changed)
     - Replace graph.traversal().V().bothE().project() with vertex.getEdges(BOTH)
     - Remove graphdb.janus.* and TinkerPop imports

     6e. EntityLineageService.java (~85 lines changed)
     - Add CassandraLineageService delegation: if (graph instanceof CassandraGraph) → delegate
     - Existing JanusGraph/Gremlin lineage code is untouched (else branch)

     6f. AtlasEntityStoreV2.java (~131 lines changed)
     - Replace GraphTraversal in repairHasLineageForProcess() with edge-based hasActiveEdgeWithActiveVertex()
     - Replace JanusGraphException class checks with generic RuntimeException handling

     Step 7: Add new repository files

     CassandraLineageService.java (NEW, ~1,085 lines)
     - Direct Cassandra CQL queries for lineage (optimized for adjacency list schema)
     - Used only when Cassandra backend is active

     LongEncodingUtil.java (NEW, ~72 lines)
     - Drop-in replacement for JanusGraph's LongEncoding utility
     - Used by Cassandra backend for vertex ID encoding

     Step 8: Build and verify

     # Full compile (both backends)
     JAVA_HOME=... mvn compile -pl webapp -am -DskipTests -Drat.skip=true -DskipEnunciate=true

     # Verify JanusGraph path unchanged (default)
     # Existing tests should pass unmodified

     # Verify Cassandra module compiles
     JAVA_HOME=... mvn compile -pl graphdb/cassandra -am -DskipTests -Drat.skip=true

     # Migrator tests
     JAVA_HOME=... mvn test -pl graphdb/migrator -Drat.skip=true

     ---
     File Change Summary

     Modified existing files (~12 files, only 6 need real review)

     ┌──────────────────────────────────────────────────┬───────────────────────┬───────────────────────────┐
     │                       File                       │         Lines         │       Review effort       │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ graphdb/api/.../AtlasGraph.java                  │ +72 (default methods) │ Low — backward compatible │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ intg/.../ApplicationProperties.java              │ +3                    │ Trivial                   │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ repository/.../AtlasRepositoryConfiguration.java │ +5                    │ Low                       │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ repository/.../EntityGraphRetriever.java         │ ~350                  │ Real review               │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ repository/.../GlossaryService.java              │ ~164                  │ Real review               │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ repository/.../DeleteHandlerV1.java              │ ~73                   │ Real review               │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ repository/.../GraphHelper.java                  │ ~46                   │ Real review               │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ repository/.../EntityLineageService.java         │ ~85                   │ Real review               │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ repository/.../AtlasEntityStoreV2.java           │ ~131                  │ Real review               │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ repository/pom.xml                               │ +5                    │ Trivial                   │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ webapp/pom.xml                                   │ +5                    │ Trivial                   │
     ├──────────────────────────────────────────────────┼───────────────────────┼───────────────────────────┤
     │ graphdb/pom.xml                                  │ +2                    │ Trivial                   │
     └──────────────────────────────────────────────────┴───────────────────────┴───────────────────────────┘

     New files (purely additive, ~12,500 lines)

     ┌──────────────────────────────┬───────┬────────┬───────────────────────────────┐
     │            Module            │ Files │ Lines  │            Nature             │
     ├──────────────────────────────┼───────┼────────┼───────────────────────────────┤
     │ graphdb/cassandra/           │ ~27   │ ~9,300 │ New AtlasGraph implementation │
     ├──────────────────────────────┼───────┼────────┼───────────────────────────────┤
     │ graphdb/migrator/            │ ~22   │ ~4,000 │ Migration tooling             │
     ├──────────────────────────────┼───────┼────────┼───────────────────────────────┤
     │ CassandraLineageService.java │ 1     │ ~1,085 │ Cassandra-optimized lineage   │
     ├──────────────────────────────┼───────┼────────┼───────────────────────────────┤
     │ LongEncodingUtil.java        │ 1     │ ~72    │ Vertex ID encoding utility    │
     └──────────────────────────────┴───────┴────────┴───────────────────────────────┘

     Unchanged modules (zero risk)

     - graphdb/janus/ — zero changes
     - graphdb/common/ — zero changes
     - common/src/.../groovy/ — zero changes
     - repository/.../query/ (Gremlin DSL) — zero changes
     - All test files for existing modules — zero changes

     ---
     Risk Assessment

     - JanusGraph default: Default backend stays janus. Existing deployments see zero behavior change.
     - JanusGraph untouched: graphdb/janus and graphdb/common have zero modifications.
     - 6 file refactors are safe: They replace TinkerPop-specific code with AtlasVertex/AtlasEdge interface calls. JanusGraph implements these interfaces, so the refactored code works with BOTH backends.
     - Gremlin DSL preserved: The Gremlin query system stays intact. It only activates when JanusGraph is the backend.
     - Additive modules: graphdb/cassandra and graphdb/migrator are new modules — no impact on existing code paths.
     - Config-gated: Cassandra backend only activates when explicitly configured (atlas.graphdb.backend=cassandra).

     Source of changes

     All modified file diffs and new module code can be cherry-picked/adapted from the zerograph branch. The key difference: we keep JanusGraph alive and default, whereas zerograph deleted it entirely.