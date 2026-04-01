  ---                                                                                                                                                                                                                                                                 
1. IndexSearch Flow

                    POST /api/atlas/v2/entity/indexsearch                                                                                                                                                                                                           
                                    │                                                                                                                                                                                                                               
                          DiscoveryREST.indexSearch()
                                    │
                    EntityDiscoveryService.directIndexSearch()
                                    │
                         graph.elasticsearchQuery(indexName)
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
              AtlasJanusGraph                  CassandraGraph
           returns AtlasElasticsearch-      returns CassandraIndexQuery
           Query (JanusGraph ES client)     (direct ES REST client)
                    │                               │
                    └───────────────┬───────────────┘
                                    │
                       indexQuery.vertices(searchParams)
                          ↓ (both talk to ES directly)
                    ┌─────────────────────────────┐
                    │      Elasticsearch          │
                    │   (same index, same data)   │
                    └─────────────────────────────┘
                                    │
                         Returns vertex IDs + hits
                                    │
               EntityGraphRetriever.prepareSearchResult()
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
   getVertexProperties-      getConnectedRelation-     preloadProperties()
   ValueMap()                Edges()                         │
   │                         │                  ┌─────┴──────┐
   ┌──────┴──────┐           ┌──────┴──────┐           │            │
   │  Cassandra  │           │  Cassandra  │        Cassandra    JanusGraph
   │instanceof?──┤           │instanceof?──┤       instanceof?  VertexProperty
   │  YES    NO  │           │  YES    NO  │       YES │         iterator +
   │   │      │  │           │   │      │  │     AtlasAPI│     CacheVertexProperty
   └───┼──────┼──┘           └───┼──────┼──┘           │
   │      │                  │      │              │
   AtlasAPI  Gremlin         AtlasAPI  Gremlin     AtlasAPI
   bulk      graph.V()       bulk      V().bothE() getPropertyKeys()
   getVertices()             getEdgesFor             │
   │      │              Vertices()     getAllClassifications_V1()
   └──┬───┘                  │               │
   │                      │          ┌────┴────┐
   │                      │          │Cassandra │
   │                      │          │ guard:   │
   │                      │          │!(instOf) │
   │                      │          │ YES  NO  │
   │                      │          │  │    │  │
   │                      │          │ std  JG  │
   │                      │          │ path optim│
   │                      │          └─────────┘
   └──────────┬───────────┘
   │
   AtlasSearchResult

Assessment: Clean. The fork happens at two levels:
1. Interface polymorphism — graph.elasticsearchQuery() returns different impl classes (no instanceof needed)
2. instanceof guards — In EntityGraphRetriever for vertex/edge batch loading (4 guard points)

Both paths converge on the same ES cluster. The JanusGraph path uses Gremlin for post-ES vertex loading; Cassandra uses bulk Atlas API. Foolproof: Yes — the guard is always the first thing checked in each method, and the return exits early.

  ---
2. Bulk Entity Create Flow

2A: WITHOUT Classifications

                POST /api/atlas/v2/entity/bulk
                            │
                      EntityREST.createOrUpdate()
                            │
                    AtlasEntityStoreV2.createOrUpdate()
                            │
                ┌───────────┴───────────────┐
                │  preCreateOrUpdate()       │
                │  → EntityGraphMapper       │
                │    .createVertexWithGuid() │
                └───────────┬───────────────┘
                            │
                entityGraphMapper.mapAttributesAndClassifications()
                            │
                ┌───────────┴───────────┐
                │ mapAttributes()       │
                │ mapRelationshipAttrs()│
                │ setCustomAttributes() │
                │                       │
                │ classifications=null  │
                │ → SKIP classification │
                │   handling entirely   │
                └───────────┬───────────┘
                            │
                    @GraphTransaction commit
                            │
                 ┌──────────┴──────────┐
                 │                     │
           AtlasJanusGraph        CassandraGraph
           .commit()              .commit()
                 │                     │
           tx().commit()          TransactionBuffer:
           (JanusGraph             1. batchInsertVertices
            handles it)            2. batchInsertEdges
                                   3. buildIndexEntries
                                   4. syncTypeDefsToCache
                                   5. syncVerticesToES
                                   6. markPersisted()

No forking at entity creation level — both backends use the same graph.addVertex() / graph.addEdge() interface. The fork is entirely at commit time via interface polymorphism.

2B: WITH Classifications

                POST /api/atlas/v2/entity/bulk  (with classifications)
                            │
                      EntityREST.createOrUpdate()
                            │
                    AtlasEntityStoreV2.createOrUpdate()
                            │
                entityGraphMapper.mapAttributesAndClassifications()
                            │
                handleAddClassifications(guid, classifications)
                            │
                  ┌─────────┴─────────┐
                  │ DynamicConfigStore │
                  │ .isTagV2Enabled()? │
                  └────┬─────────┬────┘
                       │         │
                     YES         NO
                       │         │
            ┌──────────┴──┐  ┌──┴──────────────┐
            │  V2 Path    │  │   V1 Path        │
            │ (Cassandra  │  │ (Graph-based)    │
            │  TagDAO)    │  │                  │
            │             │  │                  │
            │ tagDAO      │  │ createClassifi-  │
            │ .putDirect- │  │ cationVertex()   │
            │  Tag()      │  │       │          │
            │      │      │  │ mapClassifi-     │
            │ Record      │  │ cation() → edge  │
            │ CassandraTag│  │ creation         │
            │ Operation   │  │       │          │
            │      │      │  │ IF propagate:    │
            │ Queue ES    │  │ addTagPropagation│
            │ Deferred Op │  │ (immediate, in   │
            │      │      │  │  transaction)    │
            │ IF propagate│  │                  │
            │ queue async │  └──────────────────┘
            │ task        │
            └─────────────┘

            KEY DIFFERENCE:
            ┌────────────────────┬──────────────────────┐
            │ V2 (TagV2=true)    │ V1 (TagV2=false)     │
            ├────────────────────┼──────────────────────┤
            │ No graph vertex    │ Creates graph vertex  │
            │ for classification │ for classification    │
            │                    │                       │
            │ Native Cassandra   │ Graph edges between   │
            │ tags table         │ entity & class vertex  │
            │                    │                       │
            │ Async propagation  │ Immediate propagation │
            │ via tasks          │ in same transaction   │
            │                    │                       │
            │ Rollback via       │ Rollback via graph    │
            │ CassandraTagOp     │ transaction rollback  │
            └────────────────────┴──────────────────────┘

Assessment: The classification fork is controlled by DynamicConfigStore.isTagV2Enabled(), NOT by instanceof CassandraGraph. This is a feature flag, not a backend switch. This means:
- JanusGraph + TagV2=true → Uses Cassandra TagDAO (hybrid mode)
- JanusGraph + TagV2=false → Uses graph vertices (classic JanusGraph)
- Cassandra + TagV2=true → Uses Cassandra TagDAO (natural fit)
- Cassandra + TagV2=false → Uses graph vertices (CassandraGraph API)

Potential concern: TagDAOCassandraImpl.getInstance() is always initialized (line 237), even for JanusGraph. The V1 path doesn't call TagDAO — this is safe.

  ---
3. Lineage Flow

           GET/POST /api/atlas/v2/lineage/{guid}
                          │
                    LineageREST
                          │
                EntityLineageService
                          │
         ┌────────────────┴────────────────┐
         │  cassandraLineageService != null │
         │  (set at constructor time based  │
         │   on graph instanceof)           │
         └──────┬──────────────────┬────────┘
                │                  │
              YES                  NO
                │                  │
   CassandraLineageService   EntityLineageService
   (direct CQL queries)      (graph-based)
   │                  │
   │           ┌──────┴──────┐
   │           │LINEAGE_USING│
   │           │_GREMLIN?    │
   │           └──┬──────┬───┘
   │            YES      NO
   │              │      │
   ┌───────────┤        Gremlin    Graph API
   │           │        Scripts   getEdges()
   │           │        via       iteration
   │           │        ScriptEngine
   │           │              │      │
   │    ┌──────┴──────┐       └──┬───┘
   │    │ CQL Queries │         │
   │    │             │    AtlasLineageInfo
   │    │ vertex_index│
   │    │ vertices    │
   │    │ edges_in    │
   │    │ edges_out   │
   │    └──────┬──────┘
   │           │
   │    AtlasLineageInfo
   │           │
   └───────────┴──── (same response type)

   SAME PATTERN FOR:
   ├── Classic lineage     (getClassicLineageInfo / getLineageInfoV1/V2)
   ├── On-demand lineage   (getLineageInfoOnDemand)
   └── List lineage        (getLineageListInfo / traverseEdgesUsingBFS)

Assessment: Very clean. The fork is set once at constructor time (EntityLineageService constructor, line 102-118) and never re-evaluated. All 3 lineage APIs (classic, on-demand, list) use the same guard: cassandraLineageService != null. Foolproof: Yes — the
Cassandra path is a completely independent service with its own CQL queries. No shared mutable state.

  ---
4. Delete Flow (Soft & Hard)

           DELETE /api/atlas/v2/entity/guid/{guid}
                          │
                    EntityREST.deleteByGuid()
                          │
              AtlasEntityStoreV2.deleteById()
                          │
                    deleteVertices()
                          │
              ┌───────────┴───────────────────────────┐
              │                                       │
   removeHasLineageOnDelete()              deleteDelegate.getHandler()
   (repair hasLineage flags                       .deleteEntities()
   BEFORE deletion)                                     │
   │                               ┌───────────┴───────────┐
   │                               │                       │
   resetHasLineageOn-                 SoftDeleteHandler        HardDeleteHandler
   InputOutputDelete()                      │                       │
   │                          _deleteVertex():        _deleteVertex():
   hasActiveLineageDirection()        set STATE=DELETED        graphHelper
   │                         set MODIFIED_TS          .removeVertex()
   ┌────────┴────────┐               set MODIFIED_BY                │
   │instanceof       │                     │                  ┌─────┴─────┐
   │CassandraGraph?  │                deleteEdge():           │ JanusGraph│
   └──┬──────────┬───┘            set STATE=DELETED           │ graph     │
   │          │                      │                     │.remove-   │
   YES          NO                     │                     │ Vertex()  │
   │          │                      │                     ├───────────┤
   AtlasAPI    Gremlin              ┌────┴────┐                │ Cassandra │
   getEdges()  g.V().outE()         │ COMMIT  │                │ markDelete│
   iterate     .project()           └────┬────┘                │ → txBuffer│
   manually    .toStream()               │                     │.removeVtx │
   │       .anyMatch()          ┌────┴────┐                └───────────┘
   │          │                 │         │
   └────┬─────┘           JanusGraph  Cassandra
   │                 tx.commit() commit():
   Also used by:                         deleteEdge()
   repairHasLineageForProcess()          deleteVertex()
   checkIfAssetShouldHaveLineage()       removeEdgeIndex()
   │                           syncToES()
   ┌────────┴────────┐
   │instanceof       │
   │CassandraGraph?  │
   └──┬──────────┬───┘
   │          │
   YES          NO
   │          │
   Atlas API   Gremlin
   edge iter   g.V().outE()
    + loop      .inV().has()
      .hasNext()

Assessment: The fork pattern is consistent — always instanceof CassandraGraph as first check with early return. The hasLineage repair has 3 fork points (DeleteHandlerV1:2207, AtlasEntityStoreV2:3225, AtlasEntityStoreV2:3356), all using the same pattern.

Error handling quirk: AtlasEntityStoreV2:1116 catches JanusGraphException specifically. On Cassandra, this never fires — exceptions fall through to catch (Exception e) which is correct but gives slightly different error codes.

  ---
Overall Assessment

Forking Pattern Used

      ┌─────────────────────────────────┐
      │     PATTERN: Early-Return Guard │
      │                                 │
      │  method() {                     │
      │    if (graph instanceof         │
      │        CassandraGraph) {        │
      │      return cassandraPath();    │  ← Cassandra exits early
      │    }                            │
      │                                 │
      │    // JanusGraph code below     │  ← JanusGraph falls through
      │    ((AtlasJanusGraph) graph)    │
      │      .getGraph().traversal()    │
      │      ...                        │
      │  }                              │
      └─────────────────────────────────┘

Scorecard

┌──────────────┬───────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  Criterion   │ Score │                                                          Notes                                                           │
├──────────────┼───────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Consistency  │ 9/10  │ Same instanceof CassandraGraph pattern everywhere except classifications (uses feature flag instead)                     │
├──────────────┼───────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Completeness │ 9/10  │ All hot paths covered. Cold paths (RepairIndex, DSL queries) are JanusGraph-only but acceptable                          │
├──────────────┼───────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Safety       │ 9/10  │ Early-return pattern means JanusGraph code is untouched. Only risk: JanusGraphException catch in AtlasEntityStoreV2      │
├──────────────┼───────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Readability  │ 8/10  │ Pattern is easy to grep for. Each *ViaAtlasApi() helper method is self-contained. Slight verbosity from duplicated logic │
├──────────────┼───────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Testability  │ 8/10  │ Both paths independently testable. Integration tests validated 164/164 passing                                           │
└──────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

One Observation

The classification flow uses a different forking mechanism (DynamicConfigStore.isTagV2Enabled()) than the rest of the codebase (instanceof CassandraGraph). This is intentional — TagV2 is a feature flag that can be enabled even on JanusGraph. But it means there
are two orthogonal axes:
- Backend: JanusGraph vs Cassandra
- Feature: TagV1 vs TagV2

This creates 4 possible combinations, which is worth documenting for operators.