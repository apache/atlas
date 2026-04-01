Here's what each file does:

┌──────────────────────┬───────────────────────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────┐                                                                        
│         File         │                     Guard Pattern                     │                                           Cassandra Alternative                                           │
├──────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤                                                                        
│ EntityGraphRetriever │ graph instanceof CassandraGraph at 4 locations        │ getVertexPropertiesValueMapViaAtlasApi(), getEdgeInfoMapsViaAtlasApi(), preloadPropertiesViaAtlasApi()    │                                                                        
├──────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤                                                                        
│ GlossaryService      │ CassandraGraph.class.isInstance(graph) at 3 locations │ getActiveNeighbours(), runPaginatedTermsQueryViaAtlasApi(), getGlossaryCategoriesHeadersFullViaAtlasApi() │                                                                        
├──────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ DeleteHandlerV1      │ graph instanceof CassandraGraph at 1 location         │ hasActiveLineageDirectionViaAtlasApi()                                                                    │
├──────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ GraphHelper          │ graph instanceof CassandraGraph at 1 location         │ retrieveEdgeLabelsAndTypeNameViaAtlasApi()                                                                │
├──────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ AtlasEntityStoreV2   │ graph instanceof CassandraGraph at 2 locations        │ hasActiveEdgeWithActiveVertex(), checkIfAssetShouldHaveLineageViaAtlasApi()                               │
└──────────────────────┴───────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────┘

The approach used is more conservative (and safer) than the plan's original "replace TinkerPop code" strategy — instead it preserves the optimized JanusGraph paths while adding Cassandra alternatives via early-return dispatch. This means:
- Zero risk to JanusGraph behavior
- Cassandra code paths use Atlas interface methods that work with the Cassandra backend
- Both JanusGraph jars are always on the classpath (compile deps), so the JanusGraph-specific references don't cause issues

Two minor cosmetic items (non-blocking):
1. AtlasEntityStoreV2:1116 — catch (JanusGraphException) won't fire for Cassandra, but falls through safely to catch (Exception)
2. GraphHelper:2349 — instanceof JanusGraphException check in error handler — cosmetic only

Bottom line: Plan Steps 1-8 are all complete. The switchable graph provider branch is functionally ready for both backends.