=====================================
Building with a chosen graph database
=====================================

The Atlas build is currently set up to include one of the graph backends in the Atlas war file.
The choice of graph backend is determined by the setting of the GRAPH-PROVIDER system variable.

If GRAPH-PROVIDER is not set, the default graph backend is adopted. This is currently JanusGraph 0.2.0

In order to build with a specific (non-default) graph backend set the GRAPH-PROVDER system variable.

If GRAPH-PROVIDER is set to titan0, the build will contain Titan 0.5.4
If GRAPH-PROVIDER is set to janus, the build will contain JanusGraph 0.2.0 (i.e. the default above)

For example, to build Atlas with the janus graph-provider:
mvn install [-P dist] -DGRAPH-PROVIDER=janus


Titan 0.5.4 supports Gremlin2 only, whereas JanusGraph support Gremlin3 only (and NOT Gremlin2).
Gremlin2 and Gremlin3 are not compatible. The gremlin used by Atlas is translated into either Gremlin2 or
Gremlin3 depending on which graph backend is used in the build. This is implemented in GremlinExpressionFactory.


REQUIREMENTS
------------
JanusGraph 0.2.0 require Java 8 to be used both when building and running Atlas.
Unless Java 8 is used, the janus module will not be built - this is checked by the maven-enforcer-plugin.
