==================
Titan 1.0.0 README
==================

IMPORTANT: Titan 1 support in Atlas is currently a work in progress.


ARCHITECTURE NOTES
------------------

The build is currently set up to include only one of the graph backends in the Atlas war file. The configured graph
backend is determined by maven profile. The default profile is titan0. To build Atlas configured to run against Titan 1,
the titan1 profile must be enabled. Titan1 project has support for Gremlin3.  Gremlin Translator translates the DSL to
gremlin3 compliant syntax when titan1 is configured as backend graph db.


REQUIREMENTS
--------------

Titan 1 requires Java 8 to be used both when building and running Atlas. While building on Java7,  the java classes in
the Titan 1 project are not compiled. But an empty jar is produced. If java 8 is used, then titan 1 classes are compiled
but tests are only executed if titan1 profile is explicitly enabled.



USING ATLAS WITH TITAN 1
------------------------

1) Build Atlas with the titan1 maven profile enabled:

mvn install -P dist -P titan1

Note that there are some tests in repository and webapp project which are skipped when running with the titan1 profile.
Please refer to known issues section below.

This will build Atlas and run all of the tests against Titan 1.  Only Atlas builds that were generated with the titan1
maven profile enabled can be used to use Atlas with Titan 1.

2) Configure the Atlas runtime to use Titan 1 by setting the atlas.graphdb.backend property in
ATLAS_HOME/conf/atlas-application.properties, as follows:

atlas.graphdb.backend=org.apache.atlas.repository.graphdb.titan1.Titan1GraphDatabase

3) Attempt to start the Atlas server.  NOTE: As of this writing, Atlas fails to start (see issue 2 below).


KNOWN ISSUES
------------

1) EntityLineageService is hard-coded to generate Gremlin that is specific to Tinker Pop 2.  It needs to be updated to
use GremlinExpressionFactory to generate Gremlin that is appropriate for the version of Titan being used. Currently
these tests are skipped when the titan1 profile is enabled.

https://issues.apache.org/jira/browse/ATLAS-1579

2) Catalog project is hard-coded to generate Gremlin that is specific to Tinker Pop 2.  It needs to be updated to use
GremlinExpressionFactory to generate Gremlin that is appropriate for the version of Titan being used.  In addition, it
has direct dependencies on titan 0 / Tinker Pop 2 classes.

https://issues.apache.org/jira/browse/ATLAS-1580

3) The Atlas war file startup is currently failing when Titan1 is being used.  Due to the titan 0 dependencies in
catalog, the catalog jar is currently being excluded from webapp when titan1 is being used.  However, Atlas appears to
have runtime dependencies on the stuff in Catalog.  The war startup currently fails with the following exception:

Caused by: java.lang.ClassNotFoundException: org.apache.atlas.catalog.exception.CatalogException
        at org.codehaus.plexus.classworlds.strategy.SelfFirstStrategy.loadClass(SelfFirstStrategy.java:50)
        at org.codehaus.plexus.classworlds.realm.ClassRealm.unsynchronizedLoadClass(ClassRealm.java:271)

https://issues.apache.org/jira/browse/ATLAS-1581

4) There are some known test failures in webapp project. Those need to be addressed. There is work needed to refactor
webapp so that it can function without catalog until issue #2 above is resolved.

https://issues.apache.org/jira/browse/ATLAS-1582

5) We cannot bundle both titan0 and titan1 in the Atlas war file because titan1 and titan0 require different versions of
the same libraries.  We cannot have multiple versions of the same library on the classpath.  Another issue there is that
Gremin queries are executed via the java services mechanism.  Specifically, the titan 1 implemention bundles a file
called javax.script.ScriptingEngineFactory, which tells Java to
use org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory.  We cannot have this file on the
classpath when using titan 0 since it would cause the Titan 0 implemention to try to execute queries using TP 3.  There
may be ways of working around this, such as making Titan1Graph and Titan0Graph explicitly instantiate the
GremlinGroovyScriptEngineFactory.  This was not investigated very much, though.  If we combine that with shading
Titan0/1 and all of their dependencies, we might be able to bundle both the Titan 1 and Titan 0 implementations in the
Atlas war file and have things work correctly for both versions.  Another possibility would be to use a custom
classloader to load the correct atlas-graphdb-titan0/1 jar during Atlas startup based on the configuration.
