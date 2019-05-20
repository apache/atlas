/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

class GroovyIndexRepair {
    final long COMMIT_BATCH_SIZE    = 200
    final int  MAX_TRIES_ON_FAILURE = 3
    final int  NUM_THREADS          = Runtime.getRuntime().availableProcessors()

    Graph g
    def   indexSerializer
    long  totalVertexCount

    def setup(String configPath) {
        display("Initializing graph..")

        g                = TitanFactory.open(configPath)
        indexSerializer  = g.getIndexSerializer() as com.thinkaurelius.titan.graphdb.database.IndexSerializer
        totalVertexCount = g.V.count()

        display("Graph initialized!")
    }

    def processIndex(String indexName) {
        processBatch(indexName, 0, totalVertexCount, NUM_THREADS)
    }

    def processIndex(List<String> indexNames, String guid) {
        for (String indexName : indexNames) {
            restore(indexName, guid)
        }
    }

    def processBatch(String indexName, long startIndex, long endIndex, long threadCount) {
        Date startTime = new Date()
        long batchSize = (endIndex - startIndex) / threadCount

        if ((batchSize * threadCount) < endIndex) {
            batchSize++
        }

        display("Processing index: " + indexName)
        display("    Start Index : " + startIndex)
        display("    End Index   : " + endIndex)
        display("    Chunk Count : " + threadCount)
        display("    Batch Size  : " + batchSize)

        def threads = []
        for (long batchStartIdx = startIndex; batchStartIdx < endIndex; batchStartIdx += batchSize) {
            final String lIdxName  = indexName
            final long   lStartIdx = batchStartIdx;
            final long   lEndIdx   = (lStartIdx + batchSize) > endIndex ? endIndex : (lStartIdx + batchSize);

            threads << new Thread({
                restore(lIdxName, lStartIdx, lEndIdx)
            })
        }

        threads.each { it.start() }
        threads.each { it.join() }

        Date endTime = new Date()

        display("Restore complete: " + indexName + ". Time taken: " + (endTime.time - startTime.time) + "ms")
    }

    void restore(String indexName, String guid) {
        Date   startTime = new Date()

        def mgmt              = g.getManagementSystem()
        def documentsPerStore = new java.util.HashMap()
        def tx                = mgmt.getWrappedTx()
        def mutator           = tx.getTxHandle()
        def tIdx              = mgmt.getGraphIndex(indexName)
        def indexType         = mgmt.getSchemaVertex(tIdx).asIndexType()
        def mixedIndexType    = indexType as com.thinkaurelius.titan.graphdb.types.indextype.MixedIndexTypeWrapper
        def vertex            = g.V().has("__guid", guid).next();
        if (vertex == null) {
            display(" Could not find: " + guid);
            return;
        }

        for (int attemptCount = 1; attemptCount <= MAX_TRIES_ON_FAILURE; attemptCount++) {
            try {
                indexSerializer.reindexElement(vertex, mixedIndexType, documentsPerStore);
                break;
            } catch (all) {
                display("Exception: " + all)
                display("Pausing before retry..")
                Thread.sleep(2000 * attemptCount)
            }
        }

        display(" commit")
        mutator.getIndexTransaction(mixedIndexType.getBackingIndexName()).restore(documentsPerStore)

        def endTime = new Date()
        display(" thread end. Time taken: " + (endTime.time - startTime.time) + "ms")
    }

    void restore(String indexName, long startIndex, long endIndex) {
        Date   startTime = new Date()
        String batchId   = "Batch Id: [" + startIndex + "-" + endIndex + "]"

        display(batchId + " thread start")

        def mgmt              = g.getManagementSystem()
        def documentsPerStore = new java.util.HashMap()
        def tx                = mgmt.getWrappedTx()
        def mutator           = tx.getTxHandle()
        def tIdx              = mgmt.getGraphIndex(indexName)
        def indexType         = mgmt.getSchemaVertex(tIdx).asIndexType()
        def mixedIndexType    = indexType as com.thinkaurelius.titan.graphdb.types.indextype.MixedIndexTypeWrapper

        for (long batchStartIdx = startIndex; batchStartIdx < endIndex; batchStartIdx += COMMIT_BATCH_SIZE) {
            long   batchEndIdx = (batchStartIdx + COMMIT_BATCH_SIZE) > endIndex ? endIndex : (batchStartIdx + COMMIT_BATCH_SIZE)
            String displayStr  = batchId + ": " + "{" + batchStartIdx + "-" + batchEndIdx + "}"

            for (def vertex : g.V[batchStartIdx..batchEndIdx]) {
                for (int attemptCount = 1; attemptCount <= MAX_TRIES_ON_FAILURE; attemptCount++) {
                    try {
                        indexSerializer.reindexElement(vertex, mixedIndexType, documentsPerStore);
                        break;
                    } catch (all) {
                        display("Exception: " + all)
                        display("Pausing before retry..")
                        Thread.sleep(2000 * attemptCount)
                    }
                }
            }

            display(displayStr + " commit")

            mutator.getIndexTransaction(mixedIndexType.getBackingIndexName()).restore(documentsPerStore)
        }
        def endTime = new Date()

        display(batchId + " thread end. Time taken: " + (endTime.time - startTime.time) + "ms")
    }

    def close() {
        g.shutdown()
        display("Graph shutdown!")
    }

    def display(String s) {
        println(getTimestamp() + " " + s)
    }

    def getTimestamp() {
        new Date().format("yyyy-MM-dd HH:mm:ss.SSS")
    }
}

def repairAtlasIndex(String configPath) {
    r = new GroovyIndexRepair()

    r.setup(configPath)
    r.processIndex("vertex_index")
    r.processIndex("fulltext_index")
    r.processIndex("edge_index")

    r.close()
}

def repairAtlasEntity(String configPath, String guid) {
    r = new GroovyIndexRepair()

    r.setup(configPath)
    r.processIndex(["vertex_index", "fulltext_index", "edge_index"], guid)
    r.close()
}
