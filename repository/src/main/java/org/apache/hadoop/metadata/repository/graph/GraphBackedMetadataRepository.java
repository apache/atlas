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

package org.apache.hadoop.metadata.repository.graph;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.ITypedInstance;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.ITypedStruct;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.storage.MapIds;
import org.apache.hadoop.metadata.storage.RepositoryException;
import org.apache.hadoop.metadata.types.AttributeInfo;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.ObjectGraphWalker;
import org.apache.hadoop.metadata.types.StructType;
import org.apache.hadoop.metadata.types.TraitType;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation backed by a Graph database provided
 * as a Graph Service.
 */
public class GraphBackedMetadataRepository implements MetadataRepository {

    private static final Logger LOG =
            LoggerFactory.getLogger(GraphBackedMetadataRepository.class);

    private final AtomicInteger ID_SEQ = new AtomicInteger(0);

    private final TypedInstanceToGraphMapper instanceToGraphMapper
            = new TypedInstanceToGraphMapper();
    private final GraphToTypedInstanceMapper graphToInstanceMapper
            = new GraphToTypedInstanceMapper();

    private final GraphService graphService;
    private final TypeSystem typeSystem;

    private final TitanGraph titanGraph;

    @Inject
    public GraphBackedMetadataRepository(GraphService graphService) throws MetadataException {
        this.graphService = graphService;
        this.typeSystem = TypeSystem.getInstance();

        this.titanGraph = ((TitanGraphService) graphService).getTitanGraph();
    }

    /**
     * Starts the service. This method blocks until the service has completely started.
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
    }

    /**
     * Stops the service. This method blocks until the service has completely shut down.
     */
    @Override
    public void stop() {
    }

    /**
     * A version of stop() that is designed to be usable in Java7 closure
     * clauses.
     * Implementation classes MUST relay this directly to {@link #stop()}
     *
     * @throws java.io.IOException never
     * @throws RuntimeException    on any failure during the stop operation
     */
    @Override
    public void close() throws IOException {
        stop();
    }

    @Override
    public String createEntity(IReferenceableInstance typedInstance,
                               String typeName) throws RepositoryException {
        LOG.info("adding entity={} type={}", typedInstance, typeName);

        final TransactionalGraph transactionalGraph = graphService.getTransactionalGraph();
        try {
            transactionalGraph.rollback();
            final String guid = instanceToGraphMapper.mapTypedInstanceToGraph(typedInstance);
            transactionalGraph.commit();  // commit if there are no errors
            return guid;

        } catch (MetadataException e) {
            transactionalGraph.rollback();
            throw new RepositoryException(e);
        }
    }

    @Override
    public ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException {
        LOG.info("Retrieving entity with guid={}", guid);

        final TransactionalGraph transactionalGraph = graphService.getTransactionalGraph();
        try {
            transactionalGraph.rollback();  // clean up before starting a query
            Vertex instanceVertex = GraphHelper.findVertexByGUID(transactionalGraph, guid);
            if (instanceVertex == null) {
                LOG.debug("Could not find a vertex for guid {}", guid);
                return null;
            }

            LOG.debug("Found a vertex {} for guid {}", instanceVertex, guid);
            return graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);

        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public List<String> getEntityList(String entityType) throws RepositoryException {
        LOG.info("Retrieving entity list for type={}", entityType);

        GraphQuery query = graphService.getBlueprintsGraph().query()
                .has(Constants.ENTITY_TYPE_PROPERTY_KEY, entityType);
        Iterator<Vertex> results = query.vertices().iterator();
        if (!results.hasNext()) {
            return Collections.emptyList();
        }

        ArrayList<String> entityList = new ArrayList<>();
        while (results.hasNext()) {
            Vertex vertex = results.next();
            entityList.add(vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY));
        }

        return entityList;
    }
    
    private static void searchWalker (Vertex vtx, final int max, int counter, HashMap<String,Map<String,String>> e, HashMap<String,Map<String,String>> v, String edgesToFollow) {
    	
    	counter++;
    	if (counter <= max) {
    		
    		Map<String,String> jsonVertexMap = new HashMap<String,String>();
    		Iterator<Edge> edgeIterator = null;
    		
    		// If we're doing a lineage traversal, only follow the edges specified by the query.  Otherwise
    		// return them all.
    		if (edgesToFollow != null) {
    			IteratorChain ic = new IteratorChain();
    			
    			for (String iterateOn: edgesToFollow.split(",")){
    				ic.addIterator(vtx.query().labels(iterateOn).edges().iterator());
    			}
    			
    			edgeIterator = ic;
    			
    		} else {
    			edgeIterator = vtx.query().edges().iterator();
    		}
    		
   			//Iterator<Edge> edgeIterator = vtx.query().labels("Fathered").edges().iterator();
   			jsonVertexMap.put("HasRelationships", ((Boolean)edgeIterator.hasNext()).toString());
   			
   			for (String pKey: vtx.getPropertyKeys()) {
   				jsonVertexMap.put(pKey, vtx.getProperty(pKey).toString());
   			}
   			
   			// Add to the Vertex map.
   			v.put(vtx.getId().toString(), jsonVertexMap);
   			
   			// Follow this Vertex's edges
   			while (edgeIterator != null && edgeIterator.hasNext()) {
   					   				
   				Edge edge = edgeIterator.next();
   				String label = edge.getLabel();
   				
   				Map<String,String> jsonEdgeMap = new HashMap<String,String>();
   				String tail = edge.getVertex(Direction.OUT).getId().toString();
   				String head = edge.getVertex(Direction.IN).getId().toString();
   				
   				jsonEdgeMap.put("tail", tail);
   	   			jsonEdgeMap.put("head", head);
   	   			jsonEdgeMap.put("label", label);
   	   			
   	   			Direction d = null;
   	   			
   	   			if (tail.equals(vtx.getId().toString())) {
   	   				d = Direction.IN;
   	   			} else {
   	   				d = Direction.OUT;
   	   			}
   	   			
   	   			/* If we want an Edge's property keys, uncomment here.  Or we can parameterize it.
   	   			 * Code is here now for reference/memory-jogging.
   				for (String pKey: edge.getPropertyKeys()) {
   	   				jsonEdgeMap.put(pKey, edge.getProperty(pKey).toString());
   	   			}
   	   			*/
   	   			
  	   			e.put(edge.getId().toString(), jsonEdgeMap);   			
   	   			searchWalker (edge.getVertex(d), max, counter, e, v, edgesToFollow);
   				
   			}
   			
   			
    	} 
    	
    }
    
    
    /*
     * Simple direct graph search and depth traversal.
     * @param searchText is plain text
     * @param prop is the Vertex property to search.
     */
    @Override
    public Map<String,HashMap<String,Map<String,String>>> textSearch(String searchText, int depth, String prop) {
 
    	
    	HashMap<String,HashMap<String,Map<String,String>>> result = new HashMap<String,HashMap<String,Map<String,String>>>();  
    	
    	// HashMaps, which contain sub JOSN Objects to be relayed back to the parent. 
        HashMap<String,Map<String,String>> vertices = new HashMap<String,Map<String,String>>();
        HashMap<String,Map<String,String>> edges = new HashMap<String,Map<String,String>>();
        
        /* Later - when we allow search limitation by "type".
        ArrayList<String> typesList = new ArrayList<String>();
        for (String s: types.split(",")) {
        	
        	// Types validity check.
        	if (typesList.contains(s)) {
        		LOG.error("Specifyed type is not a member of the Type System= {}", s);
        		throw new WebApplicationException(
                        Servlets.getErrorResponse("Invalid type specified in query.", Response.Status.INTERNAL_SERVER_ERROR));
        	}
        	typesList.add(s);
        }*/
        
        int resultCount = 0;
       	for (Vertex v: graphService.getBlueprintsGraph().query().has(prop,searchText).vertices()) {
       		
       		searchWalker(v, depth, 0, edges, vertices, null);
       		resultCount++;
       			
      	}
       	
       	LOG.debug("Search for {} returned {} results.", searchText ,resultCount);   
       	
       	result.put("vertices", vertices);
       	result.put("edges",edges);
       	
       	return result;
    }
    
    /*
     * Simple graph walker for search interface, which allows following of specific edges only.
     * @param edgesToFollow is a comma-separated-list of edges to follow.
     */
    @Override
    public Map<String,HashMap<String,Map<String,String>>> relationshipWalk(String guid, int depth, String edgesToFollow) {
	
    	HashMap<String,HashMap<String,Map<String,String>>> result = new HashMap<String,HashMap<String,Map<String,String>>>();
    	
        // HashMaps, which contain sub JOSN Objects to be relayed back to the parent. 
        HashMap<String,Map<String,String>> vertices = new HashMap<String,Map<String,String>>();
        HashMap<String,Map<String,String>> edges = new HashMap<String,Map<String,String>>();
        
        // Get the Vertex with the specified GUID.
        Vertex v = GraphHelper.findVertexByGUID(graphService.getBlueprintsGraph(), guid);
        
        if (v != null) {
        	searchWalker(v, depth, 0, edges, vertices, edgesToFollow);
        	LOG.debug("Vertex {} found for guid {}", v, guid);
        } else {
        	LOG.debug("Vertex not found for guid {}", guid);
        }
        
       	result.put("vertices", vertices);
       	result.put("edges",edges);
       	
       	return result;
        
    }

    /**
     * Assumes the User is familiar with the persistence structure of the Repository.
     * The given query is run uninterpreted against the underlying Graph Store.
     * The results are returned as a List of Rows. each row is a Map of Key,Value pairs.
     *
     * @param gremlinQuery query in gremlin dsl format
     * @return List of Maps
     * @throws org.apache.hadoop.metadata.MetadataException
     */
    @Override
    public List<Map<String, String>> searchByGremlin(String gremlinQuery) throws MetadataException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("gremlin-groovy");
        Bindings bindings = engine.createBindings();
        bindings.put("g", graphService.getTransactionalGraph());

        try {
            Object o = engine.eval(gremlinQuery, bindings);
            if ( !(o instanceof  List)) {
                throw new RepositoryException(String.format("Cannot process gremlin result %s", o.toString()));
            }

            List l = (List) o;
            List<Map<String,String>> result = new ArrayList<>();
            for(Object r : l) {

                Map<String,String> oRow = new HashMap<>();
                if ( r instanceof  Map ) {
                    @SuppressWarnings("unchecked")
                    Map<Object,Object> iRow = (Map) r;
                    for(Map.Entry e : iRow.entrySet()) {
                        Object k = e.getKey();
                        Object v = e.getValue();
                        oRow.put(k.toString(), v.toString());
                    }
                } else if ( r instanceof TitanVertex) {
                    Iterable<TitanProperty> ps = ((TitanVertex)r).getProperties();
                    for(TitanProperty tP : ps) {
                        String pName = tP.getPropertyKey().getName();
                        Object pValue = ((TitanVertex)r).getProperty(pName);
                        if ( pValue != null ) {
                            oRow.put(pName, pValue.toString());
                        }
                    }

                }  else if ( r instanceof String ) {
                    oRow.put("", r.toString());
                } else {
                    throw new RepositoryException(String.format("Cannot process gremlin result %s", o.toString()));
                }

                result.add(oRow);
            }
            return result;

        }catch(ScriptException se) {
            throw new RepositoryException(se);
        }
    }

    private final class EntityProcessor implements ObjectGraphWalker.NodeProcessor {

        public final Map<Id, Id> idToNewIdMap;
        public final Map<Id, IReferenceableInstance> idToInstanceMap;
        public final Map<Id, Vertex> idToVertexMap;

        public EntityProcessor() {
            idToNewIdMap = new HashMap<>();
            idToInstanceMap = new HashMap<>();
            idToVertexMap = new HashMap<>();
        }

        @Override
        public void processNode(ObjectGraphWalker.Node nd) throws MetadataException {
            IReferenceableInstance ref = null;
            Id id = null;

            if (nd.attributeName == null) {
                ref = (IReferenceableInstance) nd.instance;
                id = ref.getId();
            } else if (nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                if (nd.value != null && (nd.value instanceof Id)) {
                    id = (Id) nd.value;
                }
            }

            if (id != null) {
                if (id.isUnassigned()) {
                    if (!idToNewIdMap.containsKey(id)) {
                        idToNewIdMap.put(id, new Id(ID_SEQ.getAndIncrement(), 0, id.className));
                    }

                    if (ref != null) {
                        if (idToInstanceMap.containsKey(id)) { // Oops
                            throw new RepositoryException(String.format(
                                    "Unexpected internal error: Id %s processed again", id));
                        }

                        idToInstanceMap.put(id, ref);
                    }
                }
            }
        }

        public void createVerticesForClassTypes(List<ITypedReferenceableInstance> newInstances) {
            for (ITypedReferenceableInstance typedInstance : newInstances) {
                final Id id = typedInstance.getId();
                if (!idToVertexMap.containsKey(id)) {
                    Vertex instanceVertex;
                    if (id.isAssigned()) {  // has a GUID
                        instanceVertex = GraphHelper.findVertexByGUID(titanGraph, id.id);
                    } else {
                        instanceVertex = GraphHelper.createVertex(titanGraph, typedInstance);
                    }

                    idToVertexMap.put(id, instanceVertex);
                }
            }
        }
    }

    private final class TypedInstanceToGraphMapper {

        private String mapTypedInstanceToGraph(IReferenceableInstance typedInstance)
            throws MetadataException {

            EntityProcessor entityProcessor = new EntityProcessor();
            try {
                LOG.debug("Walking the object graph for instance {}", typedInstance.getTypeName());
                new ObjectGraphWalker(typeSystem, entityProcessor, typedInstance).walk();
            } catch (MetadataException me) {
                throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
            }

            List<ITypedReferenceableInstance> newTypedInstances = discoverInstances(entityProcessor);
            entityProcessor.createVerticesForClassTypes(newTypedInstances);
            return addDiscoveredInstances(typedInstance, entityProcessor, newTypedInstances);
        }

        /*
         * Step 2: Traverse oldIdToInstance map create newInstances :
         * List[ITypedReferenceableInstance]
         *  - create a ITypedReferenceableInstance.
         *   replace any old References ( ids or object references) with new Ids.
         */
        private List<ITypedReferenceableInstance> discoverInstances(EntityProcessor entityProcessor)
                throws RepositoryException {
            List<ITypedReferenceableInstance> newTypedInstances = new ArrayList<>();
            for (IReferenceableInstance transientInstance : entityProcessor.idToInstanceMap.values()) {
                LOG.debug("Discovered instance {}", transientInstance.getTypeName());
                try {
                    ClassType cT = typeSystem.getDataType(
                            ClassType.class, transientInstance.getTypeName());
                    ITypedReferenceableInstance newInstance = cT.convert(
                            transientInstance, Multiplicity.REQUIRED);
                    newTypedInstances.add(newInstance);

                /*
                 * Now replace old references with new Ids
                 */
                    MapIds mapIds = new MapIds(entityProcessor.idToNewIdMap);
                    new ObjectGraphWalker(typeSystem, mapIds, newTypedInstances).walk();

                } catch (MetadataException me) {
                    throw new RepositoryException(
                            String.format("Failed to create Instance(id = %s",
                                    transientInstance.getId()), me);
                }
            }

            return newTypedInstances;
        }

        private String addDiscoveredInstances(IReferenceableInstance entity,
                                              EntityProcessor entityProcessor,
                                              List<ITypedReferenceableInstance> newTypedInstances)
            throws MetadataException {
            String typedInstanceGUID = null;
            for (ITypedReferenceableInstance typedInstance : newTypedInstances) { // Traverse over newInstances
                LOG.debug("Adding typed instance {}", typedInstance.getTypeName());

                Id id = typedInstance.getId();
                if (id == null) { // oops
                    throw new RepositoryException("id cannot be null");
                }

                Vertex instanceVertex = entityProcessor.idToVertexMap.get(id);

                // add the attributes for the instance
                final Map<String, AttributeInfo> fields = typedInstance.fieldMapping().fields;

                mapInstanceToVertex(
                        id, typedInstance, instanceVertex, fields, entityProcessor.idToVertexMap);

                for (String traitName : typedInstance.getTraits()) {
                    LOG.debug("mapping trait {}", traitName);
                    ((TitanVertex) instanceVertex).addProperty("traits", traitName);
                    ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

                    // add the attributes for the trait instance
                    mapTraitInstanceToVertex(traitName, traitInstance, typedInstance,
                            instanceVertex, entityProcessor.idToVertexMap);
                }

                if (typedInstance.getId() == entity.getId()) { // save the guid for return
                    typedInstanceGUID = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                }
            }

            return typedInstanceGUID;
        }

        private void mapInstanceToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
                                         Map<String, AttributeInfo> fields,
                                         Map<Id, Vertex> idToVertexMap) throws MetadataException {
            LOG.debug("Mapping instance {} to vertex {} for fields {}",
                    typedInstance.getTypeName(), instanceVertex, fields);
            for (AttributeInfo attributeInfo : fields.values()) {
                final IDataType dataType = attributeInfo.dataType();
                mapAttributesToVertex(id, typedInstance, instanceVertex,
                        idToVertexMap, attributeInfo, dataType);
            }
        }

        private void mapAttributesToVertex(Id id, ITypedInstance typedInstance,
                                           Vertex instanceVertex,
                                           Map<Id, Vertex> idToVertexMap,
                                           AttributeInfo attributeInfo,
                                           IDataType dataType) throws MetadataException {
            LOG.debug("mapping attributeInfo {}", attributeInfo);
            final String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;

            switch (dataType.getTypeCategory()) {
                case PRIMITIVE:
                    mapPrimitiveToVertex(typedInstance, instanceVertex, attributeInfo);
                    break;

                case ENUM:
                    instanceVertex.setProperty(propertyName,
                            typedInstance.getInt(attributeInfo.name));
                    break;

                case ARRAY:
                    mapArrayCollectionToVertex(
                            id, typedInstance, instanceVertex, attributeInfo, idToVertexMap);
                    break;

                case MAP:
                    mapMapCollectionToVertex(
                            id, typedInstance, instanceVertex, attributeInfo, idToVertexMap);
                    break;

                case STRUCT:
                    Vertex structInstanceVertex = mapStructInstanceToVertex(id,
                            (ITypedStruct) typedInstance.get(attributeInfo.name),
                            attributeInfo, idToVertexMap);
                    // add an edge to the newly created vertex from the parent
                    GraphHelper.addEdge(instanceVertex, structInstanceVertex, propertyName);
                    break;

                case TRAIT:
                    // do NOTHING - this is taken care of earlier
                    break;

                case CLASS:
                    Id referenceId = (Id) typedInstance.get(attributeInfo.name);
                    mapClassReferenceAsEdge(
                        instanceVertex, idToVertexMap, propertyName, referenceId
                    );
                    break;

                default:
                    break;
            }
        }

        private void mapArrayCollectionToVertex(Id id, ITypedInstance typedInstance,
                                                Vertex instanceVertex,
                                                AttributeInfo attributeInfo,
                                                Map<Id, Vertex> idToVertexMap) throws MetadataException {
            LOG.debug("Mapping instance {} to vertex {} for name {}",
                    typedInstance.getTypeName(), instanceVertex, attributeInfo.name);
            List list = (List) typedInstance.get(attributeInfo.name);
            if (list == null || list.isEmpty()) {
                return;
            }

            String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            // todo: move this to the indexer
            GraphHelper.createPropertyKey(titanGraph.getManagementSystem(), propertyName);

            IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();

            StringBuilder buffer = new StringBuilder();
            Object[] array = list.toArray();
            for (int index = 0; index < array.length; index++) {
                String propertyNameWithSuffix = propertyName + "." + index;
                buffer.append(propertyNameWithSuffix).append(",");
                mapCollectionEntryToVertex(id, instanceVertex, attributeInfo,
                        idToVertexMap, elementType, array[index], propertyNameWithSuffix);
            }

            buffer.setLength(buffer.length() - 1);
            // for dereference on way out
            instanceVertex.setProperty(propertyName, buffer.toString());
        }

        private void mapMapCollectionToVertex(Id id, ITypedInstance typedInstance,
                                              Vertex instanceVertex,
                                              AttributeInfo attributeInfo,
                                              Map<Id, Vertex> idToVertexMap) throws MetadataException {
            LOG.debug("Mapping instance {} to vertex {} for name {}",
                    typedInstance.getTypeName(), instanceVertex, attributeInfo.name);
            @SuppressWarnings("unchecked")
            Map<Object, Object> collection = (Map<Object, Object>) typedInstance.get(attributeInfo.name);
            if (collection == null || collection.isEmpty()) {
                return;
            }

            String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            // todo: move this to the indexer
            GraphHelper.createPropertyKey(titanGraph.getManagementSystem(), propertyName);

            StringBuilder buffer = new StringBuilder();
            IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();
            for (Map.Entry entry : collection.entrySet()) {
                String propertyNameWithSuffix = propertyName + "." + entry.getKey();
                buffer.append(propertyNameWithSuffix).append(",");
                mapCollectionEntryToVertex(id, instanceVertex, attributeInfo,
                        idToVertexMap, elementType, entry.getValue(), propertyNameWithSuffix);
            }

            buffer.setLength(buffer.length() - 1);
            // for dereference on way out
            instanceVertex.setProperty(propertyName, buffer.toString());
        }

        private void mapCollectionEntryToVertex(Id id, Vertex instanceVertex,
                                                AttributeInfo attributeInfo,
                                                Map<Id, Vertex> idToVertexMap,
                                                IDataType elementType, Object value,
                                                String propertyName) throws MetadataException {
            switch (elementType.getTypeCategory()) {
                case PRIMITIVE:
                    instanceVertex.setProperty(propertyName, value);
                    break;

                case ENUM:
                    instanceVertex.setProperty(propertyName, value);
                    break;

                case ARRAY:
                case MAP:
                case TRAIT:
                    // do nothing
                    break;

                case STRUCT:
                    Vertex structInstanceVertex = mapStructInstanceToVertex(id,
                            (ITypedStruct) value, attributeInfo, idToVertexMap);
                    // add an edge to the newly created vertex from the parent
                    GraphHelper.addEdge(instanceVertex, structInstanceVertex,
                            propertyName);
                    break;

                case CLASS:
                    Id referenceId = (Id) value;
                    mapClassReferenceAsEdge(
                            instanceVertex, idToVertexMap,
                            propertyName, referenceId);
                    break;

                default:
                    break;
            }
        }

        private void mapClassReferenceAsEdge(Vertex instanceVertex,
                                             Map<Id, Vertex> idToVertexMap,
                                             String propertyKey, Id id) throws MetadataException {

            if (id != null) {
                Vertex referenceVertex;
                if (id.isAssigned()) {
                    referenceVertex = GraphHelper.findVertexByGUID(titanGraph, id.id);
                } else {
                    referenceVertex = idToVertexMap.get(id);
/*
                    ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
                    mapInstanceToVertex(id, typedInstance, referenceVertex,
                            classType.fieldMapping().fields, idToVertexMap);
*/
                }

                if (referenceVertex != null) {
                    // add an edge to the class vertex from the instance
                    GraphHelper.addEdge(instanceVertex, referenceVertex, propertyKey);
                } else { // Oops - todo - throw an exception?
                    System.out.println("BOOOOO = " + id);
                }
            }
        }

        private Vertex mapStructInstanceToVertex(Id id, ITypedStruct structInstance,
                                                 AttributeInfo attributeInfo,
                                                 Map<Id, Vertex> idToVertexMap) throws MetadataException {
            // add a new vertex for the struct or trait instance
            Vertex structInstanceVertex = GraphHelper.createVertex(
                    graphService.getBlueprintsGraph(), structInstance.getTypeName(), id);
            LOG.debug("created vertex {} for struct {}", structInstanceVertex, attributeInfo.name);

            // map all the attributes to this newly created vertex
            mapInstanceToVertex(id, structInstance, structInstanceVertex,
                    structInstance.fieldMapping().fields, idToVertexMap);

            return structInstanceVertex;
        }

        private void mapTraitInstanceToVertex(String traitName, ITypedStruct traitInstance,
                                              ITypedReferenceableInstance typedInstance,
                                              Vertex parentInstanceVertex,
                                              Map<Id, Vertex> idToVertexMap) throws MetadataException {
            // add a new vertex for the struct or trait instance
            Vertex traitInstanceVertex = GraphHelper.createVertex(
                    graphService.getBlueprintsGraph(), traitInstance, typedInstance.getId());
            LOG.debug("created vertex {} for trait {}", traitInstanceVertex, traitName);

            // map all the attributes to this newly created vertex
            mapInstanceToVertex(typedInstance.getId(), traitInstance, traitInstanceVertex,
                    traitInstance.fieldMapping().fields, idToVertexMap);

            // add an edge to the newly created vertex from the parent
            String relationshipLabel = typedInstance.getTypeName() + "." + traitName;
            GraphHelper.addEdge(parentInstanceVertex, traitInstanceVertex, relationshipLabel);
        }

        private void mapPrimitiveToVertex(ITypedInstance typedInstance,
                                          Vertex instanceVertex,
                                          AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("Adding primitive {} to v {}", attributeInfo, instanceVertex);
            if (typedInstance.get(attributeInfo.name) == null) { // add only if instance has this attribute
                return;
            }

            final String vertexPropertyName = typedInstance.getTypeName() + "." + attributeInfo.name;

            if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getString(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getShort(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getInt(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getBigInt(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getBoolean(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getByte(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getLong(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getFloat(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getDouble(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getBigDecimal(attributeInfo.name));
            }
        }
    }

    private final class GraphToTypedInstanceMapper {

        private ITypedReferenceableInstance mapGraphToTypedInstance(String guid,
                                                                    Vertex instanceVertex)
            throws MetadataException {
            LOG.debug("Mapping graph root vertex {} to typed instance for guid {}",
                    instanceVertex, guid);
            String typeName = instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
            List<String> traits = new ArrayList<>();
            for (TitanProperty property : ((TitanVertex) instanceVertex).getProperties("traits")) {
                traits.add((String) property.getValue());
            }

            Id id = new Id(guid, instanceVertex.<Integer>getProperty("version"), typeName);
            LOG.debug("Created id {} for instance type {}", id, typeName);

            ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
            ITypedReferenceableInstance typedInstance = classType.createInstance(
                    id, traits.toArray(new String[traits.size()]));

            mapVertexToInstance(instanceVertex, typedInstance, classType.fieldMapping().fields);
            mapVertexToInstanceTraits(instanceVertex, typedInstance, traits);

            return typedInstance;
        }

        private void mapVertexToInstanceTraits(Vertex instanceVertex,
                                               ITypedReferenceableInstance typedInstance,
                                               List<String> traits) throws MetadataException {
            for (String traitName : traits) {
                LOG.debug("mapping trait {} to instance", traitName);
                TraitType traitType = typeSystem.getDataType(TraitType.class, traitName);
                mapVertexToTraitInstance(
                        instanceVertex, typedInstance, traitName, traitType);
            }
        }

        private void mapVertexToInstance(Vertex instanceVertex, ITypedInstance typedInstance,
                                         Map<String, AttributeInfo> fields) throws MetadataException {
            LOG.debug("Mapping vertex {} to instance {} for fields",
                    instanceVertex, typedInstance.getTypeName(), fields);
            for (AttributeInfo attributeInfo : fields.values()) {
                LOG.debug("mapping attributeInfo = " + attributeInfo);
                final IDataType dataType = attributeInfo.dataType();
                final String vertexPropertyName =
                        typedInstance.getTypeName() + "." + attributeInfo.name;

                switch (dataType.getTypeCategory()) {
                    case PRIMITIVE:
                        mapVertexToInstance(instanceVertex, typedInstance, attributeInfo);
                        break;  // add only if vertex has this attribute

                    case ENUM:
                        typedInstance.setInt(attributeInfo.name,
                                instanceVertex.<Integer>getProperty(vertexPropertyName));
                        break;

                    case ARRAY:
                        mapVertexToArrayInstance(instanceVertex, typedInstance, attributeInfo);
                        break;

                    case MAP:
                        mapVertexToMapInstance(instanceVertex, typedInstance, attributeInfo);
                        break;

                    case STRUCT:
                        mapVertexToStructInstance(instanceVertex, typedInstance, attributeInfo);
                        break;

                    case TRAIT:
                        // do NOTHING - handled in class
                        break;

                    case CLASS:
                        String relationshipLabel = typedInstance.getTypeName() + "." + attributeInfo.name;
                        Object idOrInstance = mapClassReferenceToVertex(instanceVertex,
                                attributeInfo, relationshipLabel, attributeInfo.dataType());
                        typedInstance.set(attributeInfo.name, idOrInstance);
                        break;

                    default:
                        break;
                }
            }
        }

        private Object mapClassReferenceToVertex(Vertex instanceVertex,
                                                 AttributeInfo attributeInfo,
                                                 String relationshipLabel,
                                                 IDataType dataType) throws MetadataException {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            Iterator<Edge> results = instanceVertex.getEdges(
                    Direction.OUT, relationshipLabel).iterator();
            if (results.hasNext()) {
                final Vertex referenceVertex = results.next().getVertex(Direction.IN);
                if (referenceVertex != null) {
                    final String guid = referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                    LOG.debug("Found vertex {} for label {} with guid {}",
                            referenceVertex, relationshipLabel, guid);
                    if (attributeInfo.isComposite) {
                        LOG.debug("Found composite, mapping vertex to instance");
                        return mapGraphToTypedInstance(guid, referenceVertex);
                    } else {
                        Id referenceId = new Id(guid,
                                referenceVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                                dataType.getName());
                        LOG.debug("Found non-composite, adding id {} ", referenceId);
                        return referenceId;
                    }
                }
            }

            return null;
        }

        @SuppressWarnings("unchecked")
        private void mapVertexToArrayInstance(Vertex instanceVertex, ITypedInstance typedInstance,
                                              AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
            String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            String keys = instanceVertex.getProperty(propertyName);
            if (keys == null || keys.length() == 0) {
                return;
            }
            DataTypes.ArrayType arrayType = (DataTypes.ArrayType) attributeInfo.dataType();
            final IDataType elementType = arrayType.getElemType();

            ArrayList values = new ArrayList();
            for (String propertyNameWithSuffix : keys.split(",")) {
                values.add(mapVertexToCollectionEntry(
                        instanceVertex, attributeInfo, elementType, propertyNameWithSuffix));
            }

            typedInstance.set(attributeInfo.name, values);
        }

        private Object mapVertexToCollectionEntry(Vertex instanceVertex,
                                                  AttributeInfo attributeInfo,
                                                  IDataType elementType,
                                                  String propertyNameWithSuffix) throws MetadataException {
            switch (elementType.getTypeCategory()) {
                case PRIMITIVE:
                    return instanceVertex.getProperty(propertyNameWithSuffix);

                case ENUM:
                    return instanceVertex.<Integer>getProperty(propertyNameWithSuffix);

                case ARRAY:
                case MAP:
                case TRAIT:
                    // do nothing
                    break;

                case STRUCT:
                    return getStructInstanceFromVertex(instanceVertex,
                            elementType, attributeInfo.name, propertyNameWithSuffix);

                case CLASS:
                    return mapClassReferenceToVertex(
                            instanceVertex, attributeInfo, propertyNameWithSuffix, elementType);

                default:
                    break;
            }

            throw new IllegalArgumentException();
        }

        @SuppressWarnings("unchecked")
        private void mapVertexToMapInstance(Vertex instanceVertex, ITypedInstance typedInstance,
                                            AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
            String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            String keys = instanceVertex.getProperty(propertyName);
            if (keys == null || keys.length() == 0) {
                return;
            }
            DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
            final IDataType elementType = mapType.getValueType();

            HashMap values = new HashMap();
            for (String propertyNameWithSuffix : keys.split(",")) {
                final String key = propertyNameWithSuffix.substring(
                        propertyNameWithSuffix.lastIndexOf("."), propertyNameWithSuffix.length());
                values.put(key, mapVertexToCollectionEntry(
                        instanceVertex, attributeInfo, elementType, propertyNameWithSuffix));
/*
                switch (valueType.getTypeCategory()) {
                    case PRIMITIVE:
                        values.put(key, instanceVertex.getProperty(propertyNameWithSuffix));
                        break;

                    case ENUM:
                        values.put(key, instanceVertex.getProperty(propertyNameWithSuffix));
                        break;

                    case ARRAY:
                    case MAP:
                    case TRAIT:
                        // do nothing
                        break;

                    case STRUCT:
                        ITypedStruct structInstance = getStructInstanceFromVertex(instanceVertex,
                                valueType, attributeInfo.name, propertyNameWithSuffix);
                        values.put(key, structInstance);
                        break;

                    case CLASS:
                        Object idOrInstance = mapClassReferenceToVertex(
                                instanceVertex, attributeInfo, propertyNameWithSuffix);
                        values.put(key, idOrInstance);
                        break;

                    default:
                        break;
                }
*/
            }

            typedInstance.set(attributeInfo.name, values);
        }

        private ITypedStruct getStructInstanceFromVertex(Vertex instanceVertex,
                                                         IDataType elemType,
                                                         String attributeName,
                                                         String relationshipLabel) throws MetadataException {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            Iterator<Edge> results = instanceVertex.getEdges(
                    Direction.OUT, relationshipLabel).iterator();
            Edge edge = results.hasNext() ? results.next() : null;
            if (edge == null) {
                return null;
            }

            Vertex structInstanceVertex = edge.getVertex(Direction.IN);
            LOG.debug("mapping vertex {} to struct {}", structInstanceVertex, attributeName);

            if (structInstanceVertex != null) {
                LOG.debug("Found struct instance vertex {}, mapping to instance {} ",
                        structInstanceVertex, elemType.getName());
                StructType structType = typeSystem.getDataType(StructType.class, elemType.getName());
                ITypedStruct structInstance = structType.createInstance();

                mapVertexToInstance(structInstanceVertex, structInstance,
                        structType.fieldMapping().fields);
                return structInstance;
            }

            return null;
        }

        private void mapVertexToStructInstance(Vertex instanceVertex,
                                               ITypedInstance typedInstance,
                                               AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("mapping vertex {} to struct {}", instanceVertex, attributeInfo.name);
            StructType structType = typeSystem.getDataType(
                    StructType.class, attributeInfo.dataType().getName());
            ITypedStruct structInstance = structType.createInstance();
            typedInstance.set(attributeInfo.name, structInstance);

            String relationshipLabel = typedInstance.getTypeName() + "." + attributeInfo.name;
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            for (Edge edge : instanceVertex.getEdges(Direction.OUT, relationshipLabel)) {
                final Vertex structInstanceVertex = edge.getVertex(Direction.IN);
                if (structInstanceVertex != null) {
                    LOG.debug("Found struct instance vertex {}, mapping to instance {} ",
                            structInstanceVertex, structInstance.getTypeName());
                    mapVertexToInstance(structInstanceVertex, structInstance,
                            structType.fieldMapping().fields);
                    break;
                }
            }
        }

        private void mapVertexToTraitInstance(Vertex instanceVertex,
                                              ITypedReferenceableInstance typedInstance,
                                              String traitName,
                                              TraitType traitType) throws MetadataException {
            ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

            String relationshipLabel = typedInstance.getTypeName() + "." + traitName;
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            for (Edge edge : instanceVertex.getEdges(Direction.OUT, relationshipLabel)) {
                final Vertex traitInstanceVertex = edge.getVertex(Direction.IN);
                if (traitInstanceVertex != null) {
                    LOG.debug("Found trait instance vertex {}, mapping to instance {} ",
                            traitInstanceVertex, traitInstance.getTypeName());
                    mapVertexToInstance(traitInstanceVertex, traitInstance,
                            traitType.fieldMapping().fields);
                    break;
                }
            }
        }

        private void mapVertexToInstance(Vertex instanceVertex,
                                         ITypedInstance typedInstance,
                                         AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("Adding primitive {} from vertex {}", attributeInfo, instanceVertex);
            final String vertexPropertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            if (instanceVertex.getProperty(vertexPropertyName) == null) {
                return;
            }

            if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
                typedInstance.setString(attributeInfo.name,
                        instanceVertex.<String>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
                typedInstance.setShort(attributeInfo.name,
                        instanceVertex.<Short>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
                typedInstance.setInt(attributeInfo.name,
                        instanceVertex.<Integer>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
                typedInstance.setBigInt(attributeInfo.name,
                        instanceVertex.<BigInteger>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
                typedInstance.setBoolean(attributeInfo.name,
                        instanceVertex.<Boolean>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
                typedInstance.setByte(attributeInfo.name,
                        instanceVertex.<Byte>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
                typedInstance.setLong(attributeInfo.name,
                        instanceVertex.<Long>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
                typedInstance.setFloat(attributeInfo.name,
                        instanceVertex.<Float>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
                typedInstance.setDouble(attributeInfo.name,
                        instanceVertex.<Double>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
                typedInstance.setBigDecimal(attributeInfo.name,
                        instanceVertex.<BigDecimal>getProperty(vertexPropertyName));
            }
        }
    }
}
