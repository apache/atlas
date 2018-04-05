/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.omrs.metadatacollection.properties.instances;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * InstanceGraph stores a subgraph of entities and relationships and provides methods to access its content.
 * It stores a list of entities and a list of relationships.  It is possible to request a list for each
 * of these two lists, or request elements that link to a specific element.  For example, request the relationships
 * that link to an entity or the entity at a specific end of a relationship.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class InstanceGraph extends InstanceElementHeader
{
    private ArrayList<EntityDetail>   entityElementList = null;
    private ArrayList<Relationship>   relationshipElementList = null;


    /**
     * Default constructor
     */
    public InstanceGraph()
    {
    }

    /**
     * Typical Constructor creates a graph with the supplied list of elements.  It assumes the caller has supplied
     * elements that do link together.  However, this graph supports graph fragments.
     *
     * @param entityElementList - list of entity elements to add to the list
     * @param relationshipElementList - list of relationship elements to add to the list
     */
    public InstanceGraph(List<EntityDetail> entityElementList,
                         List<Relationship> relationshipElementList)
    {
        if (entityElementList == null)
        {
            this.entityElementList = null;
        }
        else
        {
            this.entityElementList = new ArrayList<>(entityElementList);
        }

        if (relationshipElementList == null)
        {
            this.relationshipElementList = null;
        }
        else
        {
            this.relationshipElementList = new ArrayList<>(relationshipElementList);
        }
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateGraph - graph to copy; null to create an empty graph
     */
    public InstanceGraph(InstanceGraph templateGraph)
    {
        if (templateGraph != null)
        {
            setEntities(templateGraph.getEntities());
            setRelationships(templateGraph.getRelationships());
        }
    }


    /**
     * Return the list of all of the entities (vertices/nodes) in the instance graph.  Null means empty graph.
     *
     * @return EntityDetails - entity list
     */
    public List<EntityDetail> getEntities()
    {
        if (entityElementList == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(entityElementList);
        }
    }


    /**
     * Set up the list of entities for this instance graph.
     *
     * @param entityElementList - list of entities
     */
    public void setEntities(List<EntityDetail> entityElementList)
    {
        if (entityElementList == null)
        {
            this.entityElementList = null;
        }
        else
        {
            this.entityElementList = new ArrayList<>(entityElementList);
        }
    }



    /**
     * Return the list of all relationships (edges/links) in the instance graph. Null means a disconnected/empty graph.
     *
     * @return Relationships - relationship list
     */
    public List<Relationship> getRelationships()
    {
        if (relationshipElementList == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(relationshipElementList);
        }
    }


    /**
     * Set up the list of relationships in this instance graph.
     *
     * @param relationshipElementList - list of relationships
     */
    public void setRelationships(List<Relationship> relationshipElementList)
    {
        if (relationshipElementList == null)
        {
            this.relationshipElementList = null;
        }
        else
        {
            this.relationshipElementList = new ArrayList<>(relationshipElementList);
        }
    }


    /**
     * Return a list of relationships that are connected to a specific entity.
     *
     * @param anchorEntityGUID - unique identifier for an entity
     * @return Relationships - relationship iterator
     */
    public List<Relationship> getRelationshipsForEntity(String  anchorEntityGUID)
    {
        ArrayList<Relationship> matchingRelationships = new ArrayList<>();

        /*
         * Load copies of each relationship that matches the requested entity into matchingRelationships.
         */
        if (relationshipElementList != null)
        {
            for (Relationship  relationship : relationshipElementList)
            {
                if (relationship.relatedToEntity(anchorEntityGUID))
                {
                    matchingRelationships.add(new Relationship(relationship));
                }
            }
        }

        /*
         * Return any matched relationships in an iterator for the caller to step through.
         */
        if (matchingRelationships.isEmpty())
        {
            return null;
        }
        else
        {
            return matchingRelationships;
        }
    }


    /**
     * Return the entity connected at the far end of an entity's relationship.
     *
     * @param anchorEntityGUID - unique id for the known entity.
     * @param linkingRelationshipGUID - the relationship to traverse.
     * @return EntityDetail - the requested entity at the far end of the relationship.
     * Null if the relationship or entity is not found.
     */
    public EntityDetail getLinkedEntity(String  anchorEntityGUID, String linkingRelationshipGUID)
    {
        Relationship    matchingRelationship = null;
        String          linkedEntityGUID = null;
        EntityDetail    linkedEntity = null;

        /*
         * Step through the list of relationships looking for the matching one.  If parameters are null we will not
         * match with the list.
         */
        if (relationshipElementList != null)
        {
            for (Relationship  relationship : relationshipElementList)
            {
                if (relationship.getGUID().equals(linkingRelationshipGUID))
                {
                    matchingRelationship = relationship;
                    break;
                }
            }
        }

        /*
         * Return null if the relationship is not known
         */
        if (matchingRelationship == null)
        {
            return null;
        }

        /*
         * Extract the guid of the linking entity.
         */
        linkedEntityGUID = matchingRelationship.getLinkedEntity(anchorEntityGUID);

        /*
         * Return null if the entity does not match.
         */
        if (linkedEntityGUID == null)
        {
            return null;
        }

        /*
         * Step through the list of entities in the graph looking for the appropriate entity to return.
         * If no match occurs, null will be returned.
         */
        for (EntityDetail  entity : entityElementList)
        {
            if (entity.getGUID().equals(linkedEntityGUID))
            {
                linkedEntity = new EntityDetail(entity);
                break;
            }
        }

        return linkedEntity;
    }


    /**
     * Return the number of entities in the graph.
     *
     * @return elementCount for entities
     */
    public int getEntityElementCount()
    {
        return entityElementList.size();
    }


    /**
     * Return the number of relationships in the graph.
     *
     * @return elementCount for relationships
     */
    public int getRelationshipElementCount()
    {
        return relationshipElementList.size();
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "InstanceGraph{" +
                "entityElementList=" + entityElementList +
                ", relationshipElementList=" + relationshipElementList +
                ", entities=" + getEntities() +
                ", relationships=" + getRelationships() +
                ", entityElementCount=" + getEntityElementCount() +
                ", relationshipElementCount=" + getRelationshipElementCount() +
                '}';
    }
}
