/**
 * Licensed to the Apache Software Foundation (ASF) under oneØ
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
package org.apache.atlas.model.typedef;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * AtlasRelationshipDef is a TypeDef that defines a relationship.
 *
 * As with other typeDefs the AtlasRelationshipDef has a name. Once created the RelationshipDef has a guid.
 * The name and the guid are the 2 ways that the RelationshipDef is identified.
 *
 * RelationshipDefs have 2 endpoints, each of which specify cardinality, an EntityDef type name and name and optionally
 * whether the endpoint is a container.
 * RelationshipDefs can have AttributeDefs - though only primitive types are allowed.
 * RelationshipDefs have a relationshipCategory specifying the UML type of relationship required
 * RelationshipDefs also have a PropogateTag - indicating which way tags could flow over the relationships.
 *
 * The way EntityDefs and RelationshipDefs are intended to be used is that EntityDefs will define AttributeDefs these AttributeDefs
 * will not specify an EntityDef type name as their types.
 *
 * RelationshipDefs introduce new atributes to the entity instances. For example
 * EntityDef A might have attributes attr1,attr2,attr3
 * EntityDef B might have attributes attr4,attr5,attr6
 * RelationshipDef AtoB might define 2 endpoints
 *  endpoint1:  type A, name attr7
 *  endpoint1:  type B, name attr8
 *
 * When an instance of EntityDef A is created, it will have attributes attr1,attr2,attr3,attr7
 * When an instance of EntityDef B is created, it will have attributes attr4,attr5,attr6,attr8
 *
 * In this way relationshipDefs can be authored separately from entityDefs and can inject relationship attributes into
 * the entity instances
 *
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasRelationshipDef extends AtlasStructDef implements java.io.Serializable {
    private static final long serialVersionUID = 1L;


    /**
     * The Relationship category determines the style of relationship around containment and lifecycle.
     * UML terminology is used for the values.
     * ASSOCIATION is a relationship with no containment.
     * COMPOSITION and AGGREGATION are containment relationships.
     * The difference being in the lifecycles of the container and its children. In the COMPOSITION case,
     * the children cannot exist without the container. For AGGREGATION, the life cycles
     * of the container and children are totally independant.
     */
    public enum RelationshipCategory {
        ASSOCIATION, AGGREGATION, COMPOSITION
    };

    /**
     * PropagateTags indicates whether tags should propagate across the relationship instance.
     * Tags can propagate:
     * NONE - not at all
     * ONE_TO_TWO - from endpoint 1 to 2
     * TWO_TO_ONE - from endpoint 2 to 1
     * BOTH - both ways
     *
     * Care needs to be taken when specifying. The use cases we are aware of this flag being useful are :
     *
     * - propagating confidentiality classifications from a table to columns - ONE_TO_TWO could be used here
     * - propagating classifications around Glossary synonyms - BOTH could be used here.
     *
     * There is an expectation that further enhancements will allow more granular control of tag propagation and will
     * address how to resolve conflicts.
     */
    public enum PropagateTags {
        NONE, ONE_TO_TWO, TWO_TO_ONE, BOTH
    };

    private RelationshipCategory         relationshipCategory;
    private PropagateTags                propagateTags;
    private AtlasRelationshipEndPointDef endPointDef1;
    private AtlasRelationshipEndPointDef endPointDef2;

    /**
     * AtlasRelationshipDef contructor
     * @throws AtlasBaseException
     */
    public AtlasRelationshipDef() throws AtlasBaseException {
        this(null, null, null, null,null, null, null);
    }

    /**
     * Create a relationshipDef without attributeDefs
     * @param name
     *            - the name of the relationship type
     * @param description
     *            - an optional description
     * @param typeVersion
     *            - version - that defaults to 1.0
     * @param relationshipCategory
     *            - there are 3 sorts of relationship category ASSOCIATION, COMPOSITION
     *            and AGGREGATION
     * @param propagatetags
     *            -
     * @param endPointDef1
     *            - first endpoint. As endpoint specifies an entity
     *            type and an attribute name. the attribute name then appears in
     *            the relationship instance
     * @param endPointDef2
     *            - second endpoint. The endpoints are defined as 1
     *            ad 2 to avoid implying a direction. So we do not use to and
     *            from.
     * @throws AtlasBaseException
     */
    public AtlasRelationshipDef(String name, String description, String typeVersion,
                                RelationshipCategory relationshipCategory,
                                PropagateTags propagatetags,
                                AtlasRelationshipEndPointDef endPointDef1,
                                AtlasRelationshipEndPointDef endPointDef2) throws AtlasBaseException {
        this(name, description, typeVersion, relationshipCategory,propagatetags, endPointDef1, endPointDef2,
             new ArrayList<AtlasAttributeDef>());
    }

    /**
     * Create a relationshipDef with attributeDefs
     * @param name
     *            - the name of the relationship type
     * @param description
     *            - an optional description
     * @param typeVersion
     *            - version - that defaults to 1.0
     * @param relationshipCategory
     *            - there are 3 sorts of relationship category ASSOCIATION, COMPOSITION
     *            and AGGREGATION
     * @param propagatetags
     *            -
     * @param endPointDef1
     *            - First endpoint. As endpoint specifies an entity
     *            type and an attribute name. the attribute name then appears in
     *            the relationship instance
     * @param endPointDef2
     *            - Second endpoint. The endpoints are defined as 1
     *            ad 2 to avoid implying a direction. So we do not use to and
     *            from.
     * @param attributeDefs
     *            - these are the attributes on the relationship itself.
     */
    public AtlasRelationshipDef(String name, String description, String typeVersion,
                                RelationshipCategory relationshipCategory,
                                PropagateTags propagatetags, AtlasRelationshipEndPointDef endPointDef1,
                                AtlasRelationshipEndPointDef endPointDef2, List<AtlasAttributeDef> attributeDefs)
            {
        super(TypeCategory.RELATIONSHIP, name, description, typeVersion, attributeDefs, null);

        setRelationshipCategory(relationshipCategory);
        setPropagateTags(propagatetags);
        setEndPointDef1(endPointDef1);
        setEndPointDef2(endPointDef2);
    }

    public void setRelationshipCategory(RelationshipCategory relationshipCategory) {
        this.relationshipCategory = relationshipCategory;
    }

    public RelationshipCategory getRelationshipCategory() {
        return this.relationshipCategory;
    }

    public void setPropagateTags(PropagateTags propagateTags) {
        this.propagateTags=propagateTags;
    }

    public PropagateTags getPropagateTags() {
        return this.propagateTags;
    }

    public void setEndPointDef1(AtlasRelationshipEndPointDef endPointDef1) {
        this.endPointDef1 = endPointDef1;
    }

    public AtlasRelationshipEndPointDef getEndPointDef1() {
        return this.endPointDef1;
    }

    public void setEndPointDef2(AtlasRelationshipEndPointDef endPointDef2) {
        this.endPointDef2 = endPointDef2;
    }

    public AtlasRelationshipEndPointDef getEndPointDef2() {
        return this.endPointDef2;
    }

    public AtlasRelationshipDef(AtlasRelationshipDef other) throws AtlasBaseException {
        super(other);

        if (other != null) {
            setRelationshipCategory(other.getRelationshipCategory());
            setPropagateTags(other.getPropagateTags());
            setEndPointDef1(other.getEndPointDef1());
            setEndPointDef2(other.getEndPointDef2());
        }
    }
    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasRelationshipDef{");
        super.toString(sb);
        sb.append(',');
        sb.append(this.relationshipCategory);
        sb.append(',');
        sb.append(this.propagateTags);
        sb.append(',');
        sb.append(this.endPointDef1.toString());
        sb.append(',');
        sb.append(this.endPointDef2.toString());
        sb.append('}');
        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        //AttributeDefs are checked in the super
        if (!super.equals(o))
            return false;
        AtlasRelationshipDef that = (AtlasRelationshipDef) o;
        if (!Objects.equals(relationshipCategory, that.getRelationshipCategory()))
            return false;
        if (!Objects.equals(propagateTags, that.getPropagateTags()))
            return false;
        if (!Objects.equals(endPointDef1, that.getEndPointDef1()))
            return false;
        return (Objects.equals(endPointDef2, that.getEndPointDef2()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), relationshipCategory, propagateTags, endPointDef1, endPointDef2);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }
}
