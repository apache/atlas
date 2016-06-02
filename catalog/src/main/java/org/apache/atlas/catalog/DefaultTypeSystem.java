/**
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

package org.apache.atlas.catalog;

import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.atlas.AtlasException;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.catalog.exception.CatalogRuntimeException;
import org.apache.atlas.catalog.exception.ResourceAlreadyExistsException;
import org.apache.atlas.catalog.exception.ResourceNotFoundException;
import org.apache.atlas.repository.graph.TitanGraphProvider;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.exception.TypeExistsException;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Default implementation.
 */
public class DefaultTypeSystem implements AtlasTypeSystem {
    private final MetadataService metadataService;

    /**
     * Constructor.
     *
     * @param metadataService  atlas metadata service
     */
    public DefaultTypeSystem(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    @Override
    public void createEntity(ResourceDefinition definition, Request request) throws ResourceAlreadyExistsException {
        String typeName = definition.getTypeName();
        try {
            createClassType(definition, typeName, typeName + " Definition");
        } catch (ResourceAlreadyExistsException e) {
            // ok if type already exists
        }
        try {
            Referenceable entity = new Referenceable(typeName, request.getProperties());
            ITypedReferenceableInstance typedInstance = metadataService.getTypedReferenceableInstance(entity);
            metadataService.createEntities(Collections.singletonList(typedInstance).toArray(new ITypedReferenceableInstance[1]));
        } catch (EntityExistsException e) {
            throw new ResourceAlreadyExistsException(
                    "Attempted to create an entity which already exists: " + request.getProperties());
        } catch (AtlasException e) {
            throw new CatalogRuntimeException("An expected exception occurred creating an entity: " + e, e);
        }
    }

    @Override
    public void deleteEntity(ResourceDefinition definition, Request request) throws ResourceNotFoundException {
        String typeName = definition.getTypeName();
        String cleanIdPropName = definition.getIdPropertyName();
        String idValue = request.getProperty(cleanIdPropName);
        try {
            // transaction handled by atlas repository
            metadataService.deleteEntityByUniqueAttribute(typeName, cleanIdPropName, idValue);
        } catch (EntityNotFoundException e) {
            throw new ResourceNotFoundException(String.format("The specified entity doesn't exist: type=%s, %s=%s",
                    typeName, cleanIdPropName, idValue));
        } catch (AtlasException e) {
            throw new CatalogRuntimeException(String.format(
                    "An unexpected error occurred while attempting to delete entity: type=%s, %s=%s : %s",
                    typeName, cleanIdPropName, idValue, e), e);
        }
    }

    @Override
    public void createClassType(ResourceDefinition resourceDefinition, String name, String description)
            throws ResourceAlreadyExistsException {

        createType(resourceDefinition.getPropertyDefinitions(), ClassType.class, name, description, false);
    }

    @Override
    public void createTraitType(ResourceDefinition resourceDefinition, String name, String description)
            throws ResourceAlreadyExistsException {

        createType(resourceDefinition.getPropertyDefinitions(), TraitType.class, name, description, true);
    }

    @Override
    public void createTraitInstance(String guid, String typeName, Map<String, Object> properties)
            throws ResourceAlreadyExistsException {

        try {
            // not using the constructor with properties argument because it is marked 'InterfaceAudience.Private'
            Struct struct = new Struct(typeName);
            for (Map.Entry<String, Object> propEntry : properties.entrySet()) {
                struct.set(propEntry.getKey(), propEntry.getValue());
            }
            metadataService.addTrait(guid, metadataService.createTraitInstance(struct));
        } catch (IllegalArgumentException e) {
            //todo: unfortunately, IllegalArgumentException can be thrown for other reasons
            if (e.getMessage().contains("is already defined for entity")) {
                throw new ResourceAlreadyExistsException(
                        String.format("Tag '%s' already associated with the entity", typeName));
            } else {
                throw e;
            }
        } catch (AtlasException e) {
            throw new CatalogRuntimeException(String.format(
                    "Unable to create trait instance '%s' in type system: %s", typeName, e), e);
        }
    }

    @Override
    public void deleteTag(String guid, String traitName) throws ResourceNotFoundException {
        try {
            metadataService.deleteTrait(guid, traitName);
        } catch (TraitNotFoundException e) {
            throw new ResourceNotFoundException(String.format(
                    "The trait '%s' doesn't exist for entity '%s'", traitName, guid));
        } catch (AtlasException e) {
            throw new CatalogRuntimeException(String.format(
                    "Unable to delete tag '%s' from entity '%s'", traitName, guid), e);
        }
    }

    private <T extends HierarchicalType> void createType(Collection<AttributeDefinition> attributes,
                                                         Class<T> type,
                                                         String name,
                                                         String description,
                                                         boolean isTrait)
                                                         throws ResourceAlreadyExistsException {

        try {
            HierarchicalTypeDefinition<T> definition = new HierarchicalTypeDefinition<>(type, name, description, null,
                    attributes.toArray(new AttributeDefinition[attributes.size()]));

            metadataService.createType(TypesSerialization.toJson(definition, isTrait));
        } catch (TypeExistsException e) {
            throw new ResourceAlreadyExistsException(String.format("Type '%s' already exists", name));
        } catch (AtlasException e) {
            throw new CatalogRuntimeException(String.format(
                    "Unable to create type '%s' in type system: %s", name, e), e);
        }
    }
}
