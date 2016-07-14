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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasException;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.catalog.exception.CatalogRuntimeException;
import org.apache.atlas.catalog.exception.ResourceAlreadyExistsException;
import org.apache.atlas.catalog.exception.ResourceNotFoundException;
import org.apache.atlas.classification.InterfaceAudience;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.exception.TypeExistsException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
    public DefaultTypeSystem(MetadataService metadataService) throws AtlasException {
        this.metadataService = metadataService;
        //Create namespace
        createSuperTypes();
    }

    @InterfaceAudience.Private
    private void createSuperTypes() throws AtlasException {
        HierarchicalTypeDefinition<TraitType> termType = TypesUtil
            .createTraitTypeDef(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE, ImmutableSet.<String>of(),
                TypesUtil.createOptionalAttrDef(TaxonomyResourceProvider.NAMESPACE_ATTRIBUTE_NAME,
                    DataTypes.STRING_TYPE));

        createTraitType(termType);
    }

    private void createTraitType(HierarchicalTypeDefinition<TraitType> type) throws AtlasException {
        try {
            metadataService.getTypeDefinition(type.typeName);
        } catch(TypeNotFoundException tne) {
            //Type not found . Create
            TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(type),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
            metadataService.createType(TypesSerialization.toJson(typesDef));
        }
    }

    @Override
    public String createEntity(ResourceDefinition definition, Request request) throws ResourceAlreadyExistsException {
        String typeName = definition.getTypeName();
        try {
            createClassType(definition, typeName, typeName + " Definition");
        } catch (ResourceAlreadyExistsException e) {
            // ok if type already exists
        }
        try {
            Referenceable entity = new Referenceable(typeName, request.getQueryProperties());
            //add Taxonomy Namespace
            entity.set(TaxonomyResourceProvider.NAMESPACE_ATTRIBUTE_NAME, TaxonomyResourceProvider.TAXONOMY_NS);

            ITypedReferenceableInstance typedInstance = metadataService.getTypedReferenceableInstance(entity);
            final List<String> entities = metadataService.createEntities(Collections.singletonList(typedInstance).toArray(new ITypedReferenceableInstance[1]));
            return entities != null && entities.size() > 0 ? entities.get(0) : null;
        } catch (EntityExistsException e) {
            throw new ResourceAlreadyExistsException(
                    "Attempted to create an entity which already exists: " + request.getQueryProperties());
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

            //add Taxonomy Namespace
            struct.set(TaxonomyResourceProvider.NAMESPACE_ATTRIBUTE_NAME, TaxonomyResourceProvider.TAXONOMY_NS);
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
            HierarchicalTypeDefinition<T> definition = null;
            if ( isTrait) {
                definition = new HierarchicalTypeDefinition<T>(type, name, description,
                    ImmutableSet.<String>of(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE), attributes.toArray(new AttributeDefinition[attributes.size()]));
            } else {
                definition = new HierarchicalTypeDefinition<T>(type, name, description,
                    ImmutableSet.<String>of(), attributes.toArray(new AttributeDefinition[attributes.size()]));
            }

            metadataService.createType(TypesSerialization.toJson(definition, isTrait));
        } catch (TypeExistsException e) {
            throw new ResourceAlreadyExistsException(String.format("Type '%s' already exists", name));
        } catch (AtlasException e) {
            throw new CatalogRuntimeException(String.format(
                    "Unable to create type '%s' in type system: %s", name, e), e);
        }
    }
}
