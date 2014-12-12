package org.apache.metadata.types;

import com.google.common.collect.ImmutableList;

public class StructTypeDefinition {

    public final String typeName;
    public final AttributeDefinition[] attributeDefinitions;

    public StructTypeDefinition(String typeName,
                                AttributeDefinition[] attributeDefinitions) {
        this.typeName = typeName;
        this.attributeDefinitions = attributeDefinitions;
    }
}
