package org.apache.hadoop.metadata.bridge;

import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.TypeSystem;

import com.google.common.collect.ImmutableList;

public class BridgeAssistant {

	protected HierarchicalTypeDefinition<ClassType> createClassTypeDef(String name, ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {return new HierarchicalTypeDefinition(ClassType.class, name, superTypes, attrDefs);}

}
