package org.apache.hadoop.metadata.bridge;

import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.TypeSystem;

import com.google.common.collect.ImmutableList;

public interface Bridge {
	
	boolean defineBridgeTypes(TypeSystem ts);
	
}
