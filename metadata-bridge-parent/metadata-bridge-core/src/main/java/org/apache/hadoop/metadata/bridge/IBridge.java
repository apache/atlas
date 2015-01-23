package org.apache.hadoop.metadata.bridge;

import org.apache.hadoop.metadata.types.TypeSystem;

public interface IBridge {
	
	boolean defineBridgeTypes(TypeSystem ts);
	
	
}
