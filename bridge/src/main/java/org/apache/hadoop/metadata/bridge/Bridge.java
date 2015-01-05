package org.apache.hadoop.metadata.bridge;

import org.apache.hadoop.metadata.types.TypeSystem;

public interface Bridge {
	
	boolean defineBridgeTypes(TypeSystem ts);
}
