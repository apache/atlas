package org.apache.hadoop.metadata.bridge;

import java.util.ArrayList;

import org.apache.hadoop.metadata.types.StructType;
import org.apache.hadoop.metadata.types.StructTypeDefinition;
	/*
	 * Interface for creating Bridges
	 */

public interface Bridge {

	ArrayList<StructTypeDefinition> structs = new ArrayList<StructTypeDefinition>();
	
	public ArrayList<StructTypeDefinition> getStructTypeDefinitions();
	
	public boolean submitLoad(BridgeLoad load);
}
