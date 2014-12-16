package org.apache.metadata.bridge;

import java.util.ArrayList;

import org.apache.metadata.types.StructType;
import org.apache.metadata.types.StructTypeDefinition;
	/*
	 * Interface for creating Bridges
	 */

public interface Bridge {

	ArrayList<StructTypeDefinition> structs = new ArrayList<StructTypeDefinition>();
	
	public ArrayList<StructTypeDefinition> getStructTypeDefinitions();
	
	public boolean submitLoad(BridgeLoad load);
}
