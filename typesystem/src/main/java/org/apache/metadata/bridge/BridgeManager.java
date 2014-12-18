package org.apache.metadata.bridge;
	/*
	 * This will be the primary service for the Bridges (Will handle pushing type definitions into the type system and pushing entities to the repository system for Bridges)
	 */

import org.apache.metadata.ITypedStruct;
import org.apache.metadata.types.TypeSystem;

import com.google.gson.JsonObject;

public class BridgeManager {

	private TypeSystem ts;
	
	public BridgeManager(TypeSystem ts){
		this.ts = ts;
	}
		
	protected BridgeLoad convertToBridgeLoad(JsonObject jo){
		return null;
	}
	
	public ITypedStruct convertToIStruct(BridgeLoad bl, Bridge b){
		return null;
	}
	
}
