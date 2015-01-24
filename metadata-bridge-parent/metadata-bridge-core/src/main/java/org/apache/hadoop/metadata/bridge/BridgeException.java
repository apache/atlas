package org.apache.hadoop.metadata.bridge;

import org.apache.hadoop.hive.metastore.api.MetaException;

public class BridgeException extends MetaException {

	public BridgeException(String msg) {
		super(msg);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -384401342591560473L;

}
