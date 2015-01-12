package org.apache.hadoop.metadata.services;

import org.apache.commons.configuration.ConfigurationException;

import com.google.inject.throwingproviders.CheckedProvider;
import com.tinkerpop.blueprints.Graph;

public interface GraphProvider<T extends Graph> extends CheckedProvider<T> {

	@Override
	T get() throws ConfigurationException;
}
