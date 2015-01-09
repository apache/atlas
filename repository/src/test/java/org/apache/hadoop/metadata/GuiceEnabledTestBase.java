package org.apache.hadoop.metadata;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

public abstract class GuiceEnabledTestBase {

	/*
	 * Guice.createInjector() takes your Modules, and returns a new Injector
	 * instance. Most applications will call this method exactly once, in their
	 * main() method.
	 */
	public final Injector injector;

	GuiceEnabledTestBase() {
		injector = Guice.createInjector();
	}

	GuiceEnabledTestBase(Module... modules) {
		injector = Guice.createInjector(modules);
	}
}
