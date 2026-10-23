/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Test JVM system class loader for hive-bridge integration tests on Java 9+.
 * Hive 3.1.x {@code SessionState} casts {@code SessionState.class.getClassLoader()}
 * to {@link URLClassLoader}; the default application loader is not a URLClassLoader
 * on Java 9+, so tests install this loader via {@code -Djava.system.class.loader}.
 */
public final class HiveITSystemClassLoader extends URLClassLoader {
    private static final ClassLoader PLATFORM_CLASS_LOADER = platformClassLoader();

    static {
        ClassLoader.registerAsParallelCapable();
    }

    private static ClassLoader platformClassLoader() {
        try {
            Method getPlatformClassLoader = ClassLoader.class.getMethod("getPlatformClassLoader");
            return (ClassLoader) getPlatformClassLoader.invoke(null);
        } catch (ReflectiveOperationException e) {
            ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
            return parent != null ? parent : ClassLoader.getSystemClassLoader();
        }
    }

    public HiveITSystemClassLoader(ClassLoader parent) {
        super(classpathUrls(), parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loadedClass = findLoadedClass(name);

            if (loadedClass == null) {
                if (isPlatformClass(name)) {
                    loadedClass = PLATFORM_CLASS_LOADER.loadClass(name);
                } else {
                    try {
                        loadedClass = findClass(name);
                    } catch (ClassNotFoundException e) {
                        loadedClass = PLATFORM_CLASS_LOADER.loadClass(name);
                    }
                }
            }

            if (resolve) {
                resolveClass(loadedClass);
            }

            return loadedClass;
        }
    }

    private static boolean isPlatformClass(String name) {
        return name.startsWith("java.") || name.startsWith("jdk.");
    }

    private static URL[] classpathUrls() {
        String classpath = System.getProperty("java.class.path");
        String[] entries  = classpath.split(File.pathSeparator);
        URL[]    urls     = new URL[entries.length];
        int      urlCount = 0;

        for (String entry : entries) {
            if (entry.isEmpty()) {
                continue;
            }

            try {
                urls[urlCount++] = new File(entry).toURI().toURL();
            } catch (MalformedURLException e) {
                throw new IllegalStateException("Invalid classpath entry: " + entry, e);
            }
        }

        if (urlCount == urls.length) {
            return urls;
        }

        URL[] trimmed = new URL[urlCount];
        System.arraycopy(urls, 0, trimmed, 0, urlCount);
        return trimmed;
    }
}
