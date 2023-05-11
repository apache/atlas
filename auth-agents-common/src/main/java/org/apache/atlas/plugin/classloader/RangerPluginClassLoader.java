/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.classloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;
import java.util.List;

public class RangerPluginClassLoader extends URLClassLoader {
	private static final Logger LOG = LoggerFactory.getLogger(RangerPluginClassLoader.class);

    ThreadLocal<ClassLoader> preActivateClassLoader = new ThreadLocal<>();

	private static volatile RangerPluginClassLoader me               = null;
	private static  MyClassLoader				componentClassLoader = null;

	public RangerPluginClassLoader(String pluginType, Class<?> pluginClass ) throws Exception {
		super(RangerPluginClassLoaderUtil.getInstance().getPluginFilesForServiceTypeAndPluginclass(pluginType, pluginClass), null);
		componentClassLoader = AccessController.doPrivileged(
									new PrivilegedAction<MyClassLoader>() {
										public MyClassLoader run() {
												return  new MyClassLoader(Thread.currentThread().getContextClassLoader());
										}
									}
								);
    }

	public static RangerPluginClassLoader getInstance(final String pluginType, final Class<?> pluginClass ) throws Exception {
		RangerPluginClassLoader ret = me;
	    if ( ret == null) {
		  synchronized(RangerPluginClassLoader.class) {
		  ret = me;
		  if (ret == null && pluginClass != null) {
			  me = ret = AccessController.doPrivileged(
							new PrivilegedExceptionAction<RangerPluginClassLoader>(){
								public RangerPluginClassLoader run() throws Exception {
									return  new RangerPluginClassLoader(pluginType,pluginClass);
							}
						}
				   );
		      }
		   }
	   }
	    return ret;
    }
	
    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPluginClassLoader.findClass(" + name + ")");
		}

        Class<?> ret = null;

        try {
            // first we try to find a class inside the child classloader
            if(LOG.isDebugEnabled()) {
                LOG.debug("RangerPluginClassLoader.findClass(" + name + "): calling childClassLoader().findClass() ");
            }

            ret = super.findClass(name);
        } catch( Throwable e ) {
           // Use the Component ClassLoader findclass to load when childClassLoader fails to find
           if(LOG.isDebugEnabled()) {
               LOG.debug("RangerPluginClassLoader.findClass(" + name + "): calling componentClassLoader.findClass()");
           }

           MyClassLoader savedClassLoader = getComponentClassLoader();
           if (savedClassLoader != null) {
             ret = savedClassLoader.findClass(name);
           }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPluginClassLoader.findClass(" + name + "): " + ret);
        }
        return ret;
    }

    @Override
    public synchronized Class<?> loadClass(String name) throws ClassNotFoundException {
        if (LOG.isDebugEnabled()) {
             LOG.debug("==> RangerPluginClassLoader.loadClass(" + name + ")" );
        }

        Class<?> ret = null;

        try {
            // first we try to load a class inside the child classloader
            if (LOG.isDebugEnabled()) {
                 LOG.debug("RangerPluginClassLoader.loadClass(" + name + "): calling childClassLoader.findClass()");
            }
            ret = super.loadClass(name);
         } catch(Throwable e) {
            // Use the Component ClassLoader loadClass to load when childClassLoader fails to find
            if (LOG.isDebugEnabled()) {
                LOG.debug("RangerPluginClassLoader.loadClass(" + name + "): calling componentClassLoader.loadClass()");
           }

            MyClassLoader savedClassLoader = getComponentClassLoader();

            if(savedClassLoader != null) {
              ret = savedClassLoader.loadClass(name);
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPluginClassLoader.loadClass(" + name + "): " + ret);
        }

        return ret;
    }

    @Override
	public URL findResource(String name) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPluginClassLoader.findResource(" + name + ") ");
        }

        URL ret =  super.findResource(name);

        if (ret == null) {
           if(LOG.isDebugEnabled()) {
               LOG.debug("RangerPluginClassLoader.findResource(" + name + "): calling componentClassLoader.getResources()");
           }

           MyClassLoader savedClassLoader = getComponentClassLoader();
           if (savedClassLoader != null) {
              ret = savedClassLoader.getResource(name);
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPluginClassLoader.findResource(" + name + "): " + ret);
        }

        return ret;
    }

    @Override
	public Enumeration<URL> findResources(String name) throws IOException {
        Enumeration<URL> ret = null;

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPluginClassLoader.findResources(" + name + ") ");
        }

        ret =  new MergeEnumeration(findResourcesUsingChildClassLoader(name),findResourcesUsingComponentClassLoader(name));

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPluginClassLoader.findResources(" + name + ") ");
        }

        return ret;
    }

    public Enumeration<URL> findResourcesUsingChildClassLoader(String name) {

        Enumeration<URL> ret = null;

        try {
            if(LOG.isDebugEnabled()) {
                LOG.debug("RangerPluginClassLoader.findResourcesUsingChildClassLoader(" + name + "): calling childClassLoader.findResources()");
            }

		    ret =  super.findResources(name);

        } catch ( Throwable t) {
           //Ignore any exceptions. Null / Empty return is handle in following statements
           if(LOG.isDebugEnabled()) {
               LOG.debug("RangerPluginClassLoader.findResourcesUsingChildClassLoader(" + name + "): class not found in child. Falling back to componentClassLoader", t);
           }
        }
       return ret;
    }

    public Enumeration<URL> findResourcesUsingComponentClassLoader(String name) {

	     Enumeration<URL> ret = null;

         try {

             if(LOG.isDebugEnabled()) {
                 LOG.debug("RangerPluginClassLoader.findResourcesUsingComponentClassLoader(" + name + "): calling componentClassLoader.getResources()");
             }

             MyClassLoader savedClassLoader = getComponentClassLoader();

             if (savedClassLoader != null) {
                 ret = savedClassLoader.getResources(name);
             }

             if(LOG.isDebugEnabled()) {
                 LOG.debug("<== RangerPluginClassLoader.findResourcesUsingComponentClassLoader(" + name + "): " + ret);
             }
         } catch( Throwable t) {
	        if(LOG.isDebugEnabled()) {
		       LOG.debug("RangerPluginClassLoader.findResourcesUsingComponentClassLoader(" + name + "): class not found in componentClassLoader.", t);
            }
         }

         return ret;
     }

    public void activate() {
        if(LOG.isDebugEnabled()) {
           LOG.debug("==> RangerPluginClassLoader.activate()");
        }

        //componentClassLoader.set(new MyClassLoader(Thread.currentThread().getContextClassLoader()));

        preActivateClassLoader.set(Thread.currentThread().getContextClassLoader());

        Thread.currentThread().setContextClassLoader(this);

        if(LOG.isDebugEnabled()) {
           LOG.debug("<== RangerPluginClassLoader.activate()");
        }
    }

    public void deactivate() {

       if(LOG.isDebugEnabled()) {
          LOG.debug("==> RangerPluginClassLoader.deactivate()");
       }

       ClassLoader classLoader = preActivateClassLoader.get();

       if (classLoader != null) {
           preActivateClassLoader.remove();
       } else {
           MyClassLoader savedClassLoader = getComponentClassLoader();
           if (savedClassLoader != null && savedClassLoader.getParent() != null) {
               classLoader = savedClassLoader.getParent();
           }
       }

       if (classLoader != null) {
           Thread.currentThread().setContextClassLoader(classLoader);
       } else {
           LOG.warn("RangerPluginClassLoader.deactivate() was not successful. Couldn't not get the saved classLoader...");
       }

       if(LOG.isDebugEnabled()) {
          LOG.debug("<== RangerPluginClassLoader.deactivate()");
       }
    }

    private MyClassLoader getComponentClassLoader() {
    	return  componentClassLoader;
        //return componentClassLoader.get();
   }

   static class  MyClassLoader extends ClassLoader {
        public MyClassLoader(ClassLoader realClassLoader) {
           super(realClassLoader);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException { //NOPMD
           return super.findClass(name);
        }
    }

   static class MergeEnumeration implements Enumeration<URL> { //NOPMD

        Enumeration<URL>  e1 = null;
        Enumeration<URL>  e2 = null;

        public MergeEnumeration(Enumeration<URL> e1, Enumeration<URL> e2 ) {
			this.e1 = e1;
			this.e2 = e2;
		}

        @Override
		public boolean hasMoreElements() {
			return ( (e1 != null && e1.hasMoreElements() ) || ( e2 != null && e2.hasMoreElements()) );
		}

        @Override
		public URL nextElement() {
			URL ret = null;
			if (e1 != null && e1.hasMoreElements())
			    ret = e1.nextElement();
			else if ( e2 != null && e2.hasMoreElements() ) {
				ret = e2.nextElement();
			}
			return ret;
		}
    }

    public ScriptEngine getScriptEngine(String engineName) {

	    final ScriptEngine ret;

        ClassLoader classLoader = preActivateClassLoader.get();

        if (classLoader == null) {
            MyClassLoader savedClassLoader = getComponentClassLoader();
            if (savedClassLoader != null && savedClassLoader.getParent() != null) {
                classLoader = savedClassLoader.getParent();
            }
        }

        ScriptEngineManager manager = classLoader != null ? new ScriptEngineManager(classLoader) : new ScriptEngineManager();

        if (LOG.isDebugEnabled()) {

            List<ScriptEngineFactory> factories = manager.getEngineFactories();

            if (factories == null || factories.size() == 0) {
                LOG.debug("List of scriptEngineFactories is empty!!");

            } else {
                for (ScriptEngineFactory factory : factories) {
                    LOG.debug("engineName=" + factory.getEngineName() + ", language=" + factory.getLanguageName());

                }
            }
        }

        ret = manager.getEngineByName(engineName);

        if (ret == null) {
            LOG.error("scriptEngine for JavaScript is null!!");
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("scriptEngine for JavaScript:[" + ret + "]");
            }
        }

        return ret;
    }
}
