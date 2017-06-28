/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.odf.core.controlcenter;

import java.util.concurrent.Callable;

/**
 * Use this interface in the core framework whenever you want to run code that is run from an unmanaged thread (typically in the Kafka consumers)
 * and that accesses the metadata repository. The implementation of this class will ensure that the code will be run in the
 * correct context (regarding transactions etc.)
 * 
 *
 */
public interface TransactionContextExecutor {
	
	/**
	 * Run a generic callable in a transaction context. This is not a template function as some of the underlying infrastructures
	 * might not be able to support it.
	 */
	Object runInTransactionContext(Callable<Object> callable) throws Exception;
	
}
