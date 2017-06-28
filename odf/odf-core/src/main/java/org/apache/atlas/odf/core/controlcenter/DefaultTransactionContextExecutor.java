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
 * The default TransactionContextExecutor runs code in the same thread as the caller.
 * 
 */
public class DefaultTransactionContextExecutor implements TransactionContextExecutor {
	
	@Override
	public Object runInTransactionContext(Callable<Object> callable) throws Exception {
		return callable.call();
	}

}
