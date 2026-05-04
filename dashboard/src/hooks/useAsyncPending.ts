/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { useCallback, useRef, useState } from "react";

/**
 * Single-flight async guard + pending flag for primary actions (modals, forms).
 * Reuse wherever a button triggers an API call and should show loading state.
 */
export const useAsyncPending = () => {
	const [pending, setPending] = useState(false);
	const busyRef = useRef(false);

	const run = useCallback(async <T,>(fn: () => Promise<T>): Promise<T | undefined> => {
		if (busyRef.current) {
			return undefined;
		}
		busyRef.current = true;
		setPending(true);
		try {
			return await fn();
		} finally {
			busyRef.current = false;
			setPending(false);
		}
	}, []);

	return { pending, run };
};
