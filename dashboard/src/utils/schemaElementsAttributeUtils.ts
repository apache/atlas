/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { isEmpty } from "@utils/Utils";

/**
 * Normalizes typedef option schemaElementsAttribute (string | string[] or
 * comma-separated) into a list of relationship attribute names for API calls.
 */
export const normalizeSchemaElementsAttribute = (
  raw: string | string[] | undefined | null
): string[] => {
  if (raw === undefined || raw === null) {
    return [];
  }
  if (Array.isArray(raw)) {
    return raw
      .map((s) => String(s).trim())
      .filter((s) => s.length > 0);
  }
  const s = String(raw).trim();
  if (isEmpty(s)) {
    return [];
  }
  if (s.includes(",")) {
    return s
      .split(",")
      .map((p) => p.trim())
      .filter((p) => p.length > 0);
  }
  return [s];
};
