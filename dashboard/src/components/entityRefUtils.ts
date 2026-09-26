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

import { isObject } from "../utils/Utils";

export const mergeReferredEntity = (
  obj: Record<string, unknown>,
  referredEntities?: Record<string, unknown>
) => {
  if (obj?.guid && referredEntities?.[obj.guid as string] != undefined) {
    return { ...obj, ...(referredEntities[obj.guid as string] as object) };
  }
  return obj;
};

export const getEntityRefStatus = (ref: Record<string, unknown>) =>
  (ref?.status ||
    ref?.entityStatus ||
    (isObject(ref?.id) ? (ref.id as { state?: string }).state : ref?.state)) as
    | string
    | undefined;
