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

import { useEffect, useMemo, useState } from "react";
import { getDetailPageData } from "../api/apiMethods/detailpageApiMethod";
import { ExtractObject } from "./ExtractObject";
import { getEntityRefStatus, mergeReferredEntity } from "./entityRefUtils";

type AuditEntityRefLinkProps = {
  entityRef: Record<string, unknown>;
  referredEntities?: Record<string, unknown>;
  properties?: string;
};

/**
 * Classic UI parity for audit relationship refs: merge page referredEntities,
 * then fetch entity header for current status when not already known.
 */
const AuditEntityRefLink = ({
  entityRef,
  referredEntities,
  properties
}: AuditEntityRefLinkProps) => {
  const mergedRef = useMemo(
    () => mergeReferredEntity(entityRef, referredEntities),
    [entityRef, referredEntities]
  );
  const [resolvedRef, setResolvedRef] = useState(mergedRef);
  const guid = mergedRef?.guid as string | undefined;
  const mergedStatus = getEntityRefStatus(mergedRef);

  useEffect(() => {
    setResolvedRef(mergedRef);
  }, [mergedRef]);

  useEffect(() => {
    if (!guid || mergedStatus) {
      return;
    }

    let cancelled = false;
    getDetailPageData(guid, {}, "headers")
      .then(({ data }) => {
        if (cancelled || !data) {
          return;
        }
        setResolvedRef((prev) => ({
          ...prev,
          ...data,
          status: data.status || data.entityStatus,
          entityStatus: data.entityStatus || data.status
        }));
      })
      .catch(() => {
        // Header lookup is best-effort; keep snapshot rendering on failure.
      });

    return () => {
      cancelled = true;
    };
  }, [guid, mergedStatus]);

  return (
    <ExtractObject
      keyValue={resolvedRef}
      properties={properties}
      skipHeaderFetch={true}
    />
  );
};

export default AuditEntityRefLink;
