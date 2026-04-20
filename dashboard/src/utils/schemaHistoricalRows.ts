/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with
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

import { entityStateReadOnly } from "@utils/Enum";

/** Same ordering as Classic `sortRowsInRelation` / Schema tab. */
export const sortRowsInRelation = (rows: any[]): any[] => {
  return [...rows].sort((a, b) => {
    const pa = a?.attributes?.position;
    const pb = b?.attributes?.position;
    if (pa != null && pb != null && Number(pa) !== Number(pb)) {
      return Number(pa) - Number(pb);
    }
    const na = String(a?.attributes?.name ?? "");
    const nb = String(b?.attributes?.name ?? "");
    return na.localeCompare(nb);
  });
};

export const isSchemaRowHistorical = (row: any): boolean => {
  return Boolean(
    row?.status && entityStateReadOnly[row.status as string]
  );
};

/**
 * Client-side visibility for the schema table (Classic: active only vs
 * active + historical; if only historical exist, show historical only).
 * When historical is requested but the cache has none, return [] so the
 * table empty state shows "No records found" (not active rows + banner).
 */
export const getVisibleSchemaRowsFromFullMerged = (
  fullMergedRows: any[],
  checked: boolean
): any[] => {
  const active = fullMergedRows.filter((r) => !isSchemaRowHistorical(r));
  const historical = fullMergedRows.filter((r) => isSchemaRowHistorical(r));
  if (!checked) {
    return active;
  }
  if (historical.length === 0) {
    return [];
  }
  if (active.length === 0 && historical.length > 0) {
    return historical;
  }
  return active.concat(historical);
};

export const countHistoricalInFullMerged = (fullMergedRows: any[]): number => {
  return fullMergedRows.filter((r) => isSchemaRowHistorical(r)).length;
};

export const buildFullMergedResolvedRows = (
  rowsByRelation: Record<string, any[]>,
  relationNames: string[],
  resolve: (r: any) => any
): any[] => {
  const blocks: any[] = [];
  for (const name of relationNames) {
    const raw = rowsByRelation[name] || [];
    const resolved = raw.map((r) => resolve(r));
    blocks.push(...sortRowsInRelation(resolved));
  }
  return blocks;
};
