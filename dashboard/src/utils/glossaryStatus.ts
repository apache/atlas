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

export type GlossaryStatusKey = "ACTIVE" | "DRAFT" | "DEPRECATED" | "UNKNOWN";

export type GlossaryStatusMeta = {
  label: string;
  shortLabel: string;
  muiColor: "success" | "warning" | "error" | "default";
  ariaLabel: string;
};

export const GLOSSARY_STATUS_META: Record<GlossaryStatusKey, GlossaryStatusMeta> = {
  ACTIVE: {
    label: "Active",
    shortLabel: "Active",
    muiColor: "success",
    ariaLabel: "Active status"
  },
  DRAFT: {
    label: "Draft",
    shortLabel: "Draft",
    muiColor: "warning",
    ariaLabel: "Draft status"
  },
  DEPRECATED: {
    label: "Deprecated",
    shortLabel: "Depr.",
    muiColor: "error",
    ariaLabel: "Deprecated status"
  },
  UNKNOWN: {
    label: "Unknown",
    shortLabel: "—",
    muiColor: "default",
    ariaLabel: "Unknown status"
  }
};

export const normalizeGlossaryStatus = (status?: string): GlossaryStatusKey => {
  if (!status) {
    return "UNKNOWN";
  }

  const normalized = status.trim().toUpperCase();
  if (normalized === "ACTIVE") {
    return "ACTIVE";
  }
  if (normalized === "DRAFT") {
    return "DRAFT";
  }
  if (normalized === "DEPRECATED") {
    return "DEPRECATED";
  }

  return "UNKNOWN";
};

export const getGlossaryStatusMeta = (status?: string): GlossaryStatusMeta => {
  const key = normalizeGlossaryStatus(status);
  return GLOSSARY_STATUS_META[key];
};
