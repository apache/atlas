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

import {
  getGlossaryStatusMeta,
  GLOSSARY_STATUS_META,
  normalizeGlossaryStatus
} from "@utils/glossaryStatus";

describe("glossaryStatus", () => {
  it("normalizes glossary status values", () => {
    expect(normalizeGlossaryStatus("active")).toBe("ACTIVE");
    expect(normalizeGlossaryStatus("DRAFT")).toBe("DRAFT");
    expect(normalizeGlossaryStatus("Deprecated")).toBe("DEPRECATED");
    expect(normalizeGlossaryStatus("")).toBe("UNKNOWN");
    expect(normalizeGlossaryStatus(undefined)).toBe("UNKNOWN");
  });

  it("returns display metadata for each status", () => {
    expect(getGlossaryStatusMeta("ACTIVE")).toEqual(
      GLOSSARY_STATUS_META.ACTIVE
    );
    expect(getGlossaryStatusMeta("DRAFT").muiColor).toBe("warning");
    expect(getGlossaryStatusMeta("DEPRECATED").muiColor).toBe("error");
    expect(getGlossaryStatusMeta("invalid").muiColor).toBe("default");
  });
});
