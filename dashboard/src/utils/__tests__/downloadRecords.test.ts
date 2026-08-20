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
  dedupeDownloadRecordsByFileName,
  extractDownloadRecords,
  filterDownloadRecordsForToggle,
  mergeDownloadRecords,
  parseCreatedTime,
  parseTimestampFromFileName,
  resolveDownloadRecordTime,
  sortDownloadRecordsByLatest
} from "../downloadRecords";

describe("downloadRecords utils", () => {
  it("parseCreatedTime accepts epoch numbers", () => {
    expect(parseCreatedTime(1_700_000_000_000)).toBe(1_700_000_000_000);
  });

  it("parseCreatedTime accepts ISO strings", () => {
    const iso = "2026-08-12T15:36:29.000Z";
    expect(parseCreatedTime(iso)).toBe(Date.parse(iso));
  });

  it("parseCreatedTime returns null for invalid values", () => {
    expect(parseCreatedTime(undefined)).toBeNull();
    expect(parseCreatedTime("not-a-date")).toBeNull();
  });

  it("parseTimestampFromFileName parses glossary export filenames", () => {
    expect(
      parseTimestampFromFileName(
        "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx"
      )
    ).toBe(Date.parse("2026-08-12T15:36:29"));
  });

  it("parseTimestampFromFileName parses search export filenames", () => {
    expect(
      parseTimestampFromFileName(
        "admin_basic_2026-08-12_12-47-19.123.csv"
      )
    ).toBe(Date.parse("2026-08-12T12:47:19.123"));
  });

  it("resolveDownloadRecordTime prefers API createdTime over filename", () => {
    expect(
      resolveDownloadRecordTime({
        fileName: "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx",
        createdTime: 1_700_000_000_000
      })
    ).toBe(1_700_000_000_000);
  });

  it("resolveDownloadRecordTime falls back to filename timestamp", () => {
    expect(
      resolveDownloadRecordTime({
        fileName: "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx"
      })
    ).toBe(Date.parse("2026-08-12T15:36:29"));
  });

  it("sortDownloadRecordsByLatest orders newest first using createdTime", () => {
    const sorted = sortDownloadRecordsByLatest([
      {
        fileName: "older.csv",
        source: "search",
        createdTime: 1_000
      },
      {
        fileName: "newer.csv",
        source: "search",
        createdTime: 2_000
      }
    ]);

    expect(sorted.map((record) => record.fileName)).toEqual([
      "newer.csv",
      "older.csv"
    ]);
  });

  it("sortDownloadRecordsByLatest orders newest first using filename timestamps", () => {
    const sorted = sortDownloadRecordsByLatest([
      {
        fileName: "admin_GLOSSARY_EXPORT_2026-08-12_12-11-41.csv",
        source: "glossary"
      },
      {
        fileName: "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx",
        source: "glossary"
      }
    ]);

    expect(sorted.map((record) => record.fileName)).toEqual([
      "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx",
      "admin_GLOSSARY_EXPORT_2026-08-12_12-11-41.csv"
    ]);
  });

  it("extractDownloadRecords reads glossary records from searchDownloadRecords fallback", () => {
    const records = extractDownloadRecords(
      {
        searchDownloadRecords: [
          {
            fileName: "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx",
            createdTime: 100
          }
        ]
      },
      "glossary"
    );

    expect(records).toEqual([
      {
        fileName: "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx",
        source: "glossary",
        createdTime: 100,
        status: undefined
      }
    ]);
  });

  it("mergeDownloadRecords combines and sorts search and glossary records", () => {
    const merged = mergeDownloadRecords(
      [
        {
          fileName: "admin_basic_2026-08-12_12-47-19.csv",
          source: "search"
        }
      ],
      [
        {
          fileName: "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx",
          source: "glossary"
        }
      ]
    );

    expect(merged.map((record) => record.fileName)).toEqual([
      "admin_GLOSSARY_EXPORT_2026-08-12_15-36-29.xlsx",
      "admin_basic_2026-08-12_12-47-19.csv"
    ]);
  });

  it("dedupeDownloadRecordsByFileName keeps the newest duplicate fileName", () => {
    const deduped = dedupeDownloadRecordsByFileName([
      {
        fileName: "duplicate.csv",
        source: "search",
        createdTime: 100
      },
      {
        fileName: "duplicate.csv",
        source: "glossary",
        createdTime: 200
      }
    ]);

    expect(deduped).toEqual([
      {
        fileName: "duplicate.csv",
        source: "glossary",
        createdTime: 200
      }
    ]);
  });

  it("filterDownloadRecordsForToggle returns all files when showing all exports", () => {
    const records = [
      {
        fileName: "ready.csv",
        source: "search" as const,
        status: "COMPLETE"
      },
      {
        fileName: "pending.csv",
        source: "search" as const,
        status: "PENDING"
      }
    ];

    expect(filterDownloadRecordsForToggle(records, true)).toEqual(records);
  });

  it("filterDownloadRecordsForToggle hides pending files when showing completed only", () => {
    const records = [
      {
        fileName: "ready.csv",
        source: "search" as const,
        status: "COMPLETE"
      },
      {
        fileName: "pending.csv",
        source: "search" as const,
        status: "PENDING"
      }
    ];

    expect(filterDownloadRecordsForToggle(records, false)).toEqual([
      {
        fileName: "ready.csv",
        source: "search",
        status: "COMPLETE"
      }
    ]);
  });
});
