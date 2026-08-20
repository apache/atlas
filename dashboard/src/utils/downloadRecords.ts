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

export type DownloadSource = "search" | "glossary";

export type DownloadRecord = {
  fileName: string;
  source: DownloadSource;
  createdTime?: number | string;
  status?: string;
};

type RawDownloadRecord = {
  fileName?: string;
  createdTime?: number | string;
  status?: string;
};

const FILE_NAME_TIMESTAMP_PATTERN =
  /(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2}(?:\.\d{3})?)/;

export const parseCreatedTime = (
  createdTime?: number | string
): number | null => {
  if (createdTime === undefined || createdTime === null || createdTime === "") {
    return null;
  }

  if (typeof createdTime === "number" && Number.isFinite(createdTime)) {
    return createdTime;
  }

  const parsed = Date.parse(String(createdTime));
  return Number.isNaN(parsed) ? null : parsed;
};

export const parseTimestampFromFileName = (fileName: string): number | null => {
  const match = fileName.match(FILE_NAME_TIMESTAMP_PATTERN);
  if (!match) {
    return null;
  }

  const normalized = `${match[1]}T${match[2]}:${match[3]}:${match[4]}`;
  const parsed = Date.parse(normalized);
  return Number.isNaN(parsed) ? null : parsed;
};

export const resolveDownloadRecordTime = (record: {
  fileName: string;
  createdTime?: number | string;
}): number => {
  const fromApi = parseCreatedTime(record.createdTime);
  if (fromApi !== null) {
    return fromApi;
  }

  const fromFileName = parseTimestampFromFileName(record.fileName);
  return fromFileName ?? 0;
};

export const sortDownloadRecordsByLatest = <T extends DownloadRecord>(
  records: T[]
): T[] => {
  return [...records].sort(
    (left, right) =>
      resolveDownloadRecordTime(right) - resolveDownloadRecordTime(left)
  );
};

export const dedupeDownloadRecordsByFileName = (
  records: DownloadRecord[]
): DownloadRecord[] => {
  const byFileName = new Map<string, DownloadRecord>();

  records.forEach((record) => {
    const existing = byFileName.get(record.fileName);
    if (
      !existing ||
      resolveDownloadRecordTime(record) > resolveDownloadRecordTime(existing)
    ) {
      byFileName.set(record.fileName, record);
    }
  });

  return sortDownloadRecordsByLatest(Array.from(byFileName.values()));
};

export const filterDownloadRecordsForToggle = (
  records: DownloadRecord[],
  showAllFiles: boolean
): DownloadRecord[] => {
  if (showAllFiles) {
    return records;
  }

  return records.filter((record) => record.status !== "PENDING");
};

export const isPendingDownloadRecord = (record: DownloadRecord): boolean =>
  record.status === "PENDING";

export const extractDownloadRecords = (
  data: Record<string, unknown> | undefined,
  source: DownloadSource
): DownloadRecord[] => {
  const records =
    source === "glossary"
      ? ((data?.glossaryDownloadRecords ??
          data?.searchDownloadRecords ??
          []) as RawDownloadRecord[])
      : ((data?.searchDownloadRecords ?? []) as RawDownloadRecord[]);

  return records
    .filter((record) => Boolean(record?.fileName))
    .map((record) => ({
      fileName: record.fileName as string,
      source,
      createdTime: record.createdTime,
      status: record.status
    }));
};

export const mergeDownloadRecords = (
  searchRecords: DownloadRecord[],
  glossaryRecords: DownloadRecord[]
): DownloadRecord[] => {
  return dedupeDownloadRecordsByFileName([
    ...searchRecords,
    ...glossaryRecords
  ]);
};
