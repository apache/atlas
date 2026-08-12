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

import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import DialogShowMoreLess from "@components/DialogShowMoreLess";
import { LightTooltip } from "@components/muiComponents";
import GlossaryStatusBadge from "@views/Glossary/GlossaryStatusBadge";
import { entityStateReadOnly } from "@utils/Enum";
import {
  formatCustomAttributesForDisplay,
  formatCustomAttributesForTooltip,
  type GlossaryTableRow
} from "@utils/glossaryExport";
import type { CSSProperties } from "react";
import { ColumnDef } from "@tanstack/react-table";
import { Link } from "react-router-dom";

const COL_WIDTH_NAME = 200;
const COL_WIDTH_DESCRIPTION = 220;
const COL_WIDTH_STATUS = 76;

const ellipsisCellStyle: CSSProperties = {
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  display: "block",
  maxWidth: "100%"
};

const buildGlossaryDetailSearch = (row: GlossaryTableRow): string => {
  const params = new URLSearchParams();
  const gtype = row.recordType === "Term" ? "term" : "category";
  params.set("gid", row.glossaryGuid);
  params.set("gtype", gtype);
  params.set("viewType", gtype);
  params.set("fromView", "entity");
  if (row.recordType === "Term" && row.qualifiedName) {
    params.set("term", row.qualifiedName);
  }
  return params.toString();
};

const buildClassificationRowValue = (row: GlossaryTableRow) => ({
  guid: row.guid,
  status: row.status,
  classifications: (row.classifications ?? []).map((classification) => ({
    ...classification,
    entityGuid: classification.entityGuid ?? row.guid
  }))
});

const renderEllipsisText = (value: string, tooltip = value) => (
  <LightTooltip title={tooltip}>
    <span className="entity-name" style={ellipsisCellStyle}>
      {value}
    </span>
  </LightTooltip>
);

export const createGlossaryTermsListColumns = (
  setUpdateTable: (value: number) => void
): ColumnDef<GlossaryTableRow>[] => [
  {
    accessorKey: "glossaryName",
    header: "Glossary Name",
    cell: (info) => {
      const value = info.getValue() as string;
      if (!value) {
        return null;
      }
      return renderEllipsisText(value);
    },
    size: COL_WIDTH_NAME
  },
  {
    accessorKey: "name",
    header: "Name",
    cell: (info) => {
      const row = info.row.original;
      const name = row.name;
      if (!row.guid) {
        return <span>{name}</span>;
      }
      return (
        <div className="searchTableName">
          <LightTooltip title={name}>
            <Link
              className="entity-name nav-link text-decoration-none text-blue glossary-term-name-link"
              to={{
                pathname: `/glossary/${row.guid}`,
                search: `?${buildGlossaryDetailSearch(row)}`
              }}
              data-cy="glossaryTermNameLink"
            >
              {name}
            </Link>
          </LightTooltip>
        </div>
      );
    },
    size: COL_WIDTH_NAME,
    sortingFn: "alphanumeric"
  },
  {
    accessorKey: "recordType",
    header: "Record Type",
    cell: (info) => <span>{info.getValue() as string}</span>,
    size: 110
  },
  {
    accessorKey: "shortDescription",
    header: "Short Description",
    cell: (info) => {
      const value = info.getValue() as string;
      return value ? renderEllipsisText(value) : null;
    },
    size: COL_WIDTH_DESCRIPTION,
    enableSorting: false
  },
  {
    accessorKey: "longDescription",
    header: "Long Description",
    cell: (info) => {
      const value = info.getValue() as string;
      return value ? renderEllipsisText(value) : null;
    },
    size: COL_WIDTH_DESCRIPTION,
    enableSorting: false
  },
  {
    accessorKey: "classifications",
    header: "Classifications",
    cell: (info) => {
      const row = info.row.original;
      if (!row.guid) {
        return null;
      }

      const classificationValue = buildClassificationRowValue(row);
      const isTerm = row.recordType === "Term";
      const isReadOnly =
        !isTerm ||
        Boolean(row.status && entityStateReadOnly[row.status]);

      return (
        <DialogShowMoreLess
          value={classificationValue}
          readOnly={isReadOnly}
          setUpdateTable={setUpdateTable}
          columnVal="classifications"
          colName="Classification"
          displayText="typeName"
          removeApiMethod={isTerm ? removeClassification : undefined}
          isShowMoreLess={true}
        />
      );
    },
    enableSorting: false
  },
  {
    accessorKey: "customAttributes",
    header: "Custom Attributes",
    cell: (info) => {
      const value = info.row.original.customAttributes;
      if (!value || Object.keys(value).length === 0) {
        return null;
      }
      const displayText = formatCustomAttributesForDisplay(value);
      if (!displayText) {
        return null;
      }
      return renderEllipsisText(
        displayText,
        formatCustomAttributesForTooltip(value)
      );
    },
    size: 160,
    enableSorting: false
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: (info) => (
      <GlossaryStatusBadge status={info.getValue() as string} />
    ),
    size: COL_WIDTH_STATUS
  }
];

export default createGlossaryTermsListColumns;
