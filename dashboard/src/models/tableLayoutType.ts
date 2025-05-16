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

import { Cell, ColumnDef, Row } from "@tanstack/react-table";

export interface TableProps {
  fetchData?: any;
  data: any[];
  columns: ColumnDef<any>[];
  isFetching?: boolean;
  skeletonCount?: number;
  skeletonHeight?: number;
  headerComponent?: JSX.Element;
  pageCount?: number;
  defaultColumnVisibility?: any;
  page?: (page: number) => void;
  onClickRow?: (cell: Cell<any, unknown>, row: Row<any>) => void;
  emptyText?: string;
  children?: React.ReactNode | React.ReactElement;
  handleRow?: () => void;
  defaultColumnParams?: string;
  columnVisibility: boolean;
  currentpageIndex?: number;
  currentpageSize?: number;
  refreshTable?: () => void;
  defaultSortCol?: any;
  clientSideSorting?: boolean;
  columnSort: boolean;
  showPagination: boolean;
  showRowSelection: boolean;
  tableFilters?: boolean;
  expandRow?: boolean;
  auditTableDetails?: any;
  assignFilters?: { classifications: boolean; term: boolean } | null;
  queryBuilder?: boolean;
  allTableFilters?: boolean;
  columnVisibilityParams?: boolean;
  setUpdateTable?: any;
  isfilterQuery?: any;
  isClientSidePagination?: boolean;
}
