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
export interface EntityState {
  entity: {
    loading: boolean;
    entityData: any;
  };
}

export interface Params {
  attributes: string[];
  limit: number;
  offset: number;
  relationshipName: string | null;
  relationshipFilters: string | null;
}

export interface RowData {
  [key: string]: any;
}

export interface TableColumn<T extends Record<string, any>> {
  accessorFn: (row: T) => T[keyof T];
  accessorKey: keyof T;
  id: string;
  cell: (info: { getValue: () => T[keyof T] }) => JSX.Element;
  header: string;
  show: boolean;
}

export interface ExtendedTableColumn<T extends RowData> extends TableColumn<T> {
  enableSorting?: boolean;
}
