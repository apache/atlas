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

interface Entity {
  [key: string]: any;
}

export interface EntityDetailTabProps {
  entity: Entity;
  referredEntities?: Entity;
  loading?: boolean;
  tags?: any;
}

export type AuditTableType = {
  user: string;
  timestamp: number;
  action: string;
};

export type ProfileTableType = {
  name: string;
  owner: string;
  createTime: string;
};

export interface EntityState {
  entity: {
    loading: boolean;
    entityData: any;
  };
}
export interface ConfigType {
  [key: string]: string;
}

export interface DataModel {
  [key: string]: string | ConfigType;
}

export type State = {
  respData?: DataModel[] | undefined;
  isLoading: boolean;
  error?: string;
};

export type Action =
  | { type: "request" }
  | { type: "success"; respData: DataModel[] }
  | { type: "failure"; error: string };
