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
export interface TypeHeaderInterface {
  serviceType: string;
  category: string;
  name: string;
  guid: string;
}

export interface TypedefHeaderDataType {
  loading?: boolean;
  typeHeaderData: TypeHeaderInterface[];
}

export interface ChildrenInterface {
  nodeName?: string;
  children?: any;
  gType?: string;
  guid?: string;
  id?: string;
  name?: string;
  text?: string;
  types?: string;
  type?: any;
  parent?: string;
  cGuid?: string;
  serviceType?: string;
}

export interface ChildrenInterfaces extends ChildrenInterface {}

export interface ServiceTypeInterface {
  [key: string]: {
    children: ChildrenInterface[];
    name: string;
    totalCount?: number;
    types?: string;
  };
}

export interface ServiceTypeFlatInterface {
  text: string;
  name: string;
  type: string;
  gType: string;
  guid: string;
  id: string;
}

export type ServiceTypeArrType<T extends boolean> = T extends true
  ? ServiceTypeInterface[]
  : ServiceTypeFlatInterface[];
