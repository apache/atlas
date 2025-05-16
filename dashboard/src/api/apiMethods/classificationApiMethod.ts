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
  addTagUrl,
  deleteTagUrl,
  editAssignTagUrl,
  removeClassificationUrl,
  rootClassificationDefUrl
} from "../apiUrlLinks/classificationUrl";
import { _delete, _get, _post, _put } from "./apiMethod";

const removeClassification = (guid: string, classficationName: string) => {
  return _delete(removeClassificationUrl(guid, classficationName), {
    method: "DELETE",
    params: {}
  });
};

const addTag = (params: any) => {
  return _post(addTagUrl(), { method: "POST", params: {}, data: params });
};

const deleteClassification = (tagName: string) => {
  return _delete(deleteTagUrl(tagName), {
    method: "DELETE",
    params: {}
  });
};

const editAssignTag = (guid: string, params: any) => {
  return _put(editAssignTagUrl(guid), {
    method: "PUT",
    params: {},
    data: params
  });
};

const getRootClassificationDef = (name: string) => {
  const config = {
    method: "GET",
    params: {}
  };
  return _get(rootClassificationDefUrl(name), config);
};

export {
  removeClassification,
  addTag,
  deleteClassification,
  editAssignTag,
  getRootClassificationDef
};
