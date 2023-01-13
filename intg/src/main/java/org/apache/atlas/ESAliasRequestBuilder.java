/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasErrorCode.JSON_ERROR;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.ESAliasRequestBuilder.ESAliasAction.ADD;
import static org.apache.atlas.ESAliasRequestBuilder.ESAliasAction.REMOVE;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class ESAliasRequestBuilder {
    private List<JSONObject> jActionObject = new ArrayList<>();

    public static enum ESAliasAction {
        ADD("add"),
        REMOVE("remove");

        String type;

        ESAliasAction(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    public ESAliasRequestBuilder addAction(ESAliasAction eSAliasAction, AliasAction action) throws AtlasBaseException {
        JSONObject object = new JSONObject();
        String type = "";

        try {
            switch (eSAliasAction) {
                case ADD:
                    type = ADD.getType();
                    object.put("index", action.getIndex());
                    object.put("alias", action.getAlias());
                    if (action.getFilter() != null) {
                        object.put("filter", new JSONObject(AtlasType.toJson(action.getFilter())));
                    }
                    break;

                case REMOVE:
                    type = REMOVE.getType();
                    object.put("index", action.getIndex());
                    object.put("alias", action.getAlias());
                    break;

                default:
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, String.format("Action %s is not supported by ENUM ESAliasAction", eSAliasAction));
            }

            JSONObject j_1 = new JSONObject();
            j_1.put(type, object);

            jActionObject.add(j_1);

        } catch (JSONException e) {
            throw new AtlasBaseException(JSON_ERROR, e.getMessage());
        }
        return this;
    }

    public String build() throws AtlasBaseException {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put("actions", jActionObject);
        } catch (JSONException e) {
            throw new AtlasBaseException(JSON_ERROR, e.getMessage());
        }

        return jsonObject.toString();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class AliasAction {
        private String index;
        private String alias;
        Map<String, Object> filter;

        public AliasAction(String index, String alias, Map<String, Object> filter) {
            this.index = index;
            this.alias = alias;
            this.filter = filter;
        }

        public AliasAction(String index, String alias) {
            this(index, alias, null);
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public Map<String, Object> getFilter() {
            return filter;
        }

        public void setFilter(Map<String, Object> filter) {
            this.filter = filter;
        }
    }
}
