/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define(['require',
    'backbone',
    'hbs!tmpl/audit/CreateAuditTableLayoutView_tmpl',
    'collection/VEntityList'
], function(require, Backbone, CreateAuditTableLayoutViewTmpl, VEntityList) {
    'use strict';

    var CreateAuditTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends CreateAuditTableLayoutView */
        {
            _viewName: 'CreateAuditTableLayoutView',

            template: CreateAuditTableLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                auditValue: "[data-id='auditValue']",
                auditCreate: "[data-id='auditCreate']",
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.auditCreate] = "onClickAuditCreate";
                return events;
            },
            /**
             * intialize a new AuditTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'guid'));
                this.entityCollection = new VEntityList();
                this.entityCollection.url = "/api/atlas/entities/" + this.guid + "/audit";
                this.entityCollection.modelAttrName = "events";
                this.entityModel = new this.entityCollection.model();
            },
            bindEvents: function() {
                this.listenTo(this.entityCollection, "reset", function(value) {
                    this.auditTableGenerate();
                }, this);
            },
            onRender: function() {
                this.entityCollection.fetch({ reset: true });
                this.bindEvents();
            },
            auditTableGenerate: function() {
                var that = this,
                    table = "";
                var collectionObject = this.entityCollection.models[0].toJSON();
                var appendedString = "{" + collectionObject.details + "}";
                var auditData = appendedString.split('"')[0].split(':')[0].split("{")[1];
                var detailsObject = JSON.parse(appendedString.replace("{" + auditData + ":", '{"' + auditData + '":'))[auditData];
                //Append string for JSON parse
                var valueObject = detailsObject.values;
                _.keys(valueObject).map(function(key) {
                    var keyValue = valueObject[key];
                    if (_.isArray(keyValue)) {
                        var subLink = "";
                        for (var i = 0; i < keyValue.length; i++) {
                            var inputOutputField = keyValue[i];
                            if (_.isObject(inputOutputField.id)) {
                                id = inputOutputField.id.id;
                            } else {
                                id = inputOutputField.id;
                            }
                            that.fetchInputOutputValue(id);
                            //var coma = (i = 0) ? ('') : (',');
                            subLink += '<div data-id="' + id + '"></div>';
                        }
                        table += '<tr><td>' + key + '</td><td>' + subLink + '</td></tr>';
                    } else if (_.isObject(keyValue)) {
                        var id = "";
                        if (_.isObject(keyValue.id)) {
                            id = keyValue.id.id;
                        } else {
                            id = keyValue.id;
                        }
                        that.fetchInputOutputValue(id);
                        table += '<tr><td>' + key + '</td><td><div data-id="' + id + '"></div></td></tr>';
                    } else {
                        if (key == "createTime" || key == "lastAccessTime" || key == "retention") {
                            table += '<tr><td>' + key + '</td><td>' + new Date(valueObject[key]) + '</td></tr>';
                        } else {
                            table += '<tr><td>' + key + '</td><td>' + valueObject[key] + '</td></tr>';
                        }

                    }
                });
                that.ui.auditValue.append(table);
            },
            fetchInputOutputValue: function(id) {
                var that = this;
                this.entityModel.getEntity(id, {
                    beforeSend: function() {},
                    success: function(data) {
                        var value = "";
                        if (data.definition.values.name) {
                            value = data.definition.values.name;
                        } else {
                            value = data.GUID;
                        }
                        that.$('td div[data-id="' + data.GUID + '"]').html('<a href="#!/detailPage/' + data.GUID + '">' + value + '</a>');
                    },
                    error: function(error, data, status) {},
                    complete: function() {}
                });
            }
        });
    return CreateAuditTableLayoutView;
});
