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
    'hbs!tmpl/entity/EntityDetailTableLayoutView_tmpl',
], function(require, Backbone, EntityDetailTableLayoutView_tmpl) {
    'use strict';

    var EntityDetailTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends EntityDetailTableLayoutView */
        {
            _viewName: 'EntityDetailTableLayoutView',

            template: EntityDetailTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                detailValue: "[data-id='detailValue']",
            },
            /** ui events hash */
            events: function() {
                var events = {};
                return events;
            },
            /**
             * intialize a new EntityDetailTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'collection'));
                this.collectionObject = this.collection.toJSON();
                this.entityModel = new this.collection.model();
            },
            bindEvents: function() {},
            onRender: function() {
                this.entityTableGenerate();
            },
            entityTableGenerate: function() {
                var that = this,
                    table = "",
                    valueObject = this.collectionObject[0].values;
                _.keys(valueObject).map(function(key) {
                    /*  if (key == 'columns')
                          return;*/
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
                that.ui.detailValue.append(table);
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

                        that.$('td div[data-id="' + data.GUID + '"]').html('<a href="#!/dashboard/detailPage/' + data.GUID + '">' + value + '</a>');
                    },
                    error: function(error, data, status) {},
                    complete: function() {}
                });
            }
        });
    return EntityDetailTableLayoutView;
});
