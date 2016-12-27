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
    'utils/Globals',
    'utils/CommonViewFunction'
], function(require, Backbone, CreateAuditTableLayoutViewTmpl, Globals, CommonViewFunction) {
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
                noData: "[data-id='noData']",
                tableAudit: "[data-id='tableAudit']",
                auditHeaderValue: "[data-id='auditHeaderValue']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.auditCreate] = "onClickAuditCreate";
                return events;
            },
            /**
             * intialize a new CreateAuditTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'guid', 'entityModel', 'action'));
            },
            bindEvents: function() {},
            onRender: function() {
                this.auditTableGenerate();
            },
            auditTableGenerate: function() {
                var that = this,
                    table = "";
                if (this.entityModel.get('details').search('{') >= 0) {
                    var appendedString = "{" + this.entityModel.get('details') + "}";
                    var auditData = appendedString.split('"')[0].split(':')[0].split("{")[1];
                    var detailsObject = JSON.parse(appendedString.replace("{" + auditData + ":", '{"' + auditData + '":'))[auditData];
                    //Append string for JSON parse
                    var valueObject = detailsObject.values;
                    if (this.action == Globals.auditAction.TAG_ADD) {
                        this.ui.auditHeaderValue.html('<th>Tag</th>');
                        this.ui.auditValue.html("<tr><td>" + _.escape(detailsObject.typeName) + "</td></tr>");
                    } else {
                        this.ui.auditHeaderValue.html('<th>Key</th><th>New Value</th>');
                        table = CommonViewFunction.propertyTable(valueObject, this);
                        if (table.length) {
                            this.ui.noData.hide();
                            this.ui.tableAudit.show();
                            this.ui.auditValue.html(table);
                        } else {
                            this.ui.noData.show();
                            this.ui.tableAudit.hide();
                        }
                    }
                } else if (this.action == Globals.auditAction.TAG_DELETE) {
                    var appendedString = this.entityModel.get('details').split(':');
                    this.ui.auditHeaderValue.html('<th>Tag</th>');
                    this.ui.auditValue.html("<tr><td>" + _.escape(appendedString[1]) + "</td></tr>");
                }

            },
        });
    return CreateAuditTableLayoutView;
});
