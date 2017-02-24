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
    'utils/CommonViewFunction',
    'models/VEntity',
], function(require, Backbone, EntityDetailTableLayoutView_tmpl, CommonViewFunction, VEntity) {
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
                _.extend(this, _.pick(options, 'entity', 'referredEntities', 'typeHeaders'));
                this.entityModel = new VEntity({});
            },
            bindEvents: function() {},
            onRender: function() {
                this.entityTableGenerate();
            },
            entityTableGenerate: function() {
                var that = this,
                    attributeObject = this.entity.attributes;
                CommonViewFunction.findAndmergeRefEntity(attributeObject, that.referredEntities);
                if (attributeObject && attributeObject.columns) {
                    var valueSorted = _.sortBy(attributeObject.columns, function(val) {
                        return val.attributes.position
                    });
                    attributeObject.columns = valueSorted;
                }
                var table = CommonViewFunction.propertyTable(attributeObject, this);
                that.ui.detailValue.append(table);
            }
        });
    return EntityDetailTableLayoutView;
});
