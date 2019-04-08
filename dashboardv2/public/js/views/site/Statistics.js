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
    'hbs!tmpl/site/Statistics_tmpl',
    'modules/Modal',
    'models/VCommon',
    'utils/UrlLinks',
    'collection/VTagList',
    'utils/CommonViewFunction'
], function(require, Backbone, StatTmpl, Modal, VCommon, UrlLinks, VTagList, CommonViewFunction) {
    'use strict';

    var StatisticsView = Backbone.Marionette.LayoutView.extend(
        /** @lends AboutAtlasView */
        {
            template: StatTmpl,
            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                entityActive: "[data-id='entityActive'] tbody",
                entityDelete: "[data-id='entityDelete'] tbody",
                entityActiveHeader: "[data-id='entityActive'] .count",
                entityDeletedHeader: "[data-id='entityDelete'] .count"
            },
            /** ui events hash */
            events: function() {},
            /**
             * intialize a new AboutAtlasView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, options);
                var modal = new Modal({
                    title: 'Statistics',
                    content: this,
                    okCloses: true,
                    showFooter: true,
                    allowCancel: false
                }).open();

                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            bindEvents: function() {},
            onRender: function() {
                var that = this;
                var entityCountCollection = new VTagList();
                entityCountCollection.url = UrlLinks.entityCountApi();
                entityCountCollection.modelAttrName = "data";
                entityCountCollection.fetch({
                    success: function(data) {
                        var data = _.first(data.toJSON()),
                            no_records = '<tr class="empty text-center"><td colspan="2"><span>No records found!</span></td></tr>',
                            activeEntityTable = _.isEmpty(data.entity.entityActive) ? no_records : that.getTable({ valueObject: data.entity.entityActive }),
                            deleteEntityTable = _.isEmpty(data.entity.entityDeleted) ? no_records : that.getTable({ valueObject: data.entity.entityDeleted });
                        var totalActive = 0,
                            totalDeleted = 0;
                        if (data.entity && data.general.entityCount) {
                            totalActive = data.general.entityCount;
                        }
                        if (data.entity && data.entity.entityDeleted) {
                            _.each(data.entity.entityDeleted, function(val) {
                                totalDeleted += val;
                            });
                        }
                        that.ui.entityActive.html(activeEntityTable);
                        that.ui.entityDelete.html(deleteEntityTable);
                        that.ui.entityActiveHeader.html("&nbsp;(" + _.numberFormatWithComa((totalActive - totalDeleted)) + ")");
                        that.ui.entityDeletedHeader.html("&nbsp;(" + _.numberFormatWithComa(totalDeleted) + ")");
                    }
                });
            },
            getTable: function(obj) {
                return CommonViewFunction.propertyTable(_.extend({ scope: this, formatIntVal: true }, obj))
            }
        });
    return StatisticsView;
});