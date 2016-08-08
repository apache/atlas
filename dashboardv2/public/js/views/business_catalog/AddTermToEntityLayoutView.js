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
    'hbs!tmpl/business_catalog/AddTermToEntityLayoutView_tmpl',
    'utils/Utils',
    'modules/Modal',
    'collection/VCatalogList',
    'utils/CommonViewFunction',
    'utils/Messages'
], function(require, Backbone, AddTermToEntityLayoutViewTmpl, Utils, Modal, VCatalogList, CommonViewFunction, Messages) {
    'use strict';

    var AddTermToEntityLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends AddTermToEntityLayoutView */
        {
            _viewName: 'AddTermToEntityLayoutView',

            template: AddTermToEntityLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RTreeLayoutView: "#r_treeLayoutView"
            },
            /** ui selector cache */
            ui: {},
            /** ui events hash */
            events: function() {
                var events = {};
                return events;
            },
            /**
             * intialize a new AddTermToEntityLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'modalCollection', 'callback', 'multiple', 'showLoader'));
                this.vCatalogList = new VCatalogList();
                var that = this;
                this.modal = new Modal({
                    title: 'Assign Term',
                    content: this,
                    okText: 'Assign',
                    cancelText: "Cancel",
                    allowCancel: true,
                }).open();
                this.on('ok', function() {
                    that.asyncFetchCounter = 0;
                    if (that.multiple) {
                        for (var i = 0; i < that.multiple.length; i++) {
                            if (i == 0) {
                                that.showLoader();
                            }
                            var obj = {
                                termName: this.modal.$el.find('.taxonomyTree li.active a').data('name').split("`").join(""),
                                guid: that.multiple[i].id.id
                            }
                            CommonViewFunction.saveTermToAsset(obj, that);
                        }
                    } else {
                        that.asyncFetchCounter = 0;
                        CommonViewFunction.saveTermToAsset({
                            termName: this.modal.$el.find('.taxonomyTree li.active a').data('name').split("`").join(""),
                            guid: this.guid
                        }, that);
                    }
                });
                this.on('closeModal', function() {
                    this.modal.trigger('cancel');
                });
            },
            onRender: function() {
                this.renderTreeLayoutView();
            },
            renderTreeLayoutView: function() {
                var that = this;
                require(['views/business_catalog/TreeLayoutView'], function(TreeLayoutView) {
                    that.RTreeLayoutView.show(new TreeLayoutView({
                        url: that.url,
                        viewBased: false
                    }));
                });
            }
        });
    return AddTermToEntityLayoutView;
});
