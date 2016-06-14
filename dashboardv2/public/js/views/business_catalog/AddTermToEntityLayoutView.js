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
    'utils/Messages'
], function(require, Backbone, AddTermToEntityLayoutViewTmpl, Utils, Modal, VCatalogList, Messages) {
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
                _.extend(this, _.pick(options, 'guid', 'modalCollection', 'callback'));
                this.vCatalogList = new VCatalogList();
                var that = this;
                this.modal = new Modal({
                    title: 'Add Term',
                    content: this,
                    okText: 'Save',
                    cancelText: "Cancel",
                    allowCancel: true,
                }).open();
                this.on('ok', function() {
                    that.saveTermToAsset();
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
            },
            saveTermToAsset: function() {
                var that = this;
                var VCatalog = new this.vCatalogList.model();
                var termName = this.modal.$el.find('.taxonomyTree li.active a').data('name').split("`").join("");
                VCatalog.url = function() {
                    return "api/atlas/v1/entities/" + that.guid + "/tags/" + termName;
                };
                VCatalog.save(null, {
                    beforeSend: function() {},
                    success: function(data) {
                        Utils.notifySuccess({
                            content: "Term " + termName + Messages.addTermToEntitySuccessMessage
                        });
                        if (that.callback) {
                            that.callback();
                        }
                        if (that.modalCollection) {
                            that.modalCollection.fetch({ reset: true });
                        }
                    },
                    error: function(error, data, status) {
                        if (data && data.responseText) {
                            var data = JSON.parse(data.responseText);
                            Utils.notifyError({
                                content: data.messages
                            });
                        }
                    },
                    complete: function() {}
                });

            }
        });
    return AddTermToEntityLayoutView;

});
