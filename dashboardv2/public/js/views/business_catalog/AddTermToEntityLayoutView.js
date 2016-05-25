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
    'collection/VCatalogList'
], function(require, Backbone, AddTermToEntityLayoutViewTmpl, Utils, Modal, VCatalogList) {
    'use strict';

    var AddTermToEntityLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends AddTermToEntityLayoutView */
        {
            _viewName: 'AddTermToEntityLayoutView',

            template: AddTermToEntityLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                termName: '[data-id="termName"]',
                addTermOptions: '[data-id="addTermOptions"]'
            },
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
                this.fetchTaxonomy = true;
                this.bindEvents();
            },
            bindEvents: function() {
                this.listenTo(this.vCatalogList, 'reset', function() {
                    var url = "",
                        that = this;
                    _.each(this.vCatalogList.models, function(obj) {
                        if (that.fetchTaxonomy && obj.get('href').search("terms") == -1) {
                            url = obj.get('href');
                            that.fetchTaxonomy = false;
                        }
                    });
                    if (url.length == 0) {
                        this.generateTerm();
                    } else {
                        url = "/api" + url.split("/api")[1] + "/terms";
                        this.fetchTerms(url);
                    }
                }, this);
            },
            onRender: function() {
                this.fetchTerms();
            },
            fetchTerms: function(url) {
                if (url) {
                    this.vCatalogList.url = url;
                }
                this.vCatalogList.fetch({ reset: true });
            },
            generateTerm: function() {
                var terms = '<option selected="selected" disabled="disabled">-- Select Term --</option>';
                _.each(this.vCatalogList.fullCollection.models, function(obj, key) {
                    terms += '<option value="' + obj.get('name') + '">' + obj.get('name') + '</option>';
                });
                this.ui.addTermOptions.html(terms);
            },
            saveTermToAsset: function() {
                var that = this;
                var VCatalog = new this.vCatalogList.model();
                VCatalog.url = function() {
                    return "api/atlas/v1/entities/" + that.guid + "/tags/" + that.ui.addTermOptions.val();
                }
                VCatalog.save(null, {
                    beforeSend: function() {},
                    success: function(data) {
                        Utils.notifySuccess({
                            content: "Term " + that.ui.addTermOptions.val() + " has been added to entity"
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
                                content: data.message
                            });
                        }
                    },
                    complete: function() {}
                });

            }
        });
    return AddTermToEntityLayoutView;

});
