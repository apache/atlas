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
    'hbs!tmpl/glossary/AssignTermLayoutView_tmpl',
    'utils/Utils',
    'utils/UrlLinks',
    'modules/Modal'
], function(require, Backbone, AssignTermLayoutViewTmpl, Utils, UrlLinks, Modal) {

    var AssignTermLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends AssignTermLayoutView */
        {
            _viewName: 'AssignTermLayoutView',

            template: AssignTermLayoutViewTmpl,

            templateHelpers: function() {
                return {};
            },

            /** Layout sub regions */
            regions: {
                RGlossaryTree: "#r_glossaryTree"
            },

            /** ui selector cache */
            ui: {},
            /** ui events hash */
            events: function() {
                var events = {};
                return events;
            },
            /**
             * intialize a new AssignTermLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'glossaryCollection', 'guid', 'callback', 'hideLoader', 'isCategoryView', 'categoryData', 'isTermView', 'termData'));
                var that = this;
                this.options = options;
                if (!this.isCategoryView && !this.isTermView) {
                    this.isEntityView = true;
                }
                this.glossary = {
                    selectedItem: {}
                }
                var title = "";
                if (this.isCategoryView || this.isEntityView) {
                    title = ("Assign term to " + (this.isCategoryView ? "Category" : "entity"))
                } else {
                    title = "Assign Category to term";
                }
                this.modal = new Modal({
                    "title": title,
                    "content": this,
                    "cancelText": "Cancel",
                    "okCloses": false,
                    "okText": "Assign",
                    "allowCancel": true
                });
                this.modal.open();
                this.modal.on('closeModal', function() {
                    that.modal.trigger('cancel');
                    if (that.assignTermError && that.hideLoader) {
                        that.hideLoader();
                    }
                    if (options.onModalClose) {
                        options.onModalClose()
                    }
                });
                this.modal.on('ok', function() {
                    that.assignTerm();
                });
            },
            bindEvents: function() {},
            onRender: function() {
                this.renderGlossaryTree();
            },
            assignTerm: function() {
                this.assignTermError = false;
                var that = this,
                    data = [],
                    selectedItem = this.glossary.selectedItem,
                    selectedGuid = selectedItem.guid,
                    ajaxOptions = {
                        success: function(rModel, response) {
                            Utils.notifySuccess({
                                content: (that.isCategoryView || that.isEntityView ? "Term" : "Category") + " is associated successfully "
                            });
                            that.modal.trigger('closeModal');
                            if (that.callback) {
                                that.callback();
                            }
                        },
                        cust_error: function() {
                            that.assignTermError = true;
                        }
                    },
                    model = new this.glossaryCollection.model();
                if (this.isCategoryView) {
                    data = _.extend({}, this.categoryData);
                    if (data.terms) {
                        data.terms.push({ "termGuid": selectedGuid });
                    } else {
                        data.terms = [{ "termGuid": selectedGuid }];
                    }
                    model.assignTermToCategory(_.extend(ajaxOptions, { data: JSON.stringify(data), guid: data.guid }));
                } else if (this.isTermView) {
                    data = _.extend({}, this.termData);
                    if (data.categories) {
                        data.categories.push({ "categoryGuid": selectedGuid });
                    } else {
                        data.categories = [{ "categoryGuid": selectedGuid }];
                    }
                    model.assignCategoryToTerm(_.extend(ajaxOptions, { data: JSON.stringify(data), guid: data.guid }));
                } else {
                    data.push({ "guid": that.guid });
                    model.assignTermToEntity(selectedGuid, _.extend(ajaxOptions, { data: JSON.stringify(data) }));
                }
            },
            renderGlossaryTree: function() {
                var that = this;
                require(['views/glossary/GlossaryLayoutView'], function(GlossaryLayoutView) {
                    that.RGlossaryTree.show(new GlossaryLayoutView(_.extend({
                        "isAssignTermView": that.isCategoryView,
                        "isAssignCategoryView": that.isTermView,
                        "isAssignEntityView": that.isEntityView,
                        "glossary": that.glossary
                    }, that.options)));
                });
            },
        });
    return AssignTermLayoutView;
});