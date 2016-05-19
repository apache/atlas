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
    'hbs!tmpl/business_catalog/BusinessCatalogDetailLayoutView_tmpl',
    'utils/Utils',
    'collection/VCatalogList',
    'models/VEntity',
], function(require, Backbone, BusinessCatalogDetailLayoutViewTmpl, Utils, VCatalogList, VEntity) {
    'use strict';

    var BusinessCatalogDetailLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends BusinessCatalogDetailLayoutView */
        {
            _viewName: 'BusinessCatalogDetailLayoutView',

            template: BusinessCatalogDetailLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                REntityDetailTableLayoutView: "#r_entityDetailTableLayoutView",
                RSchemaTableLayoutView: "#r_schemaTableLayoutView",
                RTagTableLayoutView: "#r_tagTableLayoutView",
                RLineageLayoutView: "#r_lineageLayoutView",
            },
            /** ui selector cache */
            ui: {
                title: '[data-id="title"]',
                editButton: '[data-id="editButton"]',
                cancelButton: '[data-id="cancelButton"]',
                publishButton: '[data-id="publishButton"]',
                description: '[data-id="description"]',
                descriptionTextArea: '[data-id="descriptionTextArea"]',
                editBox: '[data-id="editBox"]',
                createDate: '[data-id="createDate"]',
                updateDate: '[data-id="updateDate"]',
                createdUser: '[data-id="createdUser"]',
                addTagBtn: '[data-id="addTagBtn"]',
                appendList: '[data-id="appendList"]',
                inputTagging: '[data-id="inputTagging"]',
                deleteTag: '[data-id="deleteTag"]',
                addTagtext: '[data-id="addTagtext"]',
                addTagPlus: '[data-id="addTagPlus"]',
                searchTag: '[data-id="searchTag"] input',
                addTagListBtn: '[data-id="addTagListBtn"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.editButton] = function() {
                    this.ui.editButton.hide();
                    this.ui.description.hide();
                    this.ui.editBox.show();
                    this.ui.descriptionTextArea.focus();
                    if (this.ui.description.text().length) {
                        this.ui.descriptionTextArea.val(this.ui.description.text());
                    }
                };
                events["click " + this.ui.cancelButton] = 'onCancelButtonClick';
                return events;
            },
            /**
             * intialize a new BusinessCatalogDetailLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'url', 'collection'));
                this.bindEvents();
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, 'error', function(model, response) {
                    this.$('.fontLoader').hide();
                    if (response && response.responseJSON && response.responseJSON.message) {
                        Utils.notifyError({
                            content: response.responseJSON.message
                        });
                    }

                }, this);
                this.listenTo(this.collection, 'reset', function() {
                    this.$('.fontLoader').hide();
                    this.$('.hide').removeClass('hide');
                    this.model = this.collection.first();
                    var name = this.model.get('name'),
                        description = this.model.get('description'),
                        createdDate = this.model.get('creation_time');
                    if (name) {
                        this.ui.title.show();
                        this.ui.title.html('<span>' + name + '</span>');
                    } else {
                        this.ui.title.hide();
                    }
                    if (description) {
                        this.ui.description.show();
                        this.ui.description.html('<span>' + description + '</span>');
                    } else {
                        this.ui.description.hide();
                    }
                    if (createdDate) {
                        this.ui.createDate.html('<strong> Date Created: </strong> ' + new Date(createdDate));
                    }
                }, this);
            },
            onRender: function() {
                var that = this;
                this.$('.fontLoader').show();
                this.ui.editBox.hide();
            },
            fetchCollection: function() {
                this.$('.fontLoader').show();
                this.collection.fetch({ reset: true });
            },

            onCancelButtonClick: function() {
                this.ui.description.show();
                this.ui.editButton.show();
                this.ui.editBox.hide();
            },
            addTagCollectionList: function(obj, searchString) {
                var list = "",
                    that = this;
                _.each(obj, function(model) {
                    var tags = model.get("tags");
                    if (!_.contains(that.tagElement, tags)) {
                        if (searchString) {
                            if (tags.search(new RegExp(searchString, "i")) != -1) {
                                list += '<div><span>' + tags + '</span></div>';
                                return;
                            }
                        } else {
                            list += '<div><span>' + tags + '</span></div>';
                        }
                    }
                });
                if (list.length <= 0) {
                    list += '<div><span>' + "No more tags" + '</span></div>';
                }
                this.ui.appendList.html(list);
            },
            addTagToTerms: function(tagObject) {
                var tagData = "";
                _.each(tagObject, function(val) {
                    tagData += '<span class="inputTag">' + val + '<i class="fa fa-close" data-id="deleteTag"></i></span>';
                });
                this.$('.addTag-dropdown').before(tagData);
            },
            saveTagFromList: function(ref) {
                var that = this;
                this.entityModel = new VEntity();
                var tagName = ref.text();
                var json = {
                    "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Struct",
                    "typeName": tagName,
                    "values": {}
                };
                this.entityModel.saveEntity(this.id, {
                    data: JSON.stringify(json),
                    beforeSend: function() {},
                    success: function(data) {
                        that.collection.fetch({ reset: true });
                    },
                    error: function(error, data, status) {
                        if (error && error.responseText) {
                            var data = JSON.parse(error.responseText);
                        }
                    },
                    complete: function() {}
                });
            },
            offlineSearchTag: function(e) {
                this.addTagCollectionList(this.tagCollection.fullCollection.models, $(e.currentTarget).val());
            },
            onClickAddTagBtn: function() {
                this.ui.searchTag.val("");
                this.offlineSearchTag(this.ui.searchTag[0]);
            }
        });
    return BusinessCatalogDetailLayoutView;

});
