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
    'hbs!tmpl/detail_page/DetailPageLayoutView_tmpl',
    'utils/Utils',
    'collection/VTagList',
    'models/VEntity',
    'utils/CommonViewFunction',
    'utils/Globals'
], function(require, Backbone, DetailPageLayoutViewTmpl, Utils, VTagList, VEntity, CommonViewFunction, Globals) {
    'use strict';

    var DetailPageLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends DetailPageLayoutView */
        {
            _viewName: 'DetailPageLayoutView',

            template: DetailPageLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                REntityDetailTableLayoutView: "#r_entityDetailTableLayoutView",
                RSchemaTableLayoutView: "#r_schemaTableLayoutView",
                RTagTableLayoutView: "#r_tagTableLayoutView",
                RLineageLayoutView: "#r_lineageLayoutView",
                RTermTableLayoutView: "#r_termTableLayoutView"

            },
            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
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
                deleteTag: '[data-id="deleteTag"]',
                backButton: "[data-id='backButton']",
                addTag: '[data-id="addTag"]',
                addTerm: '[data-id="addTerm"]',
                tagList: '[data-id="tagList"]',
                termList: '[data-id="termList"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.editButton] = function() {
                    this.ui.editButton.hide();
                    this.ui.description.hide();
                    this.ui.editBox.show();
                    this.ui.descriptionTextArea.focus();
                    if (this.descriptionPresent) {
                        this.ui.descriptionTextArea.val(this.ui.description.text());
                    }
                };
                events["click " + this.ui.tagClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() != "i") {
                        var scope = $(e.currentTarget);
                        if (scope.hasClass('term')) {
                            var url = scope.data('href').split(".").join("/terms/");
                            Globals.saveApplicationState.tabState.stateChanged = false;
                            Utils.setUrl({
                                url: '#!/taxonomy/detailCatalog/api/atlas/v1/taxonomies/' + url,
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        } else {
                            Utils.setUrl({
                                url: '#!/tag/tagAttribute/' + e.currentTarget.textContent,
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        }
                    }
                };
                // events["click " + this.ui.publishButton] = 'onPublishButtonClick';
                events["click " + this.ui.cancelButton] = 'onCancelButtonClick';
                events["click " + this.ui.deleteTag] = 'onClickTagCross';
                events["click " + this.ui.addTag] = 'onClickAddTagBtn';
                events["click " + this.ui.addTerm] = 'onClickAddTermBtn';
                events['click ' + this.ui.backButton] = function() {
                    Backbone.history.history.back();
                };
                return events;
            },
            /**
             * intialize a new DetailPageLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'collection', 'vent', 'id'));
                this.key = 'branchDetail';
                //this.updateValue();
                this.bindEvents();
                this.commonTableOptions = {
                    collection: this.collection,
                    includeFilter: false,
                    includePagination: false,
                    includePageSize: false,
                    includeFooterRecords: true,
                    gridOpts: {
                        className: "table table-striped table-condensed backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, 'reset', function() {
                    var collectionJSON = this.collection.toJSON();
                    if (collectionJSON[0].id && collectionJSON[0].id.id) {
                        var tagGuid = collectionJSON[0].id.id;
                    }
                    if (collectionJSON && collectionJSON.length) {
                        if (collectionJSON[0].values) {
                            this.name = collectionJSON[0].values.name;
                            this.description = collectionJSON[0].values.description;
                            if (this.name) {
                                this.ui.title.show();
                                this.ui.title.html('<span>' + this.name + '</span>');
                            } else {
                                this.ui.title.hide();
                            }
                            if (this.description) {
                                this.ui.description.show();
                                this.ui.description.html('<span>' + this.description + '</span>');
                            } else {
                                this.ui.description.hide();
                            }
                        }
                        if (collectionJSON[0].traits) {
                            this.tagElement = _.keys(collectionJSON[0].traits);
                            this.addTagToTerms(this.tagElement);
                        }
                    }

                    this.renderEntityDetailTableLayoutView();
                    this.renderTagTableLayoutView(tagGuid);
                    this.renderLineageLayoutView(tagGuid);
                    this.renderSchemaLayoutView(tagGuid);
                    this.renderTermTableLayoutView(tagGuid);
                }, this);
            },
            onRender: function() {
                var that = this;
                this.ui.editBox.hide();
                /*    this.ui.appendList.on('click', 'div', function(e) {
                        if (e.target.nodeName == "INPUT") {
                            return false;
                        }
                        that.ui.addTagtext.hide();
                        that.ui.addTagPlus.show();
                        that.saveTagFromList($(this));
                    });*/
            },
            fetchCollection: function() {
                this.collection.fetch({ reset: true });
            },
            onCancelButtonClick: function() {
                this.ui.description.show();
                this.ui.editButton.show();
                this.ui.editBox.hide();
            },
            onClickTagCross: function(e) {
                var tagName = $(e.currentTarget).parent().text(),
                    that = this,
                    modal = CommonViewFunction.deleteTagModel(tagName);
                modal.on('ok', function() {
                    that.deleteTagData(e);
                });
                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            deleteTagData: function(e) {
                var that = this,
                    tagName = $(e.currentTarget).text();
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': that.id,
                    callback: function() {
                        that.fetchCollection();
                    }
                });
            },
            addTagToTerms: function(tagObject) {
                var tagData = "",
                    termData = "";
                _.each(tagObject, function(val) {
                    var isTerm = Utils.checkTagOrTerm(val);
                    if (!isTerm.term) {
                        tagData += '<span class="inputTag" data-id="tagClick">' + val + '<i class="fa fa-close" data-id="deleteTag"></i></span>';
                    }
                    if (isTerm.term) {
                        termData += '<span class="inputTag term" data-id="tagClick" data-href="' + val + '">' + val + '<i class="fa fa-close" data-id="deleteTag"></i></span>';
                    }
                });
                this.ui.tagList.find("span.inputTag").remove();
                this.ui.termList.find("span.inputTag").remove();
                this.ui.tagList.prepend(tagData);
                this.ui.termList.prepend(termData);
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
                        that.fetchCollection();
                    },
                    error: function(error, data, status) {
                        if (error && error.responseText) {
                            var data = JSON.parse(error.responseText);
                        }
                    },
                    complete: function() {}
                });
            },
            onClickAddTagBtn: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        vent: that.vent,
                        guid: that.id,
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
                    /*view.saveTagData = function() {
                    override saveTagData function 
                    }*/
                });
            },
            onClickAddTermBtn: function(e) {
                var that = this;
                require([
                    'views/business_catalog/AddTermToEntityLayoutView',
                ], function(AddTermToEntityLayoutView) {
                    var view = new AddTermToEntityLayoutView({
                        guid: that.id,
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
                });

            },
            renderEntityDetailTableLayoutView: function() {
                var that = this;
                require(['views/entity/EntityDetailTableLayoutView'], function(EntityDetailTableLayoutView) {
                    that.REntityDetailTableLayoutView.show(new EntityDetailTableLayoutView({
                        globalVent: that.globalVent,
                        collection: that.collection
                    }));
                });
            },
            renderTagTableLayoutView: function(tagGuid) {
                var that = this;
                require(['views/tag/TagDetailTableLayoutView'], function(TagDetailTableLayoutView) {
                    that.RTagTableLayoutView.show(new TagDetailTableLayoutView({
                        globalVent: that.globalVent,
                        collection: that.collection,
                        guid: tagGuid
                    }));
                });
            },
            renderLineageLayoutView: function(tagGuid) {
                var that = this;
                require(['views/graph/LineageLayoutView'], function(LineageLayoutView) {
                    that.RLineageLayoutView.show(new LineageLayoutView({
                        globalVent: that.globalVent,
                        guid: tagGuid
                    }));
                });
            },
            renderSchemaLayoutView: function(tagGuid) {
                var that = this;
                require(['views/schema/SchemaLayoutView'], function(SchemaLayoutView) {
                    that.RSchemaTableLayoutView.show(new SchemaLayoutView({
                        globalVent: that.globalVent,
                        guid: tagGuid
                    }));
                });
            },
            renderTermTableLayoutView: function(tagGuid) {
                var that = this;
                require(['views/tag/TagDetailTableLayoutView'], function(TagDetailTableLayoutView) {
                    that.RTermTableLayoutView.show(new TagDetailTableLayoutView({
                        globalVent: that.globalVent,
                        collection: that.collection,
                        guid: tagGuid,
                        term: true
                    }));
                });
            }
        });
    return DetailPageLayoutView;
});
