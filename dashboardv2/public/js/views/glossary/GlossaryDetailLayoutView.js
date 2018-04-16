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
    'hbs!tmpl/glossary/GlossaryDetailLayoutView_tmpl',
    'utils/Utils',
    'utils/Messages',
    'utils/Globals',
    'utils/CommonViewFunction'
], function(require, Backbone, GlossaryDetailLayoutViewTmpl, Utils, Messages, Globals, CommonViewFunction) {
    'use strict';

    var GlossaryDetailLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends GlossaryDetailLayoutView */
        {
            _viewName: 'GlossaryDetailLayoutView',

            template: GlossaryDetailLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RSearchResultLayoutView: "#r_searchResultLayoutView",
            },
            templateHelpers: function() {
                return {
                    isTermView: this.isTermView,
                    isCategoryView: this.isCategoryView
                };
            },

            /** ui selector cache */
            ui: {
                details: "[data-id='details']",
                editButton: "[data-id='editButton']",
                title: "[data-id='title']",
                shortDescription: "[data-id='shortDescription']",
                longDescription: "[data-id='longDescription']",
                categoryList: "[data-id='categoryList']",
                removeCategory: "[data-id='removeCategory']",
                categoryClick: "[data-id='categoryClick']",
                addCategory: "[data-id='addCategory']",
                termList: "[data-id='termList']",
                removeTerm: "[data-id='removeTerm']",
                termClick: "[data-id='termClick']",
                addTerm: "[data-id='addTerm']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.categoryClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickRemoveAssociationBtn(e);
                    } else {
                        var guid = $(e.currentTarget).data('guid'),
                            categoryObj = _.find(this.data.categories, { "categoryGuid": guid });
                        this.glossary.selectedItem = { "type": "GlossaryCategory", "guid": guid, "model": categoryObj };
                        Utils.setUrl({
                            url: '#!/glossary/' + guid,
                            mergeBrowserUrl: false,
                            urlParams: { gType: "category" },
                            trigger: true,
                            updateTabState: true
                        });
                    }
                };
                events["click " + this.ui.termClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickRemoveAssociationBtn(e);
                    } else {
                        var guid = $(e.currentTarget).data('guid'),
                            termObj = _.find(this.data.terms, { "termGuid": guid });
                        this.glossary.selectedItem = { "type": "GlossaryTerm", "guid": guid, "model": termObj };
                        Utils.setUrl({
                            url: '#!/glossary/' + guid,
                            mergeBrowserUrl: false,
                            urlParams: { gType: "term" },
                            trigger: true,
                            updateTabState: true
                        });
                    }
                };
                events["click " + this.ui.editButton] = function(e) {
                    var that = this,
                        model = this.glossaryCollection.fullCollection.get(this.guid);
                    if (this.isGlossaryView) {
                        CommonViewFunction.createEditGlossaryCategoryTerm({
                            "model": model,
                            "isGlossaryView": this.isGlossaryView,
                            "collection": this.glossaryCollection,
                            "callback": function(newModel) {
                                that.data = newModel;
                                model.set(newModel);
                                that.renderDetails(that.data);
                                //that.glossaryCollection.trigger("update:details");
                            }
                        });
                    } else {
                        CommonViewFunction.createEditGlossaryCategoryTerm({
                            "isTermView": this.isTermView,
                            "isCategoryView": this.isCategoryView,
                            "model": this.data,
                            "collection": this.glossaryCollection,
                            "callback": function() {
                                that.getData();
                                that.glossaryCollection.trigger("update:details");
                            }
                        });
                    }
                };
                events["click " + this.ui.addTerm] = 'onClickAddTermBtn';
                events["click " + this.ui.addCategory] = 'onClickAddCategoryBtn';
                return events;
            },
            /**
             * intialize a new GlossaryDetailLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'glossaryCollection', 'glossary', 'collection', 'typeHeaders', 'value', 'entityDefCollection', 'enumDefCollection', 'classificationDefCollection'));
                if (this.value && this.value.gType) {
                    if (this.value.gType == "category") {
                        this.isCategoryView = true;
                    } else if (this.value.gType == "term") {
                        this.isTermView = true;
                    } else {
                        this.isGlossaryView = true;
                    }
                }
            },
            onRender: function() {
                this.getData();
                this.bindEvents();
            },
            bindEvents: function() {
                var that = this;
            },
            getData: function() {
                if (this.glossaryCollection.fullCollection.length && this.isGlossaryView) {
                    this.data = this.glossaryCollection.fullCollection.get(this.guid).toJSON();
                    this.renderDetails(this.data);
                } else {
                    Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.ui.details);
                    var getApiFunctionKey = "getCategory",
                        that = this;
                    if (this.isTermView) {
                        getApiFunctionKey = "getTerm";
                    }
                    this.glossaryCollection[getApiFunctionKey]({
                        "guid": this.guid,
                        "ajaxOptions": {
                            success: function(data) {
                                if (that.isTermView) {
                                    that.renderSearchResultLayoutView();
                                }
                                that.data = data;
                                that.glossary.selectedItem.model = data;
                                that.glossary.selectedItem.guid = data.guid;
                                that.renderDetails(data)
                            },
                            cust_error: function() {}
                        }
                    });
                }
            },
            renderDetails: function(data) {
                Utils.hideTitleLoader(this.$('.fontLoader'), this.ui.details);
                this.ui.title.text(data.displayName || data.displayText || data.qualifiedName);
                this.ui.shortDescription.text(data.shortDescription);
                this.ui.longDescription.text(data.longDescription);
                this.generateCategories(data.categories);
                this.generateTerm(data.terms);

            },
            generateCategories: function(data) {
                var that = this,
                    categories = "";
                _.each(data, function(val) {
                    var name = _.escape(val.displayText);
                    categories += '<span data-guid="' + val.categoryGuid + '"" class="btn btn-action btn-sm btn-icon btn-blue" title=' + _.escape(name) + ' data-id="categoryClick"><span>' + name + '</span><i class="fa fa-close" data-id="removeCategory" data-type="category" title="Remove Category"></i></span>';
                });
                this.ui.categoryList.find("span.btn").remove();
                this.ui.categoryList.prepend(categories);
            },
            generateTerm: function(data) {
                var that = this,
                    terms = "";
                _.each(data, function(val) {
                    var name = _.escape(val.displayText);
                    terms += '<span data-guid="' + val.termGuid + '"" class="btn btn-action btn-sm btn-icon btn-blue" title=' + _.escape(name) + ' data-id="termClick"><span>' + name + '</span><i class="fa fa-close" data-id="removeTerm" data-type="term" title="Remove Term"></i></span>';
                });
                this.ui.termList.find("span.btn").remove();
                this.ui.termList.prepend(terms);

            },
            onClickAddTermBtn: function(e) {
                var that = this;
                require(['views/glossary/AssignTermLayoutView'], function(AssignTermLayoutView) {
                    var view = new AssignTermLayoutView({
                        categoryData: that.data,
                        isCategoryView: that.isCategoryView,
                        callback: function() {
                            that.getData();
                        },
                        glossaryCollection: that.glossaryCollection
                    });
                    view.modal.on('ok', function() {
                        that.hideLoader();
                    });
                });
            },
            onClickAddCategoryBtn: function(e) {
                var that = this;
                require(['views/glossary/AssignTermLayoutView'], function(AssignTermLayoutView) {
                    var view = new AssignTermLayoutView({
                        termData: that.data,
                        isTermView: that.isTermView,
                        callback: function() {
                            that.getData();
                        },
                        glossaryCollection: that.glossaryCollection
                    });
                    view.modal.on('ok', function() {
                        that.hideLoader();
                    });
                });
            },
            onClickRemoveAssociationBtn: function(e) {
                var $el = $(e.currentTarget),
                    guid = $el.data('guid'),
                    name = $el.text(),
                    that = this;
                CommonViewFunction.removeCategoryTermAssociation({
                    selectedGuid: guid,
                    model: that.data,
                    collection: that.glossaryCollection,
                    msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(name) + "</b> assignment from" + " " + "<b>" + that.data.displayName + "?</b></div>",
                    titleMessage: Messages.glossary[that.isTermView ? "removeCategoryfromTerm" : "removeTermfromCategory"],
                    isCategoryView: that.isCategoryView,
                    isTermView: that.isTermView,
                    buttonText: "Remove",
                    showLoader: that.hideLoader.bind(that),
                    hideLoader: that.hideLoader.bind(that),
                    callback: function() {
                        that.getData();
                    }
                });
            },
            showLoader: function() {
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.ui.details);
            },
            hideLoader: function() {
                Utils.hideTitleLoader(this.$('.page-title .fontLoader'), this.ui.details);
            },
            renderSearchResultLayoutView: function() {
                var that = this;
                require(['views/search/SearchResultLayoutView'], function(SearchResultLayoutView) {
                    var value = {
                        'tag': "PII",
                        'searchType': 'basic'
                    };
                    if (that.RSearchResultLayoutView) {
                        that.RSearchResultLayoutView.show(new SearchResultLayoutView({
                            "value": _.extend({}, that.value, { "searchType": "basic" }),
                            "termName": that.data.qualifiedName,
                            "guid": that.guid,
                            "entityDefCollection": that.entityDefCollection,
                            "typeHeaders": that.typeHeaders,
                            "tagCollection": that.collection,
                            "enumDefCollection": that.enumDefCollection,
                            "classificationDefCollection": that.classificationDefCollection,
                            "fromView": "glossary"
                        }));
                    }
                });
            },
        });
    return GlossaryDetailLayoutView;
});