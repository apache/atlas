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
define([
    "require",
    "hbs!tmpl/search/SearchFilterBrowseLayoutView_tmpl",
    "utils/Utils",
    "utils/Globals",
    "utils/UrlLinks",
    "utils/CommonViewFunction",
    "collection/VSearchList",
    "jstree"
], function(require, SearchFilterBrowseLayoutViewTmpl, Utils, Globals, UrlLinks, CommonViewFunction, VSearchList) {
    "use strict";

    var SearchFilterBrowseLayoutViewNew = Marionette.LayoutView.extend({
        template: SearchFilterBrowseLayoutViewTmpl,

        regions: {
            // RSaveSearchBasic: '[data-id="r_saveSearchBasic"]',
            RGlossaryTreeRender: '[data-id="r_glossaryTreeRender"]',
            RClassificationTreeRender: '[data-id="r_classificationTreeRender"]',
            REntityTreeRender: '[data-id="r_entityTreeRender"]',
            RCustomFilterTreeRender: '[data-id="r_customFilterTreeRender"]',

        },
        ui: {
            //search
            searchNode: '[data-id="searchNode"]',

            sliderBar: '[data-id="sliderBar"]',
            menuItems: '.menu-items'
        },
        templateHelpers: function() {
            return {
                apiBaseUrl: UrlLinks.apiBaseUrl
            };
        },
        events: function() {
            var events = {},
                that = this;

            events["click " + this.ui.sliderBar] = function(e) {
                e.stopPropagation();
                $("#sidebar-wrapper,#page-wrapper").addClass("animate-me");
                $(".container-fluid.view-container").toggleClass("slide-in");
                $("#sidebar-wrapper,.search-browse-box,#page-wrapper").removeAttr("style");
                setTimeout(function() {
                    $("#sidebar-wrapper,#page-wrapper").removeClass("animate-me");
                }, 301);
            };

            events["keyup " + this.ui.searchNode] = function(e) {
                // var type = $(e.currentTarget).data("type");
                // var showEmpty = false;
                // this.RClassificationTreeRender.currentView.onSearchClassificationNode(showEmpty);
                // this.REntityTreeRender.currentView.onSearchEntityNode(showEmpty);
                var searchString = e.target.value;
                if (searchString.trim() === "") {
                    this.$(".panel").removeClass("hide");
                    // showEmpty = true;
                    // this.RClassificationTreeRender.currentView.onSearchClassificationNode(showEmpty);
                    // this.REntityTreeRender.currentView.onSearchEntityNode(showEmpty);
                }
                this.$(".panel-collapse.collapse").addClass("in");
                this.entitySearchTree = this.$('[data-id="entitySearchTree"]');
                this.classificationSearchTree = this.$('[data-id="classificationSearchTree"]');
                this.termSearchTree = this.$('[data-id="termSearchTree"]');
                this.customFilterSearchTree = this.$('[data-id="customFilterSearchTree"]');
                this.entitySearchTree.jstree(true).show_all();
                this.entitySearchTree.jstree("search", searchString);
                this.classificationSearchTree.jstree(true).show_all();
                this.classificationSearchTree.jstree("search", searchString);
                this.termSearchTree.jstree(true).show_all();
                this.termSearchTree.jstree("search", searchString);
                this.customFilterSearchTree.jstree(true).show_all();
                this.customFilterSearchTree.jstree("search", searchString);
            };

            events["click " + this.ui.menuItems] = function(e) {
                e.stopPropagation();
                //this.$('.menu-items').removeClass('open');
            }
            return events;
        },
        bindEvents: function() {},
        initialize: function(options) {
            this.options = options;
            _.extend(
                this,
                _.pick(
                    options,
                    "typeHeaders",
                    "searchVent",
                    "entityDefCollection",
                    "enumDefCollection",
                    "classificationDefCollection",
                    "searchTableColumns",
                    "searchTableFilters",
                    "metricCollection",
                    "glossaryCollection"
                )
            );
            this.bindEvents();
        },
        onRender: function() {
            this.renderEntityTree();
            this.renderClassificationTree();
            this.renderGlossaryTree();
            this.renderCustomFilterTree();
            //  this.renderSaveSearch();
            this.showHideGlobalFilter();
            this.showDefaultPage();
        },

        showDefaultPage: function() {
            if (this.options.value) {
                if (!this.options.value.type && !this.options.value.tag && !this.options.value.term && !this.options.value.gType) {
                    Utils.setUrl({
                        url: '!/search',
                        mergeBrowserUrl: false,
                        trigger: true,
                        updateTabState: true
                    });
                }
            }

        },
        onShow: function() {
            var that = this;
            this.$(".search-browse-box").resizable({
                handles: { 'e': '.slider-bar' },
                minWidth: 224,
                maxWidth: 360,
                resize: function(event, ui) {
                    var width = ui.size.width,
                        calcWidth = "calc(100% - " + width + "px)";
                    $("#sidebar-wrapper").width(width);
                    $("#page-wrapper").css({ "width": calcWidth, marginLeft: width + "px" });
                },
                start: function() {
                    this.expanding = $(".container-fluid.view-container").hasClass("slide-in");
                    $(".container-fluid.view-container").removeClass("slide-in");
                    if (this.expanding) {
                        $("#sidebar-wrapper,#page-wrapper").addClass("animate-me");
                    }
                },
                stop: function(event, ui) {
                    if (!this.expanding && ui.size.width < 225) {
                        $("#sidebar-wrapper,#page-wrapper").addClass("animate-me");
                        $("#sidebar-wrapper,#page-wrapper,.search-browse-box").removeAttr("style");
                        $(".container-fluid.view-container").addClass("slide-in");
                    }
                    setTimeout(function() {
                        $("#sidebar-wrapper,#page-wrapper").removeClass("animate-me");
                    }, 301);
                }
            })
        },
        showHideGlobalFilter: function() {
            if (this.options.fromDefaultSearch) {
                this.$(".mainContainer").removeClass("global-filter-browser");
            } else {
                this.$(".mainContainer").addClass("global-filter-browser");
            }
        },
        // renderSaveSearch: function() {
        //     var that = this;
        //     require(["views/search/save/SaveSearchView"], function(SaveSearchView) {
        //         var saveSearchBaiscCollection = new VSearchList(),
        //             saveSearchAdvanceCollection = new VSearchList(),
        //             saveSearchCollection = new VSearchList();
        //         saveSearchCollection.url = UrlLinks.saveSearchApiUrl();
        //         saveSearchBaiscCollection.fullCollection.comparator = function(model) {
        //             return getModelName(model);
        //         };
        //         saveSearchAdvanceCollection.fullCollection.comparator = function(model) {
        //             return getModelName(model);
        //         };
        //         var obj = {
        //             value: that.options.value,
        //             searchVent: that.searchVent,
        //             typeHeaders: that.typeHeaders,
        //             fetchCollection: fetchSaveSearchCollection,
        //             classificationDefCollection: that.classificationDefCollection,
        //             entityDefCollection: that.entityDefCollection,
        //             getValue: function() {
        //                 var queryObj = that.query[that.type],
        //                     entityObj = that.searchTableFilters["entityFilters"],
        //                     tagObj = that.searchTableFilters["tagFilters"],
        //                     urlObj = Utils.getUrlState.getQueryParams();
        //                 if (urlObj) {
        //                     // includeDE value in because we need to send "true","false" to the server.
        //                     urlObj.includeDE = urlObj.includeDE == "true" ? true : false;
        //                     urlObj.excludeSC = urlObj.excludeSC == "true" ? true : false;
        //                     urlObj.excludeST = urlObj.excludeST == "true" ? true : false;
        //                 }
        //                 return _.extend({}, queryObj, urlObj, {
        //                     entityFilters: entityObj ? entityObj[queryObj.type] : null,
        //                     tagFilters: tagObj ? tagObj[queryObj.tag] : null,
        //                     type: queryObj.type,
        //                     query: queryObj.query,
        //                     term: queryObj.term,
        //                     tag: queryObj.tag
        //                 });
        //             },
        //             applyValue: function(model, searchType) {
        //                 that.manualRender(
        //                     _.extend(
        //                         searchType,
        //                         CommonViewFunction.generateUrlFromSaveSearchObject({
        //                             value: { searchParameters: model.get("searchParameters"), uiParameters: model.get("uiParameters") },
        //                             classificationDefCollection: that.classificationDefCollection,
        //                             entityDefCollection: that.entityDefCollection
        //                         })
        //                     )
        //                 );
        //             }
        //         };

        //         //  will be shown as different tab on the screen.
        //         that.RSaveSearchBasic.show(
        //             new SaveSearchView(
        //                 _.extend(obj, {
        //                     isBasic: true,
        //                     displayButtons: false,
        //                     collection: saveSearchBaiscCollection.fullCollection
        //                 })
        //             )
        //         );
        //         // that.RSaveSearchAdvance.show(new SaveSearchView(_.extend(obj, {
        //         //     isBasic: false,
        //         //     collection: saveSearchAdvanceCollection.fullCollection
        //         // })));
        //         function getModelName(model) {
        //             if (model.get('name')) {
        //                 return model.get('name').toLowerCase();
        //             }
        //         }

        //         function fetchSaveSearchCollection() {
        //             saveSearchCollection.fetch({
        //                 success: function(collection, data) {
        //                     saveSearchAdvanceCollection.fullCollection.reset(_.where(data, { searchType: "ADVANCED" }));
        //                     saveSearchBaiscCollection.fullCollection.reset(_.where(data, { searchType: "BASIC" }));
        //                 },
        //                 silent: true
        //             });
        //         }
        //         fetchSaveSearchCollection();
        //     });
        // },
        manualRender: function(options) {
            var that = this;
            if (options) {
                _.extend(this.options, options);
                this.showHideGlobalFilter();
                if (this.RCustomFilterTreeRender.currentView) {
                    this.RCustomFilterTreeRender.currentView.manualRender(this.options);
                }
                if (this.RGlossaryTreeRender.currentView) {
                    this.RGlossaryTreeRender.currentView.manualRender(this.options);
                }
                if (this.RClassificationTreeRender.currentView) {
                    this.RClassificationTreeRender.currentView.manualRender(this.options);
                }
                if (this.REntityTreeRender.currentView) {
                    this.REntityTreeRender.currentView.manualRender(this.options);
                }
            }
        },
        renderEntityTree: function() {
            var that = this;
            require(["views/search/tree/EntityTreeLayoutView"], function(ClassificationTreeLayoutView) {
                that.REntityTreeRender.show(new ClassificationTreeLayoutView(_.extend({ query: that.query }, that.options)))
            });
        },
        renderClassificationTree: function() {
            var that = this;
            require(["views/search/tree/ClassificationTreeLayoutView"], function(ClassificationTreeLayoutView) {
                that.RClassificationTreeRender.show(new ClassificationTreeLayoutView(_.extend({ query: that.query }, that.options)))
            });
        },
        renderGlossaryTree: function() {
            var that = this;
            require(["views/search/tree/GlossaryTreeLayoutView"], function(GlossaryTreeLayoutView) {
                that.RGlossaryTreeRender.show(new GlossaryTreeLayoutView(_.extend({ query: that.query }, that.options)))
            });
        },
        renderCustomFilterTree: function() {
            var that = this;
            require(["views/search/tree/CustomFilterTreeLayoutView"], function(CustomFilterTreeLayoutView) {
                that.RCustomFilterTreeRender.show(new CustomFilterTreeLayoutView(_.extend({ query: that.query }, that.options)))
            });
        }
    });
    return SearchFilterBrowseLayoutViewNew;
});