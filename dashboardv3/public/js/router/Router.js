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
    "jquery",
    "underscore",
    "backbone",
    "App",
    "utils/Globals",
    "utils/Utils",
    "utils/UrlLinks",
    'utils/Enums',
    "collection/VGlossaryList"
], function($, _, Backbone, App, Globals, Utils, UrlLinks, Enums, VGlossaryList) {
    var AppRouter = Backbone.Router.extend({
        routes: {
            // Define some URL routes
            "": "defaultAction",
            // Search
            "!/search": "renderDefaultSearchLayoutView",
            "!/search/searchResult": function() {
                this.renderDefaultSearchLayoutView({ fromSearchResultView: true });
            },
            "!/search/customFilter": function() {
                this.renderDefaultSearchLayoutView({ fromCustomFilterView: true });
            },
            // Tag
            "!/tag": "renderTagLayoutView",
            "!/tag/tagAttribute/(*name)": "renderTagLayoutView",
            // Glossary
            "!/glossary": "renderGlossaryLayoutView",
            "!/glossary/:id": "renderGlossaryLayoutView",
            // Details
            "!/detailPage/:id": "detailPage",
            //Audit table
            '!/administrator': 'administrator',
            '!/administrator/businessMetadata/:id': 'businessMetadataDetailPage',
            // Default
            "*actions": "defaultAction"
        },
        initialize: function(options) {
            _.extend(
                this,
                _.pick(options, "entityDefCollection", "typeHeaders", "enumDefCollection", "classificationDefCollection", "metricCollection", "businessMetadataDefCollection")
            );
            this.showRegions();
            this.bindCommonEvents();
            this.listenTo(this, "route", this.postRouteExecute, this);
            this.searchVent = new Backbone.Wreqr.EventAggregator();
            this.categoryEvent = new Backbone.Wreqr.EventAggregator();
            this.glossaryCollection = new VGlossaryList([], {
                comparator: function(item) {
                    return item.get("name");
                }
            });
            this.preFetchedCollectionLists = {
                entityDefCollection: this.entityDefCollection,
                typeHeaders: this.typeHeaders,
                enumDefCollection: this.enumDefCollection,
                classificationDefCollection: this.classificationDefCollection,
                glossaryCollection: this.glossaryCollection,
                metricCollection: this.metricCollection,
                businessMetadataDefCollection: this.businessMetadataDefCollection
            };
            this.sharedObj = {
                searchTableColumns: {},
                glossary: {
                    selectedItem: {}
                },
                searchTableFilters: {
                    tagFilters: {},
                    entityFilters: {}
                }
            };
        },
        bindCommonEvents: function() {
            var that = this;
            $("body").on("click", "a.show-stat", function() {
                require(["views/site/Statistics"], function(Statistics) {
                    new Statistics(_.extend({ searchVent: that.searchVent }, that.preFetchedCollectionLists,
                        that.sharedObj));
                });
            });
            $("body").on("click", "li.aboutAtlas", function() {
                require(["views/site/AboutAtlas"], function(AboutAtlas) {
                    new AboutAtlas();
                });
            });

            $("body").on("click", "a.show-classification", function() {
                Utils.setUrl({
                    url: "!/tag",
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: true
                });
                //console.log('No route:', actions);
            });

            $("body").on("click", "a.show-glossary", function() {
                // that.renderGlossaryLayoutView();
                Utils.setUrl({
                    url: "!/glossary",
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: true
                });
            });
        },
        showRegions: function() {},
        renderViewIfNotExists: function(options) {
            var view = options.view,
                render = options.render,
                viewName = options.viewName,
                manualRender = options.manualRender;
            if (!view.currentView) {
                if (render) view.show(options.render(options));
            } else if (manualRender && viewName) {
                if (viewName === view.currentView._viewName) {
                    options.manualRender(options);
                } else {
                    if (render) view.show(options.render(options));
                }
            } else {
                if (manualRender) options.manualRender(options);
            }
        },

        /**
         * @override
         * Execute a route handler with the provided parameters. This is an
         * excellent place to do pre-route setup or post-route cleanup.
         * @param  {Function} callback - route handler
         * @param  {Array}   args - route params
         */
        execute: function(callback, args) {
            this.preRouteExecute();
            if (callback) callback.apply(this, args);
            this.postRouteExecute();
        },
        preRouteExecute: function() {
            $("body").removeClass("global-search-active");
            $(".tooltip").tooltip("hide");
            // console.log("Pre-Route Change Operations can be performed here !!");
        },
        postRouteExecute: function(name, args) {
            // console.log("Post-Route Change Operations can be performed here !!");
            // console.log("Route changed: ", name);
        },
        getHeaderOptions: function(Header, options) {
            var that = this;
            return {
                view: App.rHeader,
                manualRender: function() {
                    this.view.currentView.manualRender(options);
                },
                render: function() {
                    return new Header(_.extend({}, that.preFetchedCollectionLists, that.sharedObj, options));
                }
            };
        },
        renderTagLayoutView: function(tagName) {
            var that = this;
            require(["views/site/Header", "views/tag/TagContainerLayoutView", "views/site/SideNavLayoutView"], function(Header, TagContainerLayoutView, SideNavLayoutView) {
                var paramObj = Utils.getUrlState.getQueryParams(),
                    url = Utils.getUrlState.getQueryUrl().queyParams[0];
                that.renderViewIfNotExists(that.getHeaderOptions(Header));
                // updating paramObj to check for new queryparam.
                paramObj = Utils.getUrlState.getQueryParams();
                if (paramObj && paramObj.dlttag) {
                    return false;
                }
                var options = _.extend({
                        tag: tagName,
                        value: paramObj,
                        searchVent: that.searchVent,
                        categoryEvent: that.categoryEvent
                    },
                    that.preFetchedCollectionLists,
                    that.sharedObj
                )
                that.renderViewIfNotExists({
                    view: App.rSideNav,
                    manualRender: function() {
                        this.view.currentView.manualRender(options);
                    },
                    render: function() {
                        return new SideNavLayoutView(options);
                    }
                });
                App.rContent.show(
                    new TagContainerLayoutView(
                        _.extend({
                                tag: tagName,
                                value: paramObj,
                                searchVent: that.searchVent
                            },
                            that.preFetchedCollectionLists,
                            that.sharedObj
                        )
                    )
                );
            });
        },
        renderGlossaryLayoutView: function(id) {
            var that = this;
            require(["views/site/Header", "views/glossary/GlossaryContainerLayoutView", "views/site/SideNavLayoutView"], function(Header, GlossaryContainerLayoutView, SideNavLayoutView) {
                var paramObj = Utils.getUrlState.getQueryParams(),
                    url = Utils.getUrlState.getQueryUrl().queyParams[0];
                that.renderViewIfNotExists(that.getHeaderOptions(Header));
                // updating paramObj to check for new queryparam.
                paramObj = Utils.getUrlState.getQueryParams();
                if (paramObj && paramObj.dlttag) {
                    return false;
                }
                var options = _.extend({
                        guid: id,
                        value: paramObj,
                        searchVent: that.searchVent,
                        categoryEvent: that.categoryEvent
                    },
                    that.preFetchedCollectionLists,
                    that.sharedObj
                );

                that.renderViewIfNotExists({
                    view: App.rSideNav,
                    manualRender: function() {
                        this.view.currentView.manualRender(options);
                    },
                    render: function() {
                        return new SideNavLayoutView(options);
                    }
                });
                that.renderViewIfNotExists({
                    view: App.rContent,
                    viewName: "GlossaryContainerLayoutView",
                    manualRender: function() {
                        this.view.currentView.manualRender(options);
                    },
                    render: function() {
                        return new GlossaryContainerLayoutView(options)
                    }
                });
            });
        },
        renderDefaultSearchLayoutView: function(opt) {
            var that = this;
            require(["views/site/Header", "views/search/SearchDefaultLayoutView", "views/site/SideNavLayoutView"], function(Header, SearchDefaultLayoutView, SideNavLayoutView) {
                var paramObj = Utils.getUrlState.getQueryParams();
                if (paramObj && (paramObj.type || paramObj.tag || paramObj.term || paramObj.query || paramObj.udKeys || paramObj.udLabels) === undefined) {
                    Utils.setUrl({
                        url: "#!/search",
                        mergeBrowserUrl: false,
                        trigger: true,
                        updateTabState: true
                    });
                }
                if (Utils.getUrlState.getQueryUrl().lastValue !== "search" && Utils.getUrlState.isAdministratorTab() === false) {
                    paramObj = _.omit(paramObj, ["tabActive", "ns", "nsa"]);
                    Utils.setUrl({
                        url: "#!/search/searchResult",
                        urlParams: paramObj,
                        mergeBrowserUrl: false,
                        trigger: false,
                        updateTabState: true
                    });
                }
                if (paramObj) {
                    if (!paramObj.type) {
                        if (paramObj.entityFilters) {
                            paramObj.entityFilters = null;
                        }
                    }
                    if (!paramObj.tag) {
                        if (paramObj.tagFilters) {
                            paramObj.tagFilters = null;
                        }
                    } else {
                        var tagValidate = paramObj.tag,
                            isTagPresent = false;
                        if ((tagValidate.indexOf('*') == -1)) {
                            classificationDefCollection.fullCollection.each(function(model) {
                                var name = Utils.getName(model.toJSON(), 'name');
                                if (model.get('category') == 'CLASSIFICATION') {
                                    if (tagValidate) {
                                        if (name === tagValidate) {
                                            isTagPresent = true;
                                        }
                                    }
                                }
                            });
                            _.each(Enums.addOnClassification, function(classificationName) {
                                if (classificationName === tagValidate) {
                                    isTagPresent = true;
                                }
                            });
                            if (!isTagPresent) {
                                paramObj.tag = null;
                            }
                        }
                    }

                }
                var isinitialView = true,
                    isTypeTagNotExists = false,
                    tempParam = _.extend({}, paramObj);
                if (paramObj) {
                    isinitialView =
                        (
                            paramObj.type ||
                            (paramObj.dslChecked == "true" ? "" : paramObj.tag || paramObj.term) ||
                            (paramObj.query ? paramObj.query.trim() : "")
                        ).length === 0;
                }
                var options = _.extend({
                        value: paramObj,
                        searchVent: that.searchVent,
                        categoryEvent: that.categoryEvent,
                        initialView: isinitialView,
                        fromDefaultSearch: opt ? (opt && !opt.fromSearchResultView) : true,
                        fromSearchResultView: (opt && opt.fromSearchResultView) || false,
                        fromCustomFilterView: (opt && opt.fromCustomFilterView) || false,
                        isTypeTagNotExists: paramObj && (paramObj.type != tempParam.type || tempParam.tag != paramObj.tag)
                    },
                    that.preFetchedCollectionLists,
                    that.sharedObj
                );
                that.renderViewIfNotExists(
                    that.getHeaderOptions(Header, {
                        fromDefaultSearch: options.fromDefaultSearch
                    })
                );
                that.renderViewIfNotExists({
                    view: App.rSideNav,
                    manualRender: function() {
                        this.view.currentView.manualRender(options);
                    },
                    render: function() {
                        return new SideNavLayoutView(
                            _.extend({}, that.preFetchedCollectionLists, that.sharedObj, options)
                        );
                    }
                });
                that.renderViewIfNotExists({
                    view: App.rContent,
                    viewName: "SearchDefaultlLayoutView",
                    manualRender: function() {
                        this.view.currentView.manualRender(options);
                    },
                    render: function() {
                        return new SearchDefaultLayoutView(options);
                    }
                });
            });
        },
        detailPage: function(id) {
            var that = this;
            if (id) {
                require(["views/site/Header", "views/detail_page/DetailPageLayoutView", "collection/VEntityList", "views/site/SideNavLayoutView"], function(
                    Header,
                    DetailPageLayoutView,
                    VEntityList,
                    SideNavLayoutView
                ) {
                    this.entityCollection = new VEntityList([], {});
                    var paramObj = Utils.getUrlState.getQueryParams();
                    that.renderViewIfNotExists(that.getHeaderOptions(Header));
                    that.renderViewIfNotExists({
                        view: App.rSideNav,
                        manualRender: function() {
                            this.view.currentView.manualRender();
                        },
                        render: function() {
                            return new SideNavLayoutView(
                                _.extend({
                                    searchVent: that.searchVent,
                                    categoryEvent: that.categoryEvent
                                }, that.preFetchedCollectionLists, that.sharedObj)
                            );
                        }
                    });
                    App.rContent.show(
                        new DetailPageLayoutView(
                            _.extend({
                                    collection: this.entityCollection,
                                    id: id,
                                    value: paramObj,
                                    searchVent: that.searchVent
                                },
                                that.preFetchedCollectionLists,
                                that.sharedObj
                            )
                        )
                    );
                    this.entityCollection.url = UrlLinks.entitiesApiUrl({ guid: id, minExtInfo: true });
                    this.entityCollection.fetch({ reset: true });
                });
            }
        },
        glossaryDetailPage: function(id) {
            var that = this;
            if (id) {
                require(["views/site/Header", "views/glossary/GlossaryDetailLayoutView", "views/site/SideNavLayoutView"], function(Header, GlossaryDetailLayoutView, SideNavLayoutView) {
                    var paramObj = Utils.getUrlState.getQueryParams();
                    that.renderViewIfNotExists(that.getHeaderOptions(Header));
                    that.renderViewIfNotExists({
                        view: App.rSideNav,
                        manualRender: function() {
                            this.view.currentView.RGlossaryLayoutView.currentView.manualRender(_.extend({}, { 'guid': id, 'value': paramObj }));
                            this.view.currentView.selectTab();
                        },
                        render: function() {
                            return new SideNavLayoutView(
                                _.extend({}, that.preFetchedCollectionLists, that.sharedObj, { 'guid': id, 'value': paramObj })
                            )
                        }
                    });
                    App.rContent.show(
                        new GlossaryDetailLayoutView(
                            _.extend({
                                    guid: id,
                                    value: paramObj
                                },
                                that.preFetchedCollectionLists,
                                that.sharedObj
                            )
                        )
                    );
                });
            }
        },
        commonAction: function() {
            var that = this;
            require(["views/site/Header", "views/search/SearchDetailLayoutView", "views/site/SideNavLayoutView"], function(Header, SearchDetailLayoutView, SideNavLayoutView) {
                var paramObj = Utils.getUrlState.getQueryParams();
                that.renderViewIfNotExists(that.getHeaderOptions(Header));
                that.renderViewIfNotExists({
                    view: App.rSideNav,
                    manualRender: function() {
                        this.view.currentView.selectTab();
                    },
                    render: function() {
                        return new SideNavLayoutView(
                            _.extend({}, that.preFetchedCollectionLists, that.sharedObj)
                        );
                    }
                });

                if (Globals.entityCreate && Utils.getUrlState.isSearchTab()) {
                    App.rContent.show(
                        new SearchDetailLayoutView(
                            _.extend({
                                    value: paramObj,
                                    initialView: true,
                                    searchVent: that.searchVent
                                },
                                that.preFetchedCollectionLists,
                                that.sharedObj
                            )
                        )
                    );
                } else {
                    if (App.rNContent.currentView) {
                        App.rNContent.currentView.destroy();
                    }
                }
            });
        },
        administrator: function() {
            var that = this;
            require(["views/site/Header", "views/site/SideNavLayoutView", 'views/administrator/AdministratorLayoutView'], function(Header, SideNavLayoutView, AdministratorLayoutView) {
                var value = Utils.getUrlState.getQueryParams(),
                    paramObj = _.extend({ value: value, guid: null }, that.preFetchedCollectionLists);
                that.renderViewIfNotExists(that.getHeaderOptions(Header));
                that.renderViewIfNotExists({
                    view: App.rSideNav,
                    manualRender: function() {
                        this.view.currentView.manualRender(paramObj);
                    },
                    render: function() {
                        return new SideNavLayoutView(
                            _.extend({ searchVent: that.searchVent, categoryEvent: that.categoryEvent }, that.preFetchedCollectionLists, that.sharedObj)
                        );
                    }
                });
                App.rContent.show(new AdministratorLayoutView(paramObj));
            });
        },
        businessMetadataDetailPage: function(guid) {
            var that = this;
            require(["views/site/Header", "views/site/SideNavLayoutView", "views/business_metadata/BusinessMetadataContainerLayoutView", ], function(Header, SideNavLayoutView, BusinessMetadataContainerLayoutView) {
                var paramObj = Utils.getUrlState.getQueryParams();
                that.renderViewIfNotExists(that.getHeaderOptions(Header));
                var options = _.extend({
                        guid: guid,
                        value: paramObj,
                        searchVent: that.searchVent,
                        categoryEvent: that.categoryEvent
                    },
                    that.preFetchedCollectionLists,
                    that.sharedObj
                )
                that.renderViewIfNotExists({
                    view: App.rSideNav,
                    manualRender: function() {
                        this.view.currentView.manualRender(options);
                    },
                    render: function() {
                        return new SideNavLayoutView(options);
                    }
                });
                App.rContent.show(new BusinessMetadataContainerLayoutView(options));
            });
        },
        defaultAction: function(actions) {
            // We have no matching route, lets just log what the URL was
            Utils.setUrl({
                url: "#!/search",
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: true
            });

            console.log("No route:", actions);
        }
    });
    return AppRouter;
});