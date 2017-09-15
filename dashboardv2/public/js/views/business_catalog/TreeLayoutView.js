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
    'hbs!tmpl/business_catalog/TreeLayoutView_tmpl',
    'utils/Utils',
    'collection/VCatalogList',
    'utils/CommonViewFunction',
    'utils/Messages',
    'utils/UrlLinks'
], function(require, Backbone, TreeLayoutView_tmpl, Utils, VCatalogList, CommonViewFunction, Messages, UrlLinks) {
    'use strict';

    var TreeLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TreeLayoutView */
        {
            _viewName: 'TreeLayoutView',

            template: TreeLayoutView_tmpl,

            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                Parent: '[data-id="Parent"]',
                childList: '[data-id="childList"]',
                liClick: 'li a[data-href]',
                backTaxanomy: '[data-id="backTaxanomy"]',
                expandArrow: '[data-id="expandArrow"]',
                searchTermInput: '[data-id="searchTermInput"]',
                refreshTaxanomy: '[data-id="refreshTaxanomy"]',
                descriptionAssign: '[data-id="descriptionAssign"]',
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events['dblclick ' + this.ui.liClick] = function(e) {
                    this.changeArrowState(e);
                };
                events['click ' + this.ui.liClick] = function(e) {
                    var that = this;
                    that.addActiveClass(e);
                    that.url = $(e.currentTarget).data('href');
                    if (this.viewBased) {
                        Utils.setUrl({
                            url: '#!/taxonomy/detailCatalog' + that.url,
                            mergeBrowserUrl: false,
                            trigger: true,
                            updateTabState: true
                        });
                    }
                };
                events['click ' + this.ui.backTaxanomy] = 'backButtonTaxanomy';
                events['click ' + this.ui.refreshTaxanomy] = 'refreshButtonTaxanomy';
                events['click ' + this.ui.expandArrow] = 'changeArrowState';
                events["change " + this.ui.searchTermInput] = function() {
                    var termUrl = this.termCollection.url.split("/", 5).join("/") + "/" + this.ui.searchTermInput.val().split(".").join("/terms/");
                    this.fetchCollection(termUrl, true);
                    if (this.viewBased) {
                        Utils.setUrl({
                            url: "#!/taxonomy/detailCatalog" + termUrl,
                            mergeBrowserUrl: false,
                            trigger: true,
                            updateTabState: true
                        });
                    }
                };
                return events;
            },
            /**
             * intialize a new TreeLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'url', 'viewBased'));
                this.parentCollection = new VCatalogList();
                this.childCollection = new VCatalogList();
                this.taxanomy = new VCatalogList();
                this.termCollection = new VCatalogList();
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.parentCollection, 'reset', function() {
                    if (this.parentCollection.fullCollection.models.length) {
                        this.generateTree(true);
                    } else {
                        if (Utils.getUrlState.isTaxonomyTab() || Utils.getUrlState.isInitial()) {
                            this.createDefaultTaxonomy();
                        }
                    }
                }, this);
                this.listenTo(this.childCollection, 'reset', function() {
                    this.generateTree();
                }, this);
                this.listenTo(this.taxanomy, 'reset', function() {
                    this.searchResult();
                }, this);
                this.listenTo(this.termCollection, 'reset', function() {
                    this.termSearchData();
                }, this);
                this.listenTo(this.childCollection, 'error', function(model, response) {
                    this.hideLoader();
                }, this);
                this.listenTo(this.parentCollection, 'error', function(model, response) {
                    this.hideLoader();
                }, this);
            },
            onRender: function() {
                var that = this;
                this.bindEvents();
                that.ui.backTaxanomy.hide();
                this.fetchCollection(this.url, true);
                this.fetchTaxanomyCollections();
                if (!this.viewBased) {
                    this.ui.descriptionAssign.show();
                } else {
                    this.ui.descriptionAssign.hide();
                }
            },
            backButtonTaxanomy: function(e) {
                var that = this;
                this.backButton = true;
                var dataURL = this.$('.taxonomyTree').find('li[data-id="Parent"]').find("a").data('href').split("/terms");
                var backUrl = dataURL.pop();
                if (dataURL.join("/terms").length) {
                    this.ui.backTaxanomy.show();
                    var currentURL = "#!/taxonomy/detailCatalog" + dataURL.join("/terms");
                    this.fetchCollection(dataURL.join("/terms"), true);
                    this.url = dataURL.join("/terms");
                    if (this.viewBased) {
                        Utils.setUrl({
                            url: currentURL,
                            mergeBrowserUrl: false,
                            trigger: true,
                            updateTabState: true
                        });
                    }
                }
            },
            manualRender: function(url) {
                var splitUrl = this.url.split('/terms');
                if (url && this.url != url) {
                    if (splitUrl.length > 1 && splitUrl[splitUrl.length - 1] == "") {
                        var checkUrl = splitUrl;
                        checkUrl.pop();
                        if (url != checkUrl) {
                            this.fetchCollection(url, true);
                        }
                    } else if (Utils.getUrlState.getQueryParams() && Utils.getUrlState.getQueryParams().load == "true") {
                        if (this.viewBased) {
                            Utils.setUrl({
                                url: "#!/taxonomy/detailCatalog" + url,
                                mergeBrowserUrl: false,
                                trigger: false,
                                updateTabState: true
                            });
                        }
                        this.fetchCollection(url, true);
                    } else {
                        this.fetchCollection(url, true);
                    }
                }
                if (!url && Utils.getUrlState.isTaxonomyTab()) {
                    this.selectFirstElement();
                }
            },
            changeArrowState: function(e) {
                var scope = this.$('[data-id="expandArrow"]');
                if (e) {
                    scope = $(e.currentTarget);
                }
                if (scope.is('a')) {
                    var url = scope.data('href');
                    scope = scope.parent().find("i.toggleArrow");
                } else if (scope.is('i')) {
                    var url = scope.parent().find("a").data('href');
                }
                if (scope.hasClass('fa-angle-down')) {
                    scope.toggleClass('fa-angle-right fa-angle-down');
                    this.ui.childList.hide();
                } else {
                    if (e && $(e.currentTarget).parents('li.parentChild').length) {
                        scope.parent('li').find('.tools .taxanomyloader').show();
                        this.url = url;
                        this.fetchCollection(url, true);
                        if (this.viewBased) {
                            Utils.setUrl({
                                url: "#!/taxonomy/detailCatalog" + url,
                                mergeBrowserUrl: false,
                                trigger: true,
                                updateTabState: true
                            });
                        }
                    } else {
                        scope.toggleClass('fa-angle-right fa-angle-down');
                        this.ui.childList.show();
                    }
                }
            },
            fetchCollection: function(url, isParent) {
                if (url) {
                    this.url = url;
                } else {
                    var parentURL = this.ui.Parent.find('a').data('href');
                    if (parentURL) {
                        this.url = parentURL;
                    } else {
                        this.url = UrlLinks.taxonomiesApiUrl();
                    }
                }
                this.showLoader();
                if (isParent) {
                    this.parentCollection.url = this.url;
                    this.parentCollection.fullCollection.reset(undefined, { silent: true });
                    this.parentCollection.fetch({ reset: true, cache: true });
                } else {
                    this.childCollection.url = this.url + "?hierarchy/path:.";
                    this.childCollection.fullCollection.reset(undefined, { silent: true });
                    this.childCollection.fetch({ reset: true, cache: true });
                }
            },
            showLoader: function() {
                this.$('.taxonomyTree').find('li.active .tools .taxanomyloader').show();
                this.$('.contentLoading').show();
            },
            hideLoader: function() {
                this.$('.taxanomyloader').hide();
                this.$('.contentLoading').hide();
            },
            addActiveClass: function(e) {
                this.$('ul.taxonomyTree').find('li').removeClass('active');
                if (e.jquery) {
                    e.parent('li').addClass('active');
                } else {
                    if (e.currentTarget) {
                        $(e.currentTarget).parent('li').addClass('active');
                    } else {
                        if ($(e).parent.length) {
                            $(e).parent('li').addClass('active');
                        } else {
                            $(e).parents('li').addClass('active');
                        }
                    }
                }
            },
            fetchTaxanomyCollections: function() {
                this.taxanomy.fetch({ reset: true });
            },
            refreshButtonTaxanomy: function() {
                this.fetchTaxanomyCollections();
                var url = "";
                if (this.$('.taxonomyTree').find('.active').parents('.parentChild').length) {
                    url = this.$('.taxonomyTree').find('.active a').data('href').split("/").slice(0, -2).join("/");
                    this.refresh = this.$('.taxonomyTree').find('.active a').data('href');
                } else {
                    url = this.$('.taxonomyTree').find('.active a').data('href');
                    this.refresh = this.$('.taxonomyTree').find('.active a').data('href');
                }
                this.fetchCollection(url, true);
            },
            searchResult: function() {
                var that = this;
                _.each(this.taxanomy.models, function(model, key) {
                    var name = model.get('name');
                    that.termCollection.url = UrlLinks.taxonomiesTermsApiUrl(name)
                });
                this.termCollection.fetch({ reset: true });
            },
            termSearchData: function() {
                var that = this;
                var str = '<option></option>';
                this.termCollection.fullCollection.comparator = function(model) {
                    return model.get('name');
                };
                this.termCollection.fullCollection.sort().each(function(model) {
                    str += '<option>' + model.get('name') + '</option>';
                });
                this.ui.searchTermInput.html(str);
                // this.ui.searchTermInput.setAttribute('data-href' : that.termCollection.url);
                this.ui.searchTermInput.select2({
                    placeholder: "Search Term",
                    allowClear: true
                });
            },
            onSearchTerm: function() {
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        query: this.$('.taxonomyTree').find('li.active').find("a").data('name'),
                        searchType: "dsl",
                        dslChecked: true
                    },
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: true
                });
            },
            selectFirstElement: function() {
                var dataURL = this.$('.taxonomyTree').find('li[data-id="Parent"]').find("a").data('href');
                if (dataURL) {
                    this.url = dataURL;
                    if (this.viewBased && Utils.getUrlState.isTaxonomyTab()) {
                        Utils.setUrl({
                            url: "#!/taxonomy/detailCatalog" + dataURL,
                            mergeBrowserUrl: false,
                            trigger: true,
                            updateTabState: true
                        });
                    }
                }
            },
            generateTree: function(isParent) {
                var parentLi = "",
                    childLi = "",
                    that = this;

                function createTaxonomy(url) {
                    var href = false;
                    _.each(that.parentCollection.fullCollection.models, function(model, key) {

                        if (model.get('terms')) {
                            href = model.get('terms').href;
                        } else if (model.get('href')) {
                            href = model.get('href') + "/terms";
                        }
                        var hrefUrl = "/api" + model.get('href').split("/api")[1];
                        if (hrefUrl) {
                            var backUrlCheck = hrefUrl.split("taxonomies/");
                            if (backUrlCheck.length > 1) {
                                if (backUrlCheck[1].split("/terms").length <= 1) {
                                    that.ui.backTaxanomy.hide();
                                } else {
                                    that.ui.backTaxanomy.show();
                                }
                            }
                        }
                        var name = Utils.checkTagOrTerm(model.get('name'), true);
                        if (name.name) {
                            if (that.viewBased) {
                                parentLi = '<div class="tools"><i class="fa fa-refresh fa-spin-custom taxanomyloader"></i><i class="fa fa-ellipsis-h termPopover"></i></div><i class="fa fa-angle-right toggleArrow" data-id="expandArrow" data-href="' + hrefUrl + '"></i><a href="javascript:void(0)" data-href="' + hrefUrl + '" data-name="`' + model.get('name') + '`">' + name.name + '</a>';
                            } else {
                                parentLi = '<div class="tools"><i class="fa fa-refresh fa-spin-custom taxanomyloader"></i></div><i class="fa fa-angle-right toggleArrow" data-id="expandArrow" data-href="' + hrefUrl + '"></i><a href="javascript:void(0)" data-href="' + hrefUrl + '" data-name="`' + model.get('name') + '`">' + name.name + '</a>';
                            }
                        }
                    });
                    if (href) {
                        var hrefUrl = "/api" + href.split("/api")[1];
                        that.fetchCollection(hrefUrl);
                    }
                    that.ui.childList.html('');
                    that.ui.Parent.addClass('active');
                    that.ui.Parent.html(parentLi);
                }

                function createTerm() {
                    that.childCollection.fullCollection.comparator = function(model) {
                        return model.get('name').toLowerCase();
                    };
                    that.childCollection.fullCollection.sort().each(function(model, key) {
                        var name = Utils.checkTagOrTerm(model.get('name'), true);
                        var hrefUrl = "/api" + model.get('href').split("/api")[1];
                        if (name.name) {
                            if (that.viewBased) {
                                childLi += '<li class="children"><div class="tools"><i class="fa fa-refresh fa-spin-custom taxanomyloader"></i><i class="fa fa-ellipsis-h termPopover" ></i></div><i class="fa fa-angle-right toggleArrow" data-id="expandArrow" data-href="' + hrefUrl + '"></i><a href="javascript:void(0)" data-href="' + hrefUrl + '" data-name="`' + model.get('name') + '`">' + name.name + '</a></li>';
                            } else {
                                childLi += '<li class="children"><div class="tools"><i class="fa fa-refresh fa-spin-custom taxanomyloader"></i></div><i class="fa fa-angle-right toggleArrow" data-id="expandArrow" data-href="' + hrefUrl + '"></i><a href="javascript:void(0)" data-href="' + hrefUrl + '" data-name="`' + model.get('name') + '`">' + name.name + '</a></li>';
                            }
                        }
                    });
                    that.ui.childList.html(childLi);
                }
                if (isParent) {
                    createTaxonomy();
                } else {
                    this.changeArrowState();
                    createTerm();
                    if (Utils.getUrlState.isInitial() || Utils.getUrlState.getQueryUrl().lastValue == "taxonomy") {
                        this.selectFirstElement();
                    }
                    if (this.refresh) {
                        this.addActiveClass(this.$('.taxonomyTree').find('a[data-href="' + this.refresh + '"]'));
                        this.refresh = undefined;
                    }
                }
                this.hideLoader();
                if (this.viewBased) {


                    Utils.generatePopover({
                        el: this.$('.termPopover'),
                        container: this.$el,
                        popoverOptions: {
                            content: function() {
                                var lis = "<li class='listTerm'><i class='fa fa-plus'></i> <a href='javascript:void(0)' data-fn='onAddTerm'>Create Subterm</a></li>";
                                var termDataURL = Utils.getUrlState.getQueryUrl().hash.split("terms");
                                if (termDataURL.length > 1) {
                                    lis = "<li class='listTerm' ><i class='fa fa-search'></i> <a href='javascript:void(0)' data-fn='onSearchTerm'>Search Assets</a></li>" + lis;
                                    lis += "<li class='listTerm'><i class='fa fa-trash'></i> <a href='javascript:void(0)' data-fn='deleteTerm'>Delete Term</a></li>";
                                }
                                return "<ul>" + lis + "</ul>";
                            }
                        }
                    }).parent('.tools').off('click').on('click', 'li', function(e) {
                        e.stopPropagation();
                        that.$('.termPopover').popover('hide');
                        that[$(this).find('a').data('fn')](e);
                    });
                }
            },
            onAddTerm: function(e) {
                var that = this;
                require([
                    'views/business_catalog/AddTermLayoutView',
                    'modules/Modal'
                ], function(AddTermLayoutView, Modal) {
                    var view = new AddTermLayoutView({
                        url: that.$('.taxonomyTree').find('li.active').find(">a[data-name]").data("href"),
                        model: new that.parentCollection.model()
                    });
                    var modal = new Modal({
                        title: 'Create Sub-term',
                        content: view,
                        okCloses: true,
                        showFooter: true,
                        allowCancel: true,
                        okText: 'Create',
                    }).open();
                    modal.$el.find('button.ok').attr('disabled', true);
                    modal.on('ok', function() {
                        that.saveAddTerm(view);
                    });
                    view.ui.termName.on('keyup', function() {
                        if (this.value.indexOf(' ') >= 0) {
                            modal.$el.find('button.ok').prop('disabled', true);
                            view.ui.termName.addClass("addTermDisable");
                            view.$('.alertTerm').show();
                        } else {
                            modal.$el.find('button.ok').prop('disabled', false);
                            view.ui.termName.removeClass("addTermDisable");
                            view.$('.alertTerm').hide();
                        }
                    });
                    view.on('closeModal', function() {
                        modal.trigger('cancel');
                    });

                });
            },
            saveAddTerm: function(view) {
                var that = this;
                var url = view.url;
                view.model.url = url + "/terms/" + view.ui.termName.val();
                this.showLoader();
                view.model.set({ description: view.ui.termDetail.val() }).save(null, {
                    success: function(model, response) {
                        that.create = true;
                        that.fetchTaxanomyCollections();
                        that.fetchCollection(url, true);
                        Utils.notifySuccess({
                            content: "Term " + view.ui.termName.val() + Messages.addSuccessMessage
                        });
                    },
                    complete: function() {
                        that.hideLoader();
                    }
                });
            },
            deleteTerm: function(e) {
                var termName = this.$('.taxonomyTree').find('li.active a').data("name"),
                    assetName = $(e.target).data("assetname"),
                    that = this,
                    modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Delete: " + "<b>" + _.escape(termName) + "?</b></div>" +
                            "<p class='termNote'>Assets mapped to this term will be unclassified.</p>",
                        titleMessage: Messages.deleteTerm,
                        buttonText: "Delete"
                    });
                modal.on('ok', function() {
                    that.deleteTermData(e);
                });
                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            deleteTermData: function(e) {
                var that = this;
                this.showLoader();
                require(['models/VCatalog'], function(VCatalog) {
                    var termModel = new VCatalog(),
                        url = that.$('.taxonomyTree').find('li.active a').data('href');
                    var termName = that.$('.taxonomyTree').find('li.active a').text();
                    termModel.deleteTerm(url, {
                        skipDefaultError: true,
                        success: function(data) {
                            Utils.notifySuccess({
                                content: "Term " + termName + Messages.deleteSuccessMessage
                            });
                            var termURL = url.split("/").slice(0, -2).join("/");
                            if (that.viewBased) {
                                Utils.setUrl({
                                    url: "#!/taxonomy/detailCatalog" + termURL,
                                    mergeBrowserUrl: false,
                                    trigger: true,
                                    updateTabState: true
                                });
                            }
                            that.fetchCollection(termURL, true);
                        },
                        cust_error: function(model, response) {
                            var message = "Term " + termName + Messages.deleteErrorMessage;
                            if (response && response.responseJSON) {
                                message = response.responseJSON.errorMessage;
                            }
                            Utils.notifyError({
                                content: message
                            });
                        },
                        complete: function() {
                            that.hideLoader();
                        }
                    });
                });
            },
            moveTerm: function() {
                var that = this;
                require([
                    'views/business_catalog/MoveTermLayoutView',
                    'modules/Modal'
                ], function(MoveTermLayoutView, Modal) {
                    var view = new MoveTermLayoutView({
                        taxanomyCollection: that.collection
                    });
                    var modal = new Modal({
                        title: 'Move Term',
                        content: view,
                        okCloses: true,
                        showFooter: true,
                        allowCancel: true,
                        okText: 'Move',
                    }).open();
                    // modal.on('ok', function() {
                    //     that.saveAddTerm(view);
                    // });
                    view.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            },
            createDefaultTaxonomy: function() {
                var that = this;
                require([
                    'views/business_catalog/AddTermLayoutView',
                    'modules/Modal'
                ], function(AddTermLayoutView, Modal) {
                    var view = new AddTermLayoutView({
                        url: UrlLinks.taxonomiesApiUrl(),
                        model: new that.parentCollection.model(),
                        defaultTerm: true
                    });
                    var modal = new Modal({
                        title: 'Taxonomy',
                        content: view,
                        okCloses: true,
                        showFooter: true,
                        allowCancel: true,
                        okText: 'Create',
                    }).open();
                    modal.$el.find('button.ok').attr('disabled', true);
                    modal.on('ok', function() {
                        that.saveDefaultTaxonomy(view);
                    });
                    view.ui.termName.on('keyup', function() {
                        if (this.value.indexOf(' ') >= 0) {
                            modal.$el.find('button.ok').prop('disabled', true);
                            view.ui.termName.addClass("addTermDisable");
                            view.$('.alertTerm').show();
                        } else {
                            modal.$el.find('button.ok').prop('disabled', false);
                            view.ui.termName.removeClass("addTermDisable");
                            view.$('.alertTerm').hide();
                        }
                    });
                    view.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            },
            saveDefaultTaxonomy: function(view) {
                var that = this;
                var url = view.url;
                view.model.url = url + "/" + view.ui.termName.val();
                this.showLoader();
                view.model.set({ description: view.ui.termDetail.val() }).save(null, {
                    skipDefaultError: true,
                    success: function(model, response) {
                        that.fetchCollection(view.model.url, true);
                        Utils.notifySuccess({
                            content: "Default taxonomy " + view.ui.termName.val() + Messages.addSuccessMessage
                        });
                    },
                    cust_error: function(model, response) {
                        Utils.notifyError({
                            content: "Default taxonomy " + view.ui.termName.val() + Messages.addErrorMessage
                        });
                    },
                    complete: function() {
                        that.hideLoader();
                    }
                });
            }
        });
    return TreeLayoutView;
});