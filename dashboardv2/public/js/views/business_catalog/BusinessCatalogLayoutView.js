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
    'hbs!tmpl/business_catalog/BusinessCatalogLayoutView_tmpl',
    'utils/Utils',
    'collection/VCatalogList',
    'utils/CommonViewFunction'
], function(require, Backbone, BusinessCatalogLayoutViewTmpl, Utils, VCatalogList, CommonViewFunction) {
    'use strict';

    var BusinessCatalogLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends BusinessCatalogLayoutView */
        {
            _viewName: 'BusinessCatalogLayoutView',

            template: BusinessCatalogLayoutViewTmpl,

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
                refreshTaxanomy: '[data-id="refreshTaxanomy"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events['dblclick ' + this.ui.liClick] = function(e) {
                    $(e.currentTarget).parent('li').find('.tools .taxanomyloader').show();
                    this.singleClick = false;
                    this.forwardClick(e, true);
                };
                events['click ' + this.ui.liClick] = function(e) {
                    this.dblClick = false;
                    this.forwardClick(e);
                };
                events['click ' + this.ui.backTaxanomy] = 'backButtonTaxanomy';
                events['click ' + this.ui.refreshTaxanomy] = 'refreshButtonTaxanomy';
                events['click ' + this.ui.expandArrow] = 'changeArrowState';
                events["change " + this.ui.searchTermInput] = function() {
                    this.singleClick = false;
                    var termUrl = this.termCollection.url.split("/", 5).join("/") + "/" + this.ui.searchTermInput.val().split(".").join("/terms/");
                    this.forwardClick(undefined, true, termUrl);
                };
                return events;
            },
            /**
             * intialize a new BusinessCatalogLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'url'));
                this.parentCollection = new VCatalogList();
                this.childCollection = new VCatalogList();
                this.taxanomy = new VCatalogList();
                this.termCollection = new VCatalogList();
                this.dblClick = false;
                this.singleClick = false;
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.parentCollection, 'reset', function() {
                    this.dblClick = false;
                    if (this.parentCollection.fullCollection.models.length) {
                        this.generateTree(true);
                    } else {
                        if (Utils.getUrlState.isTaxonomyTab() || Utils.getUrlState.isInitial()) {
                            this.createDefaultTaxonomy();
                        }
                    }
                }, this);
                this.listenTo(this.childCollection, 'reset', function() {
                    this.dblClick = false;
                    this.generateTree();
                }, this);
                this.listenTo(this.taxanomy, 'reset', function() {
                    this.searchResult();
                }, this);
                this.listenTo(this.termCollection, 'reset', function() {
                    this.termSearchData();
                }, this);
                this.listenTo(this.childCollection, 'error', function(model, response) {
                    if (response && response.responseJSON && response.responseJSON.message) {
                        Utils.notifyError({
                            content: response.responseJSON.message
                        });
                    }
                    this.hideLoader();
                }, this);
                this.listenTo(this.parentCollection, 'error', function(model, response) {
                    if (response && response.responseJSON && response.responseJSON.message) {
                        Utils.notifyError({
                            content: response.responseJSON.message
                        });
                    }
                    this.hideLoader();
                }, this);
            },
            onRender: function() {
                var that = this;
                this.bindEvents();
                that.ui.backTaxanomy.hide();

                this.fetchCollection(this.url, true);

                $('body').on("click", '.termPopoverList li', function(e) {
                    that[$(this).find("a").data('fn')](e);
                });
                $('body').click(function(e) {
                    if ($('.termPopoverList').length) {
                        if ($(e.target).hasClass('termPopover')) {
                            return;
                        }
                        that.$('.termPopover').popover('hide');
                    }
                });
                this.fetchTaxanomyCollections();
            },
            manualRender: function(url, isParent, back) {
                if (back) {
                    this.backButton = back;
                }
                if (this.ui.Parent.children().length <= 0 || this.backButton) {
                    this.fetchCollection(url, isParent);
                }
                if (url && isParent && !this.backButton) {
                    this.fetchCollection(url, isParent);
                }
                if (!url && !back && isParent) {
                    var url = this.$('.taxonomyTree').find('.active a').data('href');
                    this.forwardClick(undefined, undefined, url);
                }
                if (this.backButton) {
                    this.backButton = false;
                }
            },
            changeArrowState: function(e) {
                var scope = this.$('[data-id="expandArrow"]');
                if (e) {
                    scope = $(e.currentTarget);
                }
                if (scope.hasClass('fa-chevron-down')) {
                    scope.removeClass('fa-chevron-down');
                    scope.addClass('fa-chevron-right');
                    this.addActiveClass(scope[0]);
                    this.ui.childList.hide();
                } else {
                    if (e && $(e.currentTarget).parents('li.parentChild').length) {
                        scope.parent('li').find('.tools .taxanomyloader').show();
                        this.forwardClick(e, true);
                    } else {
                        scope.addClass('fa-chevron-down');
                        scope.removeClass('fa-chevron-right');
                        this.singleClick = false;
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
                        this.url = "api/atlas/v1/taxonomies";
                    }

                }
                this.showLoader();
                if (isParent) {
                    this.parentCollection.url = this.url;
                    this.parentCollection.fullCollection.reset(undefined, { silent: true });
                    this.parentCollection.fetch({ reset: true });
                } else {
                    this.childCollection.url = this.url + "?hierarchy/path:.";
                    this.childCollection.fullCollection.reset(undefined, { silent: true });
                    this.childCollection.fetch({ reset: true });
                }
            },
            showLoader() {
                this.$('.taxonomyTree').find('li.active .tools .taxanomyloader').show();
                this.$('.contentLoading').show();
            },
            hideLoader() {
                this.$('.taxanomyloader').hide();
                this.$('.contentLoading').hide();
            },
            forwardClick: function(e, forward, url) {
                var hrefUrl = "";
                if (e) {
                    hrefUrl = $(e.currentTarget).data('href');
                }
                if (url) {
                    hrefUrl = url;
                }
                if (!e && !url) {
                    var dataHref = this.ui.Parent.find('a').data('href');
                    if (dataHref) {
                        hrefUrl = dataHref;
                    }
                }
                if (forward) {
                    this.dblClick = true;
                    this.ui.childList.show();
                    this.fetchCollection(hrefUrl, true);
                } else {
                    this.singleClick = true;
                }
                if (hrefUrl.length > 1) {
                    Utils.setUrl({
                        url: '#!/taxonomy/detailCatalog' + hrefUrl,
                        mergeBrowserUrl: false,
                        updateTabState: function() {
                            return { taxonomyUrl: this.url, stateChanged: false };
                        },
                        trigger: true
                    });
                }
                if (e) {
                    this.addActiveClass(e);
                }
            },
            addActiveClass: function(e) {
                $(e.currentTarget).parents('ul.taxonomyTree').find('li').removeClass('active');
                $(e.currentTarget).parent('li').addClass('active');
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
                        var name = Utils.checkTagOrTerm(model.get('name'));
                        parentLi = '<div class="tools"><i class="fa fa-refresh fa-spin-custom taxanomyloader"></i><i class="fa fa-ellipsis-h termPopover"></i></div><i class="fa fa-chevron-right toggleArrow" data-id="expandArrow" data-href="' + hrefUrl + '"></i><a href="javascript:void(0)" data-href="' + hrefUrl + '" data-name="`' + model.get('name') + '`">' + name.name + '</a>';
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
                    _.each(that.childCollection.fullCollection.models, function(model, key) {
                        var name = Utils.checkTagOrTerm(model.get('name'));
                        var hrefUrl = "/api" + model.get('href').split("/api")[1]
                        childLi += '<li class="children"><div class="tools"><i class="fa fa-refresh fa-spin-custom taxanomyloader"></i><i class="fa fa-ellipsis-h termPopover" ></i></div><i class="fa fa-chevron-right toggleArrow" data-id="expandArrow" data-href="' + hrefUrl + '"></i><a href="javascript:void(0)" data-href="' + hrefUrl + '" data-name="`' + model.get('name') + '`">' + name.name + '</a></li>';
                    });
                    that.ui.childList.html(childLi);
                }

                if (isParent) {
                    createTaxonomy();
                } else {
                    this.changeArrowState();
                    createTerm();
                }
                if (this.refresh) {
                    this.$('.taxonomyTree').find('a[data-href="' + this.refresh + '"]').parent().addClass('active');
                    this.refresh = undefined;
                }
                this.hideLoader();
                this.$('.termPopover').popover({
                    placement: 'bottom',
                    html: true,
                    trigger: 'manual',
                    container: 'body',
                    content: function() {
                        var li = "<li class='listTerm' ><i class='fa fa-search'></i> <a href='javascript:void(0)' data-fn='onSearchTerm'>Search Assets</a></li>" +
                            "<li class='listTerm'><i class='fa fa-plus'></i> <a href='javascript:void(0)' data-fn='onAddTerm'>Add Subterm</a></li>";
                        /* "<li class='listTerm' ><i class='fa fa-arrow-right'></i> <a href='javascript:void(0)' data-fn='moveTerm'>Move Term</a></li>" +
                         "<li class='listTerm' ><i class='fa fa-edit'></i> <a href='javascript:void(0)' data-fn='onEditTerm'>Edit Term</a></li>" +*/
                        var termDataURL = Utils.getUrlState.getQueryUrl().hash.split("terms");
                        if (termDataURL.length > 1) {
                            li += "<li class='listTerm'><i class='fa fa-trash'></i> <a href='javascript:void(0)' data-fn='deleteTerm'>Delete Term</a></li>"
                        }
                        return "<ul class='termPopoverList'>" + li + "</ul>";
                    }
                });
                this.$('.termPopover').off('click').on('click', function(e) {
                    // if any other popovers are visible, hide them
                    e.preventDefault();
                    that.$('.termPopover').not(this).popover('hide');
                    $(this).popover('toggle');
                });
                if (Utils.getUrlState.isInitial()) {
                    this.forwardClick();
                }
            },
            onAddTerm: function(e) {
                var that = this;
                require([
                    'views/business_catalog/AddTermLayoutView',
                    'modules/Modal'
                ], function(AddTermLayoutView, Modal) {
                    var view = new AddTermLayoutView({
                        url: that.$('.taxonomyTree').find('li.active').find("a").data("href"),
                        model: new that.parentCollection.model()
                    });
                    var modal = new Modal({
                        title: 'Add Term',
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
                        } else {
                            modal.$el.find('button.ok').prop('disabled', false);
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
                        that.forwardClick(undefined, true, url);
                        //that.fetchCollection(that.url);
                        Utils.notifySuccess({
                            content: "Term " + view.ui.termName.val() + " Created successfully"
                        });
                    },
                    error: function(model, response) {
                        Utils.notifyError({
                            content: "Term " + view.ui.termName.val() + " could not be Created"
                        });
                    },
                    complete: function() {
                        that.hideLoader();
                    }
                });
            },
            deleteTerm: function(e) {
                var termName = this.$('.taxonomyTree').find('li.active a').data("name"),
                    that = this,
                    modal = CommonViewFunction.deleteTagModel(termName);
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
                        beforeSend: function() {},
                        success: function(data) {
                            Utils.notifySuccess({
                                content: "Term " + termName + " has been deleted successfully"
                            });
                            var termURL = url.split("/").slice(0, -2).join("/");
                            that.forwardClick(undefined, true, termURL);
                        },
                        error: function(error, data, status) {
                            var message = "Term " + termName + " could not be deleted";
                            if (data.error) {
                                message = data.error;
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
            onSearchTerm: function() {
                this.showLoader();
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        query: this.$('.taxonomyTree').find('li.active').find("a").data('name'),
                        searchType: "dsl",
                        dslChecked: true
                    },
                    updateTabState: function() {
                        return { searchUrl: this.url, stateChanged: true };
                    },
                    mergeBrowserUrl: false,
                    trigger: true
                });
            },
            backButtonTaxanomy: function(e) {
                var that = this;
                this.dblClick = false;
                this.backButton = true;
                var dataURL = this.$('.taxonomyTree').find('li[data-id="Parent"]').find("a").data('href').split("/terms");
                var backUrl = dataURL.pop();
                if (dataURL.join("/terms").length) {
                    this.ui.backTaxanomy.show();
                    var currentURL = "!/taxonomy/detailCatalog" + dataURL.join("/terms");
                    Utils.setUrl({
                        url: currentURL,
                        mergeBrowserUrl: false,
                        trigger: true,
                        updateTabState: function() {
                            return { taxonomyUrl: currentURL, stateChanged: false };
                        }
                    });
                }
            },
            fetchTaxanomyCollections: function() {
                this.taxanomy.fetch({ reset: true });
            },
            searchResult: function() {
                var that = this;
                _.each(this.taxanomy.models, function(model, key) {
                    var name = model.get('name');
                    that.termCollection.url = "/api/atlas/v1/taxonomies/" + name + "/terms";
                });
                this.termCollection.fetch({ reset: true });
            },
            termSearchData: function() {
                var that = this;
                var str = '<option></option>';
                for (var j = 0; j < this.termCollection.models.length; j++) {
                    var terms = this.termCollection.models[j].attributes.name;
                    str += '<option>' + terms + '</option>';
                    this.ui.searchTermInput.html(str);
                }
                // this.ui.searchTermInput.setAttribute('data-href' : that.termCollection.url);
                this.ui.searchTermInput.select2({
                    placeholder: "Search Term",
                    allowClear: true
                });
            },
            refreshButtonTaxanomy: function() {
                this.fetchTaxanomyCollections();
                this.refresh = this.$('.taxonomyTree').find('.active a').data('href');
                this.fetchCollection(this.url);
                this.changeArrowState();
            },
            createDefaultTaxonomy: function() {
                var that = this;
                require([
                    'views/business_catalog/AddTermLayoutView',
                    'modules/Modal'
                ], function(AddTermLayoutView, Modal) {
                    var view = new AddTermLayoutView({
                        url: "/api/atlas/v1/taxonomies",
                        model: new that.parentCollection.model()
                    });
                    var modal = new Modal({
                        title: 'Default taxonomy',
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
                    view.ui.termName.attr("placeholder", "Default taxonomy name");
                    view.ui.termName.on('keyup', function() {
                        if (this.value.indexOf(' ') >= 0) {
                            modal.$el.find('button.ok').prop('disabled', true);
                        } else {
                            modal.$el.find('button.ok').prop('disabled', false);
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
                    success: function(model, response) {
                        that.fetchCollection(view.model.url, true);
                        that.forwardClick(undefined, undefined, view.model.url);
                        Utils.notifySuccess({
                            content: "Default taxonomy" + view.ui.termName.val() + " Created successfully"
                        });
                    },
                    error: function(error, data, status) {
                        Utils.notifyError({
                            content: "Default taxonomy " + view.ui.termName.val() + " could not be Created"
                        });
                    },
                    complete: function() {
                        that.hideLoader();
                    }
                });
            }
        });
    return BusinessCatalogLayoutView;
});
