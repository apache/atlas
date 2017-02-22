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
    'hbs!tmpl/search/SearchResultLayoutView_tmpl',
    'modules/Modal',
    'models/VEntity',
    'utils/Utils',
    'utils/Globals',
    'collection/VSearchList',
    'models/VCommon',
    'utils/CommonViewFunction',
    'utils/Messages',
    'utils/Enums',
    'utils/UrlLinks'
], function(require, Backbone, SearchResultLayoutViewTmpl, Modal, VEntity, Utils, Globals, VSearchList, VCommon, CommonViewFunction, Messages, Enums, UrlLinks) {
    'use strict';

    var SearchResultLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends SearchResultLayoutView */
        {
            _viewName: 'SearchResultLayoutView',

            template: SearchResultLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RTagLayoutView: "#r_tagLayoutView",
                RSearchLayoutView: "#r_searchLayoutView",
                REntityTableLayoutView: "#r_searchResultTableLayoutView",
            },

            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                addTag: '[data-id="addTag"]',
                addTerm: '[data-id="addTerm"]',
                showMoreLess: '[data-id="showMoreLess"]',
                showMoreLessTerm: '[data-id="showMoreLessTerm"]',
                paginationDiv: '[data-id="paginationDiv"]',
                previousData: "[data-id='previousData']",
                nextData: "[data-id='nextData']",
                pageRecordText: "[data-id='pageRecordText']",
                addAssignTag: "[data-id='addAssignTag']",
                editEntityButton: "[data-id='editEntityButton']",
                createEntity: "[data-id='createEntity']",
            },
            templateHelpers: function() {
                return {
                    entityCreate: Globals.entityCreate
                };
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.tagClick] = function(e) {
                    var scope = $(e.currentTarget);
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickTagCross(e);
                    } else {
                        if (scope.hasClass('term')) {
                            var url = scope.data('href').split(".").join("/terms/");
                            Globals.saveApplicationState.tabState.stateChanged = false;
                            Utils.setUrl({
                                url: '#!/taxonomy/detailCatalog' + UrlLinks.taxonomiesApiUrl() + '/' + url,
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        } else {
                            Utils.setUrl({
                                url: '#!/tag/tagAttribute/' + scope.text(),
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        }
                    }
                };
                events["click " + this.ui.addTag] = 'checkedValue';
                events["click " + this.ui.addTerm] = 'checkedValue';
                events["click " + this.ui.addAssignTag] = 'checkedValue';
                events["click " + this.ui.showMoreLess] = function(e) {
                    $(e.currentTarget).parents('tr').siblings().find("div.popover.popoverTag").hide();
                    $(e.currentTarget).parent().find("div.popover").toggle();
                    var positionContent = $(e.currentTarget).position();
                    positionContent.top = positionContent.top + 26;
                    positionContent.left = positionContent.left - 67;
                    $(e.currentTarget).parent().find("div.popover").css(positionContent);
                };
                events["click " + this.ui.showMoreLessTerm] = function(e) {
                    $(e.currentTarget).find('i').toggleClass('fa fa-angle-right fa fa-angle-up');
                    $(e.currentTarget).parents('.searchTerm').find('div.termTableBreadcrumb>div.showHideDiv').toggleClass('hide');
                    if ($(e.currentTarget).find('i').hasClass('fa-angle-right')) {
                        $(e.currentTarget).find('span').text('Show More');
                    } else {
                        $(e.currentTarget).find('span').text('Show less');
                    }
                };
                events["click " + this.ui.nextData] = "onClicknextData";
                events["click " + this.ui.previousData] = "onClickpreviousData";
                events["click " + this.ui.editEntityButton] = "onClickEditEntity";
                events["click " + this.ui.createEntity] = 'onClickCreateEntity';
                return events;
            },
            /**
             * intialize a new SearchResultLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'value', 'initialView', 'entityDefCollection'));
                var pagination = "";
                this.entityModel = new VEntity();
                this.searchCollection = new VSearchList();
                this.limit = 25;
                this.firstFetch = true;
                this.asyncFetchCounter = 0;
                this.offset = 0;
                this.commonTableOptions = {
                    collection: this.searchCollection,
                    includeFilter: false,
                    includePagination: false,
                    includePageSize: false,
                    includeFooterRecords: false,
                    includeSizeAbleColumns: false,
                    gridOpts: {
                        emptyText: 'No Record found!',
                        className: 'table table-hover backgrid table-quickMenu'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.bindEvents();
                this.bradCrumbList = [];
                this.arr = [];
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.searchCollection, 'backgrid:selected', function(model, checked) {
                    this.arr = [];
                    if (checked === true) {
                        model.set("isEnable", true);
                    } else {
                        model.set("isEnable", false);
                    }
                    this.searchCollection.find(function(item) {
                        if (item.get('isEnable')) {
                            var term = [];
                            if (that.searchCollection.queryType == "DSL") {
                                var obj = item.toJSON();
                            } else {
                                var obj = item.get('entity');
                            }
                            that.arr.push({
                                id: obj.guid,
                                model: obj
                            });
                        }
                    });
                    if (this.arr.length > 0) {
                        this.$('.searchResult').find(".inputAssignTag.multiSelect").show();
                        this.$('.searchResult').find(".inputAssignTag.multiSelectTag").show();
                    } else {
                        this.$('.searchResult').find(".inputAssignTag.multiSelect").hide();
                        this.$('.searchResult').find(".inputAssignTag.multiSelectTag").hide();
                    }
                });
                this.listenTo(this.searchCollection, "error", function(model, response) {
                    this.$('.fontLoader').hide();
                    var responseJSON = response ? response.responseJSON : response;
                    if (response && responseJSON && (responseJSON.errorMessage || responseJSON.message || responseJSON.error)) {
                        Utils.notifyError({
                            content: responseJSON.errorMessage || responseJSON.message || responseJSON.error
                        });
                    } else {
                        Utils.notifyError({
                            content: "Invalid Expression : " + model.queryParams.query
                        });
                    }
                }, this);
            },
            onRender: function() {
                if (!this.initialView) {
                    var value = {},
                        that = this;
                    if (this.value) {
                        value = this.value;
                    } else {
                        value = {
                            'query': '',
                            'searchType': 'fulltext'
                        };
                    }
                    this.fetchCollection(value);
                    $('body').click(function(e) {
                        var iconEvnt = e.target.nodeName;
                        if (that.$('.popoverContainer').length) {
                            if ($(e.target).hasClass('tagDetailPopover') || iconEvnt == "I") {
                                return;
                            }
                            that.$('.popover.popoverTag').hide();
                        }
                    });
                } else {
                    if (Globals.entityTypeConfList) {
                        this.$(".entityLink").show();
                    }
                }
            },
            fetchCollection: function(value) {
                var that = this;
                if (value && (value.query === undefined || value.query.trim() === "")) {
                    return;
                }
                this.showLoader();
                if (Globals.searchApiCallRef && Globals.searchApiCallRef.readyState === 1) {
                    Globals.searchApiCallRef.abort();
                }
                $.extend(this.searchCollection.queryParams, { limit: this.limit });
                if (value) {
                    if (value.searchType) {
                        this.searchCollection.url = UrlLinks.searchApiUrl(value.searchType);
                        $.extend(this.searchCollection.queryParams, { limit: this.limit });
                        this.offset = 0;
                    }
                    if (Utils.getUrlState.isTagTab()) {
                        this.searchCollection.url = UrlLinks.searchApiUrl(Enums.searchUrlType.DSL);
                    }
                    _.extend(this.searchCollection.queryParams, { 'query': value.query.trim() });
                }
                Globals.searchApiCallRef = this.searchCollection.fetch({
                    skipDefaultError: true,
                    success: function() {
                        if (that.searchCollection.models.length < that.limit) {
                            that.ui.nextData.attr('disabled', true);
                        }
                        Globals.searchApiCallRef = undefined;
                        if (that.offset === 0) {
                            that.pageFrom = 1;
                            that.pageTo = that.limit;
                            if (that.searchCollection.length > 0) {
                                that.ui.pageRecordText.html("Showing " + that.pageFrom + " - " + that.searchCollection.length);
                            }
                        } else if (that.searchCollection.models.length && !that.previousClick) {
                            //on next click, adding "1" for showing the another records..
                            that.pageFrom = that.pageTo + 1;
                            that.pageTo = that.pageTo + that.searchCollection.models.length;
                            if (that.pageFrom && that.pageTo) {
                                that.ui.pageRecordText.html("Showing " + that.pageFrom + " - " + that.pageTo);
                            }
                        } else if (that.previousClick) {
                            that.pageTo = (that.pageTo - (that.pageTo - that.pageFrom)) - 1;
                            //if limit is 0 then result is change to 1 because page count is showing from 1
                            that.pageFrom = (that.pageFrom - that.limit === 0 ? 1 : that.pageFrom - that.limit);
                            that.ui.pageRecordText.html("Showing " + that.pageFrom + " - " + that.pageTo);
                        }
                        if (that.searchCollection.models.length === 0) {
                            that.checkTableFetch();
                            that.offset = that.offset - that.limit;
                            if (that.firstFetch) {
                                that.renderTableLayoutView();
                            }
                        }
                        if (that.firstFetch) {
                            that.firstFetch = false;
                        }
                        if (that.offset < that.limit) {
                            that.ui.previousData.attr('disabled', true);
                        }
                        // checking length for not rendering the table
                        if (that.searchCollection.models.length) {
                            that.renderTableLayoutView();
                        }
                        var resultData = 'Results for <b>' + _.escape(that.searchCollection.queryParams.query) + '</b>';
                        var multiAssignDataTag = '<a href="javascript:void(0)" class="inputAssignTag multiSelectTag assignTag" style="display:none" data-id="addAssignTag"><i class="fa fa-plus"></i>' + " " + 'Assign Tag</a>';
                        var resultText = that.searchCollection.queryParams.query;
                        var multiAssignDataTerm = "",
                            createEntityTag = "";
                        if (Globals.taxonomy) {
                            multiAssignDataTerm = '<a href="javascript:void(0)" class="inputAssignTag multiSelect" style="display:none" data-id="addTerm"><i class="fa fa-folder-o"></i>' + " " + 'Assign Term</a>';
                        }
                        if (Globals.entityCreate && (resultText.indexOf("\`") != 0) && Globals.entityTypeConfList) {
                            createEntityTag = "<p>If you do not find the entity in search result below then you can" + '<a href="javascript:void(0)" data-id="createEntity"> create new entity</a></p>';
                        }
                        that.$('.searchResult').html(resultData + multiAssignDataTag + multiAssignDataTerm + createEntityTag);
                    },
                    silent: true,
                    reset: true
                });
            },
            renderTableLayoutView: function(col) {
                var that = this,
                    count = 5;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columns = new Backgrid.Columns(that.getFixedDslColumn());
                    that.REntityTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: columns
                    })));
                    that.ui.paginationDiv.show();
                    that.$('.searchResult').find(".inputAssignTag.multiSelect").hide();
                    that.renderBreadcrumb();
                });
            },
            renderBreadcrumb: function() {
                var that = this;
                _.each(this.bradCrumbList, function(object) {
                    _.each(object.value, function(subObj) {
                        var scopeObject = that.$('[dataterm-id="' + object.scopeId + '"]').find('[dataterm-name="' + subObj.name + '"] .liContent');
                        CommonViewFunction.breadcrumbMaker({ urlList: subObj.valueUrl, scope: scopeObject });
                    });
                });
            },
            checkTableFetch: function() {
                if (this.asyncFetchCounter <= 0) {
                    this.$('div[data-id="r_tableSpinner"]').removeClass('show');
                    this.hideLoader();
                }
            },
            getFixedDslColumn: function() {
                var that = this,
                    nameCheck = 0,
                    col = {};
                col['Check'] = {
                    name: "selected",
                    label: "",
                    cell: "select-row",
                    headerCell: "select-all"
                };

                col['displayText'] = {
                    label: "Name",
                    cell: "html",
                    editable: false,
                    sortable: false,
                    className: "searchTableName",
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            if (that.searchCollection.queryType == "DSL") {
                                var obj = model.toJSON();
                            } else {
                                var obj = model.get('entity');
                            }
                            var nameHtml = "";
                            var name = (_.escape(obj.attributes && obj.attributes.name ? obj.attributes.name : null) || _.escape(obj.displayText) || obj.guid)
                            if (obj.guid) {
                                nameHtml = '<a title="' + name + '" href="#!/detailPage/' + obj.guid + '">' + name + '</a>';
                            } else {
                                nameHtml = '<a title="' + name + '">' + name + '</a>';
                            }
                            if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                                nameHtml += '<button type="button" title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                                return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                            } else {
                                if (Globals.entityUpdate) {
                                    if (Globals.entityTypeConfList && _.isEmptyArray(Globals.entityTypeConfList)) {
                                        nameHtml += '<button title="Edit" data-id="editEntityButton"  data-giud= "' + obj.guid + '" class="btn btn-atlasAction btn-atlas editBtn"><i class="fa fa-pencil"></i></button>'
                                    } else {
                                        if (_.contains(Globals.entityTypeConfList, obj.typeName)) {
                                            nameHtml += '<button title="Edit" data-id="editEntityButton"  data-giud= "' + obj.guid + '" class="btn btn-atlasAction btn-atlas editBtn"><i class="fa fa-pencil"></i></button>'
                                        }
                                    }
                                }
                                return nameHtml;
                            }
                        }
                    })
                };

                col['description'] = {
                    label: "Description",
                    cell: "String",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            if (that.searchCollection.queryType == "DSL") {
                                var obj = model.toJSON();
                            } else {
                                var obj = model.get('entity');
                            }
                            if (obj && obj.attributes && obj.attributes.description) {
                                return obj.attributes.description;
                            }
                        }
                    })
                };
                col['typeName'] = {
                    label: "Type",
                    cell: "Html",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            if (that.searchCollection.queryType == "DSL") {
                                var obj = model.toJSON();
                            } else {
                                var obj = model.get('entity');
                            }
                            if (obj && obj.typeName) {
                                return '<a title="Search ' + obj.typeName + '" href="#!/search/searchResult?query=' + obj.typeName + ' &searchType=dsl&dslChecked=true">' + obj.typeName + '</a>';
                            }
                        }
                    })
                };
                col['owner'] = {
                    label: "Owner",
                    cell: "String",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            if (that.searchCollection.queryType == "DSL") {
                                var obj = model.toJSON();
                            } else {
                                var obj = model.get('entity');
                            }
                            if (obj && obj.attributes && obj.attributes.owner) {
                                return obj.attributes.owner;
                            }
                        }
                    })
                };
                col['tag'] = {
                    label: "Tags",
                    cell: "Html",
                    editable: false,
                    sortable: false,
                    orderable: true,
                    className: 'searchTag',
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            if (that.searchCollection.queryType == "DSL") {
                                var obj = model.toJSON();
                            } else {
                                var obj = model.get('entity');
                            }
                            if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                                return '<div class="readOnly">' + CommonViewFunction.tagForTable(obj); + '</div>';
                            } else {
                                return CommonViewFunction.tagForTable(obj);
                            }

                        }
                    })
                };
                if (Globals.taxonomy) {
                    col['terms'] = {
                        label: "Terms",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        orderable: true,
                        className: 'searchTerm',
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                if (that.searchCollection.queryType == "DSL") {
                                    var obj = model.toJSON();
                                } else {
                                    var obj = model.get('entity');
                                }
                                var returnObject = CommonViewFunction.termTableBreadcrumbMaker(obj);
                                if (returnObject.object) {
                                    that.bradCrumbList.push(returnObject.object);
                                }
                                if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                                    return '<div class="readOnly">' + returnObject.html + '</div>';
                                } else {
                                    return returnObject.html;
                                }
                            }
                        })
                    };
                }
                that.checkTableFetch();
                return this.searchCollection.constructor.getTableCols(col, this.searchCollection);
            },
            addTagModalView: function(guid, multiple) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        guid: guid,
                        multiple: multiple,
                        callback: function() {
                            that.fetchCollection();
                            that.arr = [];
                        },
                        tagList: that.getTagList(guid, multiple),
                        showLoader: that.showLoader.bind(that),
                        hideLoader: that.hideLoader.bind(that)
                    });
                });
            },
            getTagList: function(guid, multiple) {
                var that = this;
                if (!multiple || multiple.length === 0) {
                    var modal = this.searchCollection.find(function(item) {
                        if (that.searchCollection.queryType == "DSL") {
                            var obj = item.toJSON();
                        } else {
                            var obj = item.get('entity');
                        }
                        if (obj.guid === guid) {
                            return true;
                        }
                    });
                    if (modal) {
                        if (that.searchCollection.queryType == "DSL") {
                            var obj = modal.toJSON();
                        } else {
                            var obj = modal.get('entity');
                        }
                    } else {
                        return [];
                    }
                    return obj.classificationNames;
                } else {
                    return [];
                }

            },
            showLoader: function() {
                this.$('.fontLoader').show();
                this.$('.searchTable').hide();
                this.$('.searchResult').hide();
            },
            hideLoader: function() {
                this.$('.fontLoader').hide();
                this.$('.searchTable').show();
                this.$('.searchResult').show();
            },
            checkedValue: function(e) {
                var guid = "",
                    that = this;
                var multiSelectTag = $(e.currentTarget).hasClass('assignTag');
                if (multiSelectTag) {
                    if (this.arr && this.arr.length && multiSelectTag) {
                        that.addTagModalView(guid, this.arr);
                    } else {
                        guid = that.$(e.currentTarget).data("guid");
                        that.addTagModalView(guid);
                    }
                } else {
                    if (this.arr && this.arr.length) {
                        that.addTermModalView(guid, this.arr);
                    } else {
                        guid = that.$(e.currentTarget).data("guid");
                        that.addTermModalView(guid);
                    }
                }
            },
            addTermModalView: function(guid, multiple) {
                var that = this;
                require([
                    'views/business_catalog/AddTermToEntityLayoutView',
                ], function(AddTermToEntityLayoutView) {
                    var view = new AddTermToEntityLayoutView({
                        guid: guid,
                        multiple: multiple,
                        callback: function() {
                            that.fetchCollection();
                            that.arr = [];
                        },
                        showLoader: that.showLoader.bind(that),
                        hideLoader: that.hideLoader.bind(that)
                    });
                });
            },
            onClickTagCross: function(e) {
                var tagName = $(e.target).data("name"),
                    guid = $(e.target).data("guid"),
                    assetName = $(e.target).data("assetname"),
                    tagOrTerm = $(e.target).data("type"),
                    that = this;
                if (tagOrTerm === "term") {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + assetName + " ?</b></div>",
                        titleMessage: Messages.removeTerm,
                        buttonText: "Remove"
                    });
                } else if (tagOrTerm === "tag") {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + assetName + " ?</b></div>",
                        titleMessage: Messages.removeTag,
                        buttonText: "Remove"
                    });
                }
                if (modal) {
                    modal.on('ok', function() {
                        that.deleteTagData(e, tagOrTerm);
                    });
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                }
            },
            deleteTagData: function(e, tagOrTerm) {
                var that = this,
                    tagName = $(e.target).data("name"),
                    guid = $(e.target).data("guid");
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': guid,
                    'tagOrTerm': tagOrTerm,
                    callback: function() {
                        that.fetchCollection();
                    }
                });
            },
            onClicknextData: function() {
                var that = this;
                this.ui.previousData.removeAttr("disabled");
                that.offset = that.offset + that.limit;
                $.extend(this.searchCollection.queryParams, {
                    offset: that.offset
                });
                this.previousClick = false;
                this.fetchCollection();
            },
            onClickpreviousData: function() {
                var that = this;
                this.ui.nextData.removeAttr("disabled");
                that.offset = that.offset - that.limit;
                $.extend(this.searchCollection.queryParams, {
                    offset: that.offset
                });
                this.previousClick = true;
                this.fetchCollection();
            },

            onClickEditEntity: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                var guid = $(e.currentTarget).data('giud');
                require([
                    'views/entity/CreateEntityLayoutView'
                ], function(CreateEntityLayoutView) {
                    var view = new CreateEntityLayoutView({
                        guid: guid,
                        entityDefCollection: that.entityDefCollection,
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
                });
            },
            onClickCreateEntity: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                require([
                    'views/entity/CreateEntityLayoutView'
                ], function(CreateEntityLayoutView) {
                    var view = new CreateEntityLayoutView({
                        entityDefCollection: that.entityDefCollection,
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
                });
            }
        });
    return SearchResultLayoutView;
});
