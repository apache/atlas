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
    'hbs!tmpl/schema/SchemaTableLayoutView_tmpl',
    'collection/VSchemaList',
    'utils/Utils',
    'utils/CommonViewFunction'
], function(require, Backbone, SchemaTableLayoutViewTmpl, VSchemaList, Utils, CommonViewFunction) {
    'use strict';

    var SchemaTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends SchemaTableLayoutView */
        {
            _viewName: 'SchemaTableLayoutView',

            template: SchemaTableLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RTagLayoutView: "#r_tagLayoutView",
            },
            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                addTag: "[data-id='addTag']",
                addTerm: '[data-id="addTerm"]',
                showMoreLess: '[data-id="showMoreLess"]',
                showMoreLessTerm: '[data-id="showMoreLessTerm"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};

                events["click " + this.ui.addTag] = 'addTagModalView';
                events["click " + this.ui.addTerm] = 'checkedValue';
                events["click " + this.ui.tagClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickTagCross(e);
                    } else {
                        var value = e.currentTarget.text;
                        Utils.setUrl({
                            url: '#!/tag/tagAttribute/' + value,
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                    }
                };
                events["click " + this.ui.showMoreLess] = function(e) {
                    $(e.currentTarget).find('i').toggleClass('fa fa-angle-right fa fa-angle-up');
                    $(e.currentTarget).parents('.searchTag').find('a').toggleClass('hide show');
                    if ($(e.currentTarget).find('i').hasClass('fa-angle-right')) {
                        $(e.currentTarget).find('span').text('Show More');
                    } else {
                        $(e.currentTarget).find('span').text('Show less');
                    }
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
                return events;
            },
            /**
             * intialize a new SchemaTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'guid', 'vent'));
                this.schemaCollection = new VSchemaList([], {});
                this.schemaCollection.url = "/api/atlas/lineage/" + this.guid + "/schema";
                this.commonTableOptions = {
                    collection: this.schemaCollection,
                    includeFilter: false,
                    includePagination: true,
                    includePageSize: false,
                    includeFooterRecords: true,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.bindEvents();
                this.bradCrumbList = [];
            },
            bindEvents: function() {
                this.listenTo(this.schemaCollection, 'backgrid:selected', function(model, checked) {
                    if (checked === true) {
                        model.set("isEnable", true);
                        this.$('.multiSelect').show();
                    } else {
                        model.set("isEnable", false);
                        this.$('.multiSelect').hide();
                    }
                    this.arr = [];
                    var that = this;
                    this.schemaCollection.find(function(item) {
                        if (item.get('isEnable')) {
                            var term = [];
                            that.arr.push({
                                id: item.get("$id$"),
                                model: item
                            });
                        }
                    });
                });
                this.listenTo(this.schemaCollection, "reset", function(value) {
                    this.renderTableLayoutView();
                    $('.schemaTable').show();
                    this.$('.fontLoader').hide();
                }, this);
                this.listenTo(this.schemaCollection, "error", function(value) {
                    $('.schemaTable').hide();
                    this.$('.fontLoader').hide();
                }, this);
            },
            onRender: function() {
                this.schemaCollection.fetch({ reset: true });
                this.renderTableLayoutView();
            },
            fetchCollection: function() {
                this.$('.fontLoader').show();
                this.schemaCollection.fetch({ reset: true });
            },
            renderTableLayoutView: function() {
                var that = this,
                    count = 5;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columnCollection = Backgrid.Columns.extend({
                        sortKey: "position",
                        comparator: function(item) {
                            return item.get(this.sortKey) || 999;
                        },
                        setPositions: function() {
                            _.each(this.models, function(model, index) {
                                if (model.get('name') == "name") {
                                    model.set("position", 2, { silent: true });
                                    model.set("label", "Name");
                                } else if (model.get('name') == "description") {
                                    model.set("position", 3, { silent: true });
                                    model.set("label", "Description");
                                } else if (model.get('name') == "owner") {
                                    model.set("position", 4, { silent: true });
                                    model.set("label", "Owner");
                                }
                            });
                            return this;
                        }
                    });
                    var columns = new columnCollection(that.getSchemaTableColumns());
                    columns.setPositions().sort();
                    that.RTagLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        globalVent: that.globalVent,
                        columns: columns
                    })));
                    that.$('.multiSelect').hide();
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
            getSchemaTableColumns: function() {
                var that = this;
                var col = {};
                if (this.schemaCollection.keyList) {
                    _.each(this.schemaCollection.keyList, function(obj, key) {
                        col[obj.name] = {
                            cell: "Html",
                            editable: false,
                            sortable: false,
                            orderable: true,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    if (model) {
                                        if (!_.isArray(rawValue) && _.isObject(rawValue)) {
                                            if (rawValue.id) {
                                                return '<div><a href="#!/detailPage/' + rawValue.id + '">' + rawValue.$typeName$ + '</a></div>';
                                            } else {
                                                return rawValue.$typeName$;
                                            }
                                        } else if (_.isArray(rawValue)) {
                                            var links = "";
                                            _.each(rawValue, function(val, key) {
                                                if (val.id) {
                                                    links += '<div><a href="#!/detailPage/' + val.id + '">' + val.$typeName$ + '</a></div>';
                                                } else {
                                                    links += '<div>' + val.$typeName$ + '</div>';
                                                }
                                            });
                                            return links;

                                        } else if (model.get('$id$') && model.get('$id$').id && model.get('name') == rawValue) {
                                            return '<div><a href="#!/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a></div>';
                                        } else {
                                            return rawValue;
                                        }
                                    } else {
                                        return rawValue;
                                    }

                                }
                            })
                        };
                    });
                    col['Check'] = {
                        name: "selected",
                        label: "",
                        cell: "select-row",
                        headerCell: "select-all",
                        position: 1
                    };
                    col['tag'] = {
                        label: "Tags",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        className: 'searchTag',
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return CommonViewFunction.tagForTable(model);
                            }
                        })
                    };
                    col['terms'] = {
                        label: "Terms",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        orderable: true,
                        className: 'searchTerm',
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var returnObject = CommonViewFunction.termTableBreadcrumbMaker(model, "schema");
                                if (returnObject.object) {
                                    that.bradCrumbList.push(returnObject.object);
                                }
                                return returnObject.html;
                            }
                        })
                    };
                }
                return this.schemaCollection.constructor.getTableCols(col, this.schemaCollection);
            },
            checkedValue: function(e) {
                if (e) {
                    e.stopPropagation();
                }
                var guid = "",
                    that = this;
                if (this.arr && this.arr.length) {
                    that.addTermModalView(guid, this.arr);
                } else {
                    guid = that.$(e.currentTarget).data("guid");
                    that.addTermModalView(guid);
                }
            },
            addTagModalView: function(e) {
                if (e) {
                    e.stopPropagation();
                }
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        guid: that.$(e.currentTarget).data("guid"),
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
                    // view.saveTagData = function() {
                    //override saveTagData function 
                    // }
                });
            },
            addTermModalView: function(guid, multiple) {

                var that = this;
                require([
                    'views/business_catalog/AddTermToEntityLayoutView',
                ], function(AddTermToEntityLayoutView) {
                    var view = new AddTermToEntityLayoutView({
                        guid: guid,
                        multiple: multiple,
                        callback: function(termName) {
                            that.fetchCollection();
                            that.arr = [];
                        },
                        showLoader: function() {
                            that.$('.fontLoader').show();
                            that.$('.searchTable').hide();
                        }
                    });
                });
            },
            onClickTagCross: function(e) {
                var tagName = $(e.target).data("name"),
                    that = this,
                    modal = CommonViewFunction.deleteTagModel(tagName, "assignTerm");
                modal.on('ok', function() {
                    that.deleteTagData(e);
                });
                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            deleteTagData: function(e) {
                var that = this,
                    tagName = $(e.target).data("name"),
                    guid = $(e.target).data("guid");
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': guid,
                    callback: function() {
                        that.fetchCollection();
                    }
                });
            }
        });
    return SchemaTableLayoutView;
});
