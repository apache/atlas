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
    'utils/CommonViewFunction',
    'utils/Messages',
    'utils/Globals',
    'utils/Enums',
    'utils/UrlLinks'
], function(require, Backbone, SchemaTableLayoutViewTmpl, VSchemaList, Utils, CommonViewFunction, Messages, Globals, Enums, UrlLinks) {
    'use strict';

    var SchemaTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends SchemaTableLayoutView */
        {
            _viewName: 'SchemaTableLayoutView',

            template: SchemaTableLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RSchemaTableLayoutView: "#r_schemaTableLayoutView",
            },
            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                addTag: "[data-id='addTag']",
                addTerm: '[data-id="addTerm"]',
                showMoreLess: '[data-id="showMoreLess"]',
                showMoreLessTerm: '[data-id="showMoreLessTerm"]',
                addAssignTag: "[data-id='addAssignTag']",
                checkDeletedEntity: "[data-id='checkDeletedEntity']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.addTag] = 'checkedValue';
                events["click " + this.ui.addTerm] = 'checkedValue';
                events["click " + this.ui.addAssignTag] = 'checkedValue';
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
                    this.$('.popover.popoverTag').hide();
                    $(e.currentTarget).parent().find("div.popover").show();
                    var positionContent = $(e.currentTarget).position();
                    positionContent.top = positionContent.top + 26;
                    positionContent.left = positionContent.left - 41;
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
                events["click " + this.ui.checkDeletedEntity] = 'onCheckDeletedEntity';

                return events;
            },
            /**
             * intialize a new SchemaTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'entityDefCollection', 'attribute', 'referredEntities', 'fetchCollection', 'enumDefCollection'));
                this.schemaCollection = new VSchemaList([], {});
                this.commonTableOptions = {
                    collection: this.schemaCollection,
                    includeFilter: false,
                    includePagination: true,
                    includePageSize: false,
                    includeFooterRecords: true,
                    includeOrderAbleColumns: false,
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
                    } else {
                        model.set("isEnable", false);
                    }
                    this.arr = [];
                    var that = this;
                    this.schemaCollection.find(function(item) {
                        var obj = item.toJSON();
                        if (item.get('isEnable')) {
                            that.arr.push({
                                id: obj.guid,
                                model: obj
                            });
                        }
                    });
                    if (this.arr.length > 0) {
                        if (Globals.taxonomy) {
                            this.$('.multiSelectTerm').show();
                        }
                        this.$('.multiSelectTag').show();
                    } else {
                        if (Globals.taxonomy) {
                            this.$('.multiSelectTerm').hide();
                        }
                        this.$('.multiSelectTag').hide();
                    }
                });
            },
            onRender: function() {
                this.generateTableData();
            },
            generateTableData: function(checkedDelete) {
                var that = this,
                    newModel;
                this.activeObj = [];
                this.deleteObj = [];
                this.schemaTableAttribute = null;
                if (this.attribute && this.attribute[0]) {
                    var firstColumn = this.attribute[0],
                        defObj = that.entityDefCollection.fullCollection.find({ name: firstColumn.typeName });
                    if (defObj && defObj.get('options') && defObj.get('options').schemaAttributes) {
                        if (firstColumn) {
                            try {
                                var mapObj = JSON.parse(defObj.get('options').schemaAttributes);
                                that.schemaTableAttribute = _.pick(firstColumn.attributes, mapObj);
                            } catch (e) {}
                        }
                    }
                }
                _.each(this.attribute, function(obj) {
                    newModel = that.referredEntities[obj.guid];
                    if (newModel.attributes['position']) {
                        newModel['position'] = newModel.attributes['position'];
                    }
                    if (!Enums.entityStateReadOnly[newModel.status]) {
                        that.activeObj.push(newModel);
                        that.schemaCollection.push(newModel);
                    } else if (Enums.entityStateReadOnly[newModel.status]) {
                        that.deleteObj.push(newModel);
                    }
                });
                $('body').click(function(e) {
                    var iconEvnt = e.target.nodeName;
                    if (that.$('.popoverContainer').length) {
                        if ($(e.target).hasClass('tagDetailPopover') || iconEvnt == "I") {
                            return;
                        }
                        that.$('.popover.popoverTag').hide();
                    }
                });
                if (this.schemaCollection.length === 0 && this.deleteObj.length) {
                    this.ui.checkDeletedEntity.find("input").prop('checked', true);
                    this.schemaCollection.reset(this.deleteObj, { silent: true });
                }
                if (this.activeObj.length === 0 && this.deleteObj.length === 0) {
                    this.ui.checkDeletedEntity.hide();
                }
                this.renderTableLayoutView();
            },
            showLoader: function() {
                this.$('.fontLoader').show();
                this.$('.tableOverlay').show()
            },
            hideLoader: function(argument) {
                this.$('.fontLoader').hide();
                this.$('.tableOverlay').hide();
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columnCollection = Backgrid.Columns.extend({
                        // sortKey: "position",
                        // comparator: function(item) {
                        //     return item.get(this.sortKey) || 999;
                        // },
                        // setPositions: function() {
                        //     _.each(this.models, function(model, index) {
                        //         if (model.get('name') == "name") {
                        //             model.set("position", 2, { silent: true });
                        //             model.set("label", "Name");
                        //         } else if (model.get('name') == "description") {
                        //             model.set("position", 3, { silent: true });
                        //             model.set("label", "Description");
                        //         } else if (model.get('name') == "owner") {
                        //             model.set("position", 4, { silent: true });
                        //             model.set("label", "Owner");
                        //         }
                        //     });
                        //     return this;
                        // }
                    });
                    var columns = new columnCollection(that.getSchemaTableColumns());
                    //columns.setPositions().sort();
                    that.RSchemaTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: columns
                    })));
                    that.$('.multiSelectTerm').hide();
                    that.$('.multiSelectTag').hide();
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
            getSchemaTableColumns: function(deleteEnity) {
                var that = this,
                    col = {
                        Check: {
                            name: "selected",
                            label: "",
                            cell: "select-row",
                            headerCell: "select-all"
                        }
                    }
                if (this.schemaTableAttribute) {
                    _.each(_.keys(this.schemaTableAttribute), function(key) {
                        if (key !== "position") {
                            col[key] = {
                                label: key,
                                cell: "html",
                                editable: false,
                                sortable: false,
                                className: "searchTableName",
                                formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                    fromRaw: function(rawValue, model) {
                                        var value = model.get('attributes')[key];
                                        if (key === "name" && model.get('guid')) {
                                            var nameHtml = '<a href="#!/detailPage/' + model.get('guid') + '">' + value + '</a>';
                                            if (model.get('status') && Enums.entityStateReadOnly[model.get('status')]) {
                                                nameHtml += '<button type="button" title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                                                return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                                            } else {
                                                return nameHtml;
                                            }
                                        } else {
                                            return value
                                        }
                                    }
                                })
                            };
                        }
                    });
                    col['tag'] = {
                        label: "Tags",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        className: 'searchTag',
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var obj = model.toJSON();
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
                                    var obj = model.toJSON();
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
                    return this.schemaCollection.constructor.getTableCols(col, this.schemaCollection);
                }

            },
            checkedValue: function(e) {
                if (e) {
                    e.stopPropagation();
                }
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
            addTagModalView: function(guid, multiple) {
                var that = this;
                var tagList = that.schemaCollection.find({ 'guid': guid });
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        guid: guid,
                        multiple: multiple,
                        tagList: _.map((tagList ? tagList.get('classifications') : []), function(obj) {
                            return obj.typeName;
                        }),
                        callback: function() {
                            that.fetchCollection();
                            that.arr = [];
                        },
                        hideLoader: that.hideLoader.bind(that),
                        showLoader: that.showLoader.bind(that),
                        enumDefCollection: that.enumDefCollection
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
                        hideLoader: that.hideLoader.bind(that),
                        showLoader: that.showLoader.bind(that)
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
                    showLoader: that.showLoader.bind(that),
                    hideLoader: that.hideLoader.bind(that),
                    callback: function() {
                        that.fetchCollection();
                    }
                });
            },
            onCheckDeletedEntity: function(e) {
                if (e.target.checked) {
                    if (this.deleteObj.length) {
                        this.schemaCollection.reset(this.activeObj.concat(this.deleteObj));
                    }
                } else {
                    this.schemaCollection.reset(this.activeObj);
                }
            }
        });
    return SchemaTableLayoutView;
});
