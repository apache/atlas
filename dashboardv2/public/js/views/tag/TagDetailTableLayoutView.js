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
    'hbs!tmpl/tag/TagDetailTableLayoutView_tmpl',
    'utils/CommonViewFunction',
    'utils/Utils',
    'collection/VTagList',
    'utils/Messages'
], function(require, Backbone, TagDetailTableLayoutView_tmpl, CommonViewFunction, Utils, VTagList, Messages) {
    'use strict';

    var TagDetailTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagDetailTableLayoutView */
        {
            _viewName: 'TagDetailTableLayoutView',

            template: TagDetailTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {
                RTagTableLayoutView: "#r_tagTableLayoutView"
            },

            /** ui selector cache */
            ui: {
                detailValue: "[data-id='detailValue']",
                addTag: "[data-id='addTag']",
                deleteTag: "[data-id='delete']",
                editTag: "[data-id='edit']",
                checkPropagtedTag: "[data-id='checkPropagtedTag']",
                propagatedFromClick: "[data-id='propagatedFromClick']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.addTag] = function(e) {
                    this.addModalView(e);
                };
                events["click " + this.ui.deleteTag] = function(e) {
                    this.onClickTagCross(e);
                };
                events["click " + this.ui.editTag] = function(e) {
                    this.editTagDataModal(e);
                };
                events["click " + this.ui.propagatedFromClick] = function(e) {
                    Utils.setUrl({
                        url: '#!/detailPage/' + $(e.currentTarget).data("guid"),
                        mergeBrowserUrl: false,
                        trigger: true
                    });
                };
                events["click " + this.ui.checkPropagtedTag] = 'onCheckPropagtedTag';
                return events;
            },
            /**
             * intialize a new TagDetailTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'entity', 'guid', 'entityName', 'fetchCollection', 'enumDefCollection', 'classificationDefCollection'));
                this.collectionObject = this.entity;
                this.tagCollection = new VTagList();
                var that = this,
                    tags = _.toArray(this.collectionObject.classifications);
                this.tagCollection.fullCollection.reset(tags);
                this.commonTableOptions = {
                    collection: this.tagCollection,
                    includeFilter: false,
                    includePagination: true,
                    includeFooterRecords: true,
                    includePageSize: true,
                    includeGotoPage: true,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
            },
            bindEvents: function() {},
            onRender: function() {
                this.renderTableLayoutView();
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getSchemaTableColumns());
                    if (that.RTagTableLayoutView) {
                        that.RTagTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                            columns: cols
                        })));
                    }
                });
            },
            getSchemaTableColumns: function(options) {
                var that = this,
                    col = {};

                return this.tagCollection.constructor.getTableCols({
                        tag: {
                            label: "Classification",
                            cell: "html",
                            editable: false,
                            sortable: false,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    if (that.guid !== model.get('entityGuid')) {
                                        var propagtedFrom = ' <span class="btn btn-action btn-sm btn-icon btn-blue" title="Propagated From" data-guid=' + model.get('entityGuid') + ' data-id="propagatedFromClick"><span> Propagated From </span></span>';
                                        return '<a title="" href="#!/tag/tagAttribute/' + model.get('typeName') + '">' + model.get('typeName') + '</a>' + propagtedFrom;
                                    } else {
                                        return '<a title="' + model.get('typeName') + '" href="#!/tag/tagAttribute/' + model.get('typeName') + '">' + model.get('typeName') + '</a>';
                                    }
                                }
                            })
                        },
                        attributes: {
                            label: "Attributes",
                            cell: "html",
                            editable: false,
                            sortable: false,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    var values = model.get('attributes');
                                    var data = that.classificationDefCollection.fullCollection.findWhere({ 'name': model.get('typeName') });
                                    var attributeDefs = Utils.getNestedSuperTypeObj({ data: data.toJSON(), collection: that.classificationDefCollection, attrMerge: true });
                                    var tagValue = 'NA',
                                        dataType;
                                    if (!_.isEmpty(attributeDefs)) {
                                        var stringValue = "";
                                        _.each(_.sortBy(_.map(attributeDefs, function(obj) {
                                            obj['sortKey'] = obj.name && _.isString(obj.name) ? obj.name.toLowerCase() : "-";
                                            return obj;
                                        }), 'sortKey'), function(sortedObj) {
                                            var val = _.isNull(values[sortedObj.name]) ? "-" : values[sortedObj.name],
                                                key = sortedObj.name;
                                            if (_.isObject(val)) {
                                                val = JSON.stringify(val);
                                            }
                                            if (sortedObj.typeName === "date") {
                                                val = new Date(val)
                                            }
                                            stringValue += "<tr><td class='html-cell string-cell renderable'>" + _.escape(key) + "</td><td class='html-cell string-cell renderable' data-type='" + sortedObj.typeName + "'>" + _.escape(val) + "</td>";
                                        });
                                        tagValue = "<div class='mainAttrTable'><table class='attriTable'><tr><th class='html-cell string-cell renderable'>Name</th><th class='html-cell string-cell renderable'>Value</th>" + stringValue + "</table></div>";
                                    }
                                    return tagValue;
                                }
                            })
                        },
                        tool: {
                            label: "Action",
                            cell: "html",
                            editable: false,
                            sortable: false,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    var deleteData = '<button title="Delete" class="btn btn-action btn-sm" data-id="delete" data-entityguid="' + model.get('entityGuid') + '" data-name="' + model.get('typeName') + '"><i class="fa fa-trash"></i></button>',
                                        editData = '<button title="Edit" class="btn btn-action btn-sm" data-id="edit" data-name="' + model.get('typeName') + '"><i class="fa fa-pencil"></i></button>',
                                        btnObj = null;
                                    if (that.guid === model.get('entityGuid')) {
                                        return '<div class="btn-inline">' + deleteData + editData + '</div>'
                                    } else if (that.guid !== model.get('entityGuid') && model.get('entityStatus') === "DELETED") {
                                        return '<div class="btn-inline">' + deleteData + '</div>';
                                    }
                                }
                            })
                        },
                    },
                    this.tagCollection);
            },
            addModalView: function(e) {
                var that = this;
                require(['views/tag/AddTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        guid: that.guid,
                        modalCollection: that.collection,
                        collection: that.classificationDefCollection,
                        enumDefCollection: that.enumDefCollection
                    });
                });
            },
            onClickTagCross: function(e) {
                var that = this,
                    tagName = $(e.currentTarget).data("name"),
                    entityGuid = $(e.currentTarget).data("entityguid");
                CommonViewFunction.deleteTag({
                    tagName: tagName,
                    guid: that.guid,
                    associatedGuid: that.guid != entityGuid ? entityGuid : null,
                    msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + this.entityName + "?</b></div>",
                    titleMessage: Messages.removeTag,
                    okText: "Remove",
                    showLoader: function() {
                        that.$('.fontLoader').show();
                        that.$('.tableOverlay').show();
                    },
                    hideLoader: function() {
                        that.$('.fontLoader').hide();
                        that.$('.tableOverlay').hide();
                    },
                    callback: function() {
                        this.hideLoader();
                        if (that.fetchCollection) {
                            that.fetchCollection();
                        }
                    }
                });
            },
            editTagDataModal: function(e) {
                var that = this,
                    tagName = $(e.currentTarget).data('name'),
                    tagModel = _.find(that.collectionObject.classifications, function(tag) {
                        return (tagName === tag.typeName && that.guid === tag.entityGuid);
                    });
                require([
                    'views/tag/AddTagModalView'
                ], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        'tagModel': tagModel,
                        'callback': function() {
                            that.fetchCollection();
                        },
                        'guid': that.guid,
                        'collection': that.classificationDefCollection,
                        'enumDefCollection': that.enumDefCollection
                    });
                });
            },
            onCheckPropagtedTag: function(e) {
                var that = this,
                    tags = _.toArray(that.collectionObject.classifications),
                    unPropagatedTags = [];
                e.stopPropagation();
                if (e.target.checked) {
                    that.tagCollection.fullCollection.reset(tags);
                } else {
                    unPropagatedTags = _.filter(tags, function(val) {
                        return that.guid === val.entityGuid;
                    });
                    that.tagCollection.fullCollection.reset(unPropagatedTags);
                }
            }
        });
    return TagDetailTableLayoutView;
});