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
    "backbone",
    "hbs!tmpl/entity/EntityNameSpaceView_tmpl",
    "views/entity/EntityNameSpaceItemView",
    "models/VEntity",
    "utils/Utils",
    "utils/Enums",
    "utils/Messages",
    "utils/CommonViewFunction",
    'moment'
], function(require, Backbone, EntityNameSpaceViewTmpl, EntityNameSpaceItemView, VEntity, Utils, Enums, Messages, CommonViewFunction, moment) {
    "use strict";

    return Backbone.Marionette.CompositeView.extend({
        _viewName: "EntityNameSpaceView",
        template: EntityNameSpaceViewTmpl,
        childView: EntityNameSpaceItemView,
        childViewContainer: "[data-id='itemView']",
        childViewOptions: function() {
            return {
                editMode: this.editMode,
                entity: this.entity,
                nameSpaceCollection: this.nameSpaceCollection,
                enumDefCollection: this.enumDefCollection
            };
        },
        /** ui selector cache */
        ui: {
            addItem: "[data-id='addItem']",
            addNameSpace: "[data-id='addNameSpace']",
            saveNameSpace: "[data-id='saveNameSpace']",
            namespaceTree: "[data-id='namespaceTree']",
            cancel: "[data-id='cancel']"
        },
        events: function() {
            var events = {};
            events["click " + this.ui.addItem] = 'createNameElement';
            events["click " + this.ui.addNameSpace] = "onAddNameSpace";
            events["click " + this.ui.saveNameSpace] = "onSaveNameSpace";
            events["click " + this.ui.cancel] = "onCancel";
            return events;
        },
        initialize: function(options) {
            var that = this;
            _.extend(this, _.pick(options, "entity", "nameSpaceCollection", "enumDefCollection", "guid", "fetchCollection"));
            this.editMode = false;
            this.$("editBox").hide();
            this.actualCollection = new Backbone.Collection(
                _.map(this.entity.namespaceAttributes, function(val, key) {
                    var foundNameSpace = that.nameSpaceCollection[key];
                    if (foundNameSpace) {
                        _.each(val, function(aVal, aKey) {
                            var foundAttr = _.find(foundNameSpace, function(o) {
                                return o.name === aKey
                            });
                            if (foundAttr) {
                                val[aKey] = { value: aVal, typeName: foundAttr.typeName };
                            }
                        })
                    }
                    return _.extend({}, val, { __internal_UI_nameSpaceName: key });
                }));
            this.collection = new Backbone.Collection();
            this.entityModel = new VEntity();
        },
        updateToActualData: function(options) {
            var silent = options && options.silent || false;
            this.collection.reset(this.actualCollection.toJSON(), { silent: silent });
        },
        onAddNameSpace: function() {
            this.ui.addNameSpace.hide();
            this.ui.saveNameSpace.show();
            this.ui.cancel.show();
            this.editMode = true;
            this.ui.namespaceTree.hide();
            this.$(".editBox").show();
            this.updateToActualData({ silent: true });
            if (this.collection.length === 0) {
                this.createNameElement();
            } else {
                this.collection.trigger("reset");
            }
            this.panelOpenClose();
        },
        onCancel: function() {
            this.ui.cancel.hide();
            this.ui.saveNameSpace.hide();
            this.ui.addNameSpace.show();
            this.editMode = false;
            this.ui.namespaceTree.show();
            this.$(".editBox").hide();
            this.updateToActualData();
            this.panelOpenClose();
        },
        panelOpenClose: function() {
            var collection = this.editMode ? this.collection : this.actualCollection;
            if (collection && collection.length === 0) {
                this.$el.find(".panel-heading").addClass("collapsed");
                this.$el.find(".panel-collapse.collapse").removeClass("in");
                this.ui.addNameSpace.text("Add");
                this.ui.addNameSpace.attr("data-original-title", "Add");
            } else {
                this.ui.addNameSpace.text("Edit");
                this.ui.addNameSpace.attr("data-original-title", "Edit All");
                this.$el.find(".panel-heading").removeClass("collapsed");
                this.$el.find(".panel-collapse.collapse").addClass("in");
            }
        },
        validate: function() {
            var validation = true;
            this.$el.find('.custom-col-1[data-id="value"] [data-key]').each(function(el) {
                var val = $(this).val(),
                    elIsSelect2 = $(this).hasClass("select2-hidden-accessible");
                if (_.isString(val)) {
                    val = val.trim();
                }
                if (_.isEmpty(val)) {
                    if (validation) {
                        validation = false;
                    }
                    if (elIsSelect2) {
                        $(this).siblings(".select2").find(".select2-selection").attr("style", "border-color : red !important");
                    } else {
                        $(this).css("borderColor", "red");
                    }
                } else {
                    if (elIsSelect2) {
                        $(this).siblings(".select2").find(".select2-selection").attr("style", "");
                    } else {
                        $(this).css("borderColor", "");
                    }
                }
            });
            return validation;
        },
        onSaveNameSpace: function() {
            var that = this;
            if (!this.validate()) {
                return;
            }
            this.entityModel.saveNamespaceEntity(this.guid, {
                data: JSON.stringify(this.generateData()),
                type: "POST",
                success: function(data) {
                    Utils.notifySuccess({
                        content: "One or more namespace attributes" + Messages.getAbbreviationMsg(false, 'editSuccessMessage')
                    });
                    that.entity.namespaceAttributes = data;
                    this.editMode = false;
                    that.fetchCollection();
                    that.onCancel();
                },
                complete: function(model, response) {
                    //that.hideLoader();
                }
            });
        },
        generateData: function() {
            var finalObj = {};
            this.collection.forEach(function(model) {
                if (!model.has("addAttrButton")) {
                    var nameSpaceName = model.get("__internal_UI_nameSpaceName"),
                        modelObj = model.toJSON();
                    _.each(modelObj, function(o, k) {
                        if (k === "isNew" && k === "__internal_UI_nameSpaceName") {
                            delete modelObj[k];
                            return;
                        }
                        if (_.isObject(o) && o.value !== undefined) {
                            modelObj[k] = o.value;
                        }
                    })
                    if (nameSpaceName !== undefined) {
                        if (finalObj[nameSpaceName]) {
                            finalObj[nameSpaceName] = _.extend(finalObj[nameSpaceName], modelObj);
                        } else {
                            finalObj[nameSpaceName] = modelObj;
                        }
                    }
                }
            });
            if (_.isEmpty(finalObj)) {
                this.actualCollection.forEach(function(model) {
                    var nameSpaceName = model.get("__internal_UI_nameSpaceName");
                    if (nameSpaceName) {
                        finalObj[nameSpaceName] = {};
                    }
                })
            }
            return finalObj;
        },
        createNameElement: function(options) {
            var modelObj = { isNew: true };
            this.collection.unshift(modelObj);
        },
        renderNamespace: function() {
            var li = ""
            this.actualCollection.forEach(function(obj) {
                var attrLi = "";
                _.each(obj.attributes, function(val, key) {
                    if (key !== "__internal_UI_nameSpaceName") {
                        var newVal = val;
                        if (_.isObject(val) && !_.isUndefinedNull(val.value)) {
                            newVal = val.value;
                            if (newVal.length > 0 && val.typeName.indexOf("date") > -1) {
                                newVal = _.map(newVal, function(dates) {
                                    return moment(dates).format("MM/DD/YYYY");
                                });
                            }
                            if (val.typeName === "date") {
                                newVal = moment(newVal).format("MM/DD/YYYY");
                            }

                        }
                        attrLi += "<tr><td>" + _.escape(key) + " (" + _.escape(val.typeName) + ")</td><td>" + _.escape(newVal) + "</td></tr>";
                    }
                });
                li += "<ul class='namespace-tree-parent'><li class='table'>" + _.escape(obj.get("__internal_UI_nameSpaceName")) + "</li>" +
                    "<li class='namespace-tree-child entity-detail-table'>" +
                    "<table class='table'>" + attrLi + "</table>" +
                    "</li></ul>";
            });
            this.ui.namespaceTree.html(li);
        },
        onRender: function() {
            this.panelOpenClose();
            this.renderNamespace();
        }
    });
});