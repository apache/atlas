/*
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
    'hbs!tmpl/entity/EntityNameSpaceItemView_tmpl',
    'moment',
    'daterangepicker'
], function(require, Backbone, EntityNameSpaceItemViewTmpl, moment) {
    'use strict';

    return Backbone.Marionette.ItemView.extend({
        _viewName: 'EntityNameSpaceItemView',

        template: EntityNameSpaceItemViewTmpl,

        templateHelpers: function() {
            return {
                editMode: this.editMode,
                entity: this.entity,
                getValue: this.getValue.bind(this),
                getNamespaceDroupdown: this.getNamespaceDroupdown.bind(this),
                nameSpaceCollection: this.nameSpaceCollection.fullCollection.toJSON(),
                model: this.model.toJSON()
            }
        },
        tagName: 'tr',
        className: "custom-tr",

        /** Layout sub regions */
        regions: {},

        /** ui selector cache */
        ui: {
            keyEl: "[data-id='key']",
            valueEl: "[data-type='value']",
            addItem: "[data-id='addItem']",
            deleteItem: "[data-id='deleteItem']",
            editMode: "[data-id='editMode']"
        },
        /** ui events hash */
        events: function() {
            var events = {};
            events["click " + this.ui.deleteItem] = 'onDeleteItem';
            events["change " + this.ui.keyEl] = 'onAttrChange';
            return events;
        },

        /**
         * intialize a new EntityNameSpaceItemView Layout
         * @constructs
         */
        initialize: function(options) {
            _.extend(this, options);
        },
        onRender: function() {
            var that = this;
            this.ui.keyEl.val("");
            this.ui.keyEl.select2({ placeholder: "Select Attribute" });

            if (this.editMode && (!this.model.has("isNew"))) {
                this.getEditNamespaceEl();
            }
            this.initializeElement();
            this.bindEvent();
        },
        initializeElement: function() {},
        bindEvent: function() {
            var that = this;
            if (this.editMode) {
                this.listenTo(this.model.collection, 'destroy unset:attr', function() {
                    if (this.model.has("isNew")) {
                        this.render();
                    }
                });
                this.listenTo(this.model.collection, 'selected:attr', function(value, model) {
                    if (model.cid !== this.model.cid && this.model.has("isNew")) {
                        var $select2el = that.$el.find('.custom-col-1:first-child>select[data-id="key"]');
                        $select2el.find('option[value="' + value + '"]').remove();
                        var select2val = $select2el.select2("val");
                        $select2el.select2({ placeholder: "Select Attribute" });
                        if (this.model.keys().length <= 2) {
                            $select2el.val("").trigger("change", true);
                        }
                    }
                });
                this.$el.off("change", ".custom-col-1[data-id='value']>[data-key]").on("change", ".custom-col-1[data-id='value']>[data-key]", function(e) {
                    var key = $(this).data("key"),
                        namespace = $(this).data("namespace"),
                        typeName = $(this).data("typename"),
                        multi = $(this).data("multi"),
                        updateObj = that.model.toJSON();
                    if (_.isUndefinedNull(updateObj[key])) {
                        updateObj[key] = { value: null, typeName: typeName };
                    }
                    updateObj[key].value = multi ? $(this).select2("val") : e.currentTarget.value;
                    if (!that.model.has("__internal_UI_nameSpaceName")) {
                        updateObj["__internal_UI_nameSpaceName"] = namespace;
                    }
                    if (typeName === "date") {
                        updateObj[key].value = new Date(updateObj[key].value).getTime()
                    }
                    that.model.set(updateObj);
                });
                this.$el.on('keypress', '.select2_only_number .select2-search__field', function() {
                    $(this).val($(this).val().replace(/[^\d].+/, ""));
                    if ((event.which < 48 || event.which > 57)) {
                        event.preventDefault();
                    }
                });
            }
        },
        getAttrElement: function(options) {
            var that = this,
                returnEL = "N/A";
            if (options) {
                var key = options.key,
                    typeName = options.val.typeName || "",
                    val = options.val.value,
                    isMultiValued = typeName && typeName.indexOf("array<") === 0,
                    namespace = options.namespace,
                    allowOnlyNum = false;
                var elType = isMultiValued ? "select" : "input";
                if (!_.isEmpty(val)) {
                    val = _.escape(val);
                }
                if (typeName === "boolean") {
                    val = String(val);
                }
                if (typeName === "date" && _.isNumber(val)) {
                    val = moment(val).format("MM/DD/YYYY");
                }
                if (typeName.indexOf("string") > -1) {
                    returnEL = '<' + elType + ' type="text" data-key="' + key + '" data-namespace="' + namespace + '" data-typename="' + typeName + '" data-multi="' + isMultiValued + '"  multiple="' + isMultiValued + '" placeholder="Enter String" class="form-control" ' + (!_.isUndefinedNull(val) ? 'value="' + val + '"' : "") + '></' + elType + '>';
                } else if (typeName === "boolean") {
                    returnEL = '<select data-key="' + key + '" data-namespace="' + namespace + '" data-typename="' + typeName + '" class="form-control"><option value="">--Select Value--</option><option value="true" ' + (!_.isUndefinedNull(val) && val == "true" ? "selected" : "") + '>true</option><option value="false" ' + (!_.isUndefinedNull(val) && val == "false" ? "selected" : "") + '>false</option></select>';
                } else if (typeName === "date") {
                    returnEL = '<input type="text" data-key="' + key + '" data-namespace="' + namespace + '" data-typename="' + typeName + '" data-type="date" class="form-control" ' + (!_.isUndefinedNull(val) ? 'value="' + val + '"' : "") + '>'
                    setTimeout(function() {
                        var dateObj = { "singleDatePicker": true, "showDropdowns": true };
                        that.$el.find('input[data-type="date"]').daterangepicker(dateObj);
                    }, 0);
                } else if (typeName === "byte" || typeName === "short" || typeName.indexOf("int") > -1 || typeName.indexOf("float") > -1 || typeName === "double" || typeName === "long") {
                    allowOnlyNum = true;
                    returnEL = '<' + elType + ' data-key="' + key + '" data-namespace="' + namespace + '" data-typename="' + typeName + '" type="number" data-multi="' + isMultiValued + '"  multiple="' + isMultiValued + '" placeholder="Enter Number" class="form-control" ' + (!_.isUndefinedNull(val) ? 'value="' + val + '"' : "") + '></' + elType + '>';
                } else if (typeName) {
                    var foundEnumType = this.enumDefCollection.fullCollection.find({ name: typeName });
                    if (foundEnumType) {
                        var enumOptions = "";
                        _.forEach(foundEnumType.get("elementDefs"), function(obj) {
                            enumOptions += '<option value="' + obj.value + '">' + obj.value + '</option>'
                        });
                        returnEL = '<select data-key="' + key + '" data-namespace="' + namespace + '" data-typename="' + typeName + '">' + enumOptions + '</select>';
                    }
                    setTimeout(function() {
                        var selectEl = that.$el.find('.custom-col-1[data-id="value"] select[data-key="' + key + '"]');
                        selectEl.val((val || ""));
                        selectEl.select2();
                    }, 0);
                }
                if (isMultiValued) {
                    setTimeout(function() {
                        var selectEl = that.$el.find('.custom-col-1[data-id="value"] select[data-key="' + key + '"][data-multi="true"]');
                        var data = val && val.split(",") || [];
                        if (allowOnlyNum) {
                            selectEl.parent().addClass("select2_only_number");
                        }
                        selectEl.select2({ tags: true, multiple: true, data: data });
                        selectEl.val(data).trigger("change");
                    }, 0);
                }
            }
            return returnEL;
        },
        onAttrChange: function(e, manual) {
            var key = e.currentTarget.value.split(":");
            if (key.length && key.length === 3) {
                var valEl = $(e.currentTarget).parent().siblings(".custom-col-1"),
                    hasModalData = this.model.get(key[1]);
                if (!hasModalData) {
                    var tempObj = {
                        "__internal_UI_nameSpaceName": key[0]
                    };
                    if (this.model.has("isNew")) {
                        tempObj["isNew"] = true;
                    }
                    tempObj[key[1]] = null;
                    this.model.clear({ silent: true }).set(tempObj)
                }
                valEl.html(this.getAttrElement({ namespace: key[0], key: key[1], val: hasModalData ? hasModalData : { typeName: key[2] } }));
                if (manual === undefined) {
                    this.model.collection.trigger("selected:attr", e.currentTarget.value, this.model);
                }
            }
        },
        getValue: function(value, key, namespaceName) {
            var typeName = value.typeName,
                value = value.value;
            if (typeName === "date") {
                return moment(value).format("MM/DD/YYYY");
            } else {
                return value;
            }
        },
        getNamespaceDroupdown: function(nameSpaceCollection) {
            var optgroup = "";
            var that = this;
            var model = that.model.omit(["isNew", "__internal_UI_nameSpaceName"]),
                keys = _.keys(model),
                isSelected = false,
                selectdVal = null;
            if (keys.length === 1) {
                isSelected = true;
            }
            _.each(nameSpaceCollection, function(obj) {
                var options = "";
                if (obj.attributeDefs.length) {
                    _.each(obj.attributeDefs, function(attrObj) {
                        var entityNamespace = that.model.collection.filter({ __internal_UI_nameSpaceName: obj.name }),
                            hasAttr = false;
                        if (entityNamespace) {
                            var found = entityNamespace.find(function(obj) {
                                return obj.attributes.hasOwnProperty(attrObj.name);
                            });
                            if (found) {
                                hasAttr = true;
                            }
                        }
                        if ((isSelected && keys[0] === attrObj.name) || !(hasAttr) && attrObj.options.applicableEntityTypes.indexOf('"' + that.entity.typeName + '"') > -1) {
                            var value = obj.name + ":" + attrObj.name + ":" + attrObj.typeName;
                            if (isSelected && keys[0] === attrObj.name) { selectdVal = value };
                            options += '<option value="' + value + '">' + attrObj.name + ' (' + _.escape(attrObj.typeName) + ')</option>';
                        }
                    });
                    if (options.length) {
                        optgroup += '<optgroup label="' + obj.name + '">' + options + '</optgroup>';
                    }
                }
            });

            setTimeout(function() {
                if (selectdVal) {
                    that.$el.find('.custom-col-1:first-child>select[data-id="key"]').val(selectdVal).trigger("change", true);
                } else {
                    that.$el.find('.custom-col-1:first-child>select[data-id="key"]').val("").trigger("change", true);
                }
            }, 0);
            return '<select data-id="key">' + optgroup + '</select>';
        },
        getEditNamespaceEl: function() {
            var that = this,
                trs = "";
            _.each(this.model.attributes, function(val, key) {
                if (key !== "__internal_UI_nameSpaceName" && key !== "isNew") {
                    var td = '<td class="custom-col-1" data-key=' + key + '>' + key + '</td><td class="custom-col-0">:</td><td class="custom-col-1" data-id="value">' + that.getAttrElement({ namespace: that.model.get("__internal_UI_nameSpaceName"), key: key, val: val }) + '</td>';

                    td += '<td class="custom-col-2 btn-group">' +
                        '<button class="btn btn-default btn-sm" data-key="' + key + '" data-id="deleteItem">' +
                        '<i class="fa fa-times"> </i>' +
                        '</button></td>';
                    trs += "<tr>" + td + "</tr>";
                }
            })
            this.$("[data-id='namespaceTreeChild']").html("<table class='custom-table'>" + trs + "</table>");
        },
        onDeleteItem: function(e) {
            var key = $(e.currentTarget).data("key");
            if (this.model.has(key)) {
                if (this.model.keys().length === 2) {
                    this.model.destroy();
                } else {
                    this.model.unset(key);
                    if (!this.model.has("isNew")) {
                        this.$el.find("tr>td:first-child[data-key='" + key + "']").parent().remove()
                    }
                    this.model.collection.trigger("unset:attr");
                }
            } else {
                this.model.destroy();
            }
        }
    });
});