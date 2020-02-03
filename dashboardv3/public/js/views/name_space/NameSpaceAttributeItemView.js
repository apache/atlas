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
    'hbs!tmpl/name_space/NameSpaceAttributeItemView_tmpl',
    'utils/Utils',
    'utils/UrlLinks',
    'utils/Messages'

], function(require, Backbone, NameSpaceAttributeItemViewTmpl, Utils, UrlLinks, Messages) {
    'use strict';

    return Backbone.Marionette.ItemView.extend(
        /** @lends GlobalExclusionListView */
        {

            template: NameSpaceAttributeItemViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                attributeInput: "[data-id='attributeInput']",
                close: "[data-id='close']",
                dataTypeSelector: "[data-id='dataTypeSelector']",
                entityTypeSelector: "[data-id='entityTypeSelector']",
                enumTypeSelectorContainer: "[data-id='enumTypeSelectorContainer']",
                enumTypeSelector: "[data-id='enumTypeSelector']",
                enumValueSelectorContainer: "[data-id='enumValueSelectorContainer']",
                enumValueSelector: "[data-id='enumValueSelector']",
                multiValueSelect: "[data-id='multiValueSelect']",
                multiValueSelectStatus: "[data-id='multiValueSelectStatus']",
                stringLengthContainer: "[data-id='stringLengthContainer']",
                stringLengthValue: "[data-id='stringLength']",
                createNewEnum: "[data-id='createNewEnum']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["keyup " + this.ui.attributeInput] = function(e) {
                    this.model.set({ "name": e.target.value.trim() });
                };
                events["change " + this.ui.dataTypeSelector] = function(e) {
                    //this.ui.multiValueSelect.hide();
                    if (e.target.value.trim() === 'enumeration' || e.target.value.trim() === 'Enumeration') {
                        // this.model.set({ "typeName": "enum" });
                        this.ui.enumTypeSelectorContainer.show();
                        this.emumTypeSelectDisplay();
                        this.ui.stringLengthContainer.hide();
                    } else if (e.target.value.trim() === 'string' || e.target.value.trim() === 'String') {
                        this.model.set({ "typeName": e.target.value.trim() });
                        this.model.set({ "enumValues": null });
                        this.ui.stringLengthContainer.show();
                        //this.ui.multiValueSelect.show();
                        this.ui.enumTypeSelectorContainer.hide();
                        this.ui.enumValueSelectorContainer.hide();
                    } else {
                        // if (e.target.value.trim() === 'int' || e.target.value.trim() === 'float') {
                        //     this.ui.multiValueSelect.show();
                        // }
                        this.model.set({ "typeName": e.target.value.trim() });
                        this.model.set({ "enumValues": null });
                        this.ui.enumTypeSelectorContainer.hide();
                        this.ui.enumValueSelectorContainer.hide();
                        this.ui.stringLengthContainer.hide();
                    }
                };
                events["change " + this.ui.enumTypeSelector] = function(e) {
                    this.model.set({ "enumValues": e.target.value.trim() });
                };
                events["change " + this.ui.stringLengthContainer] = function(e) {
                    this.model.set({ "maxStrLength": e.target.value.trim() });
                };
                events["change " + this.ui.enumTypeSelector] = function(e) {
                    var emumValue = this.ui.enumTypeSelector.select2('data')[0] ? this.ui.enumTypeSelector.select2('data')[0].text : this.ui.enumTypeSelector.val();

                    this.model.set({ "typeName": emumValue });
                    if (emumValue == '' || emumValue == null) {
                        this.ui.enumValueSelectorContainer.hide();
                    } else {
                        this.ui.enumValueSelectorContainer.show();
                        this.showEnumValues(_.escape(emumValue));
                    }
                };
                events["change " + this.ui.enumValueSelector] = function(e) {
                    this.model.set({ "enumValues": this.ui.enumValueSelector.val() });
                };
                // events["change " + this.ui.multiValueSelectStatus] = function(e) {
                //     this.model.set({ "multiValueSelect": e.target.checked });
                // };
                events["click " + this.ui.close] = 'onCloseButton';
                events["click " + this.ui.createNewEnum] = 'onCreateUpdateEnum';
                return events;
            },

            /**
             * intialize a new GlobalExclusionComponentView Layout
             * @constructs
             */
            initialize: function(options) {
                this.parentView = options.parentView;

            },
            onRender: function() {
                var that = this,
                    entitytypes = '',
                    enumTypes = [];
                this.parentView.typeHeaders.fullCollection.each(function(model) {
                    if (model.toJSON().category == "ENTITY") {
                        that.ui.entityTypeSelector.append("<option>" + model.get('name') + "</option>");
                        entitytypes += '<option  value="' + (model.get('name')) + '" data-name="' + (model.get('name')) + '">' + model.get('name') + '</option>';
                    }
                });
                this.ui.entityTypeSelector.select2({
                    placeholder: "Select Entity type",
                    allowClear: true,
                    multiple: true,
                    selectionAdapter: $.fn.select2.amd.require("TagHideDeleteButtonAdapter")
                });
                this.ui.entityTypeSelector.html(entitytypes);

                this.ui.entityTypeSelector.on('select2:open', function(e) { // to make selected option disable in dropdown added remove-from-list class
                    $('.select2-dropdown--below').addClass('remove-from-list');
                });
                this.ui.stringLengthValue.val('50'); //default length for string is 50
                this.ui.enumValueSelector.attr("disabled", "false"); // cannot edit the values
                this.emumTypeSelectDisplay();
                this.ui.enumTypeSelectorContainer.hide();
                this.ui.enumValueSelectorContainer.hide();
                if (this.parentView.isAttrEdit) {
                    this.ui.close.hide();
                    this.ui.createNewEnum.hide(); // cannot add new namespace on edit view
                    this.ui.attributeInput.val(this.parentView.attrDetails.name);
                    this.ui.attributeInput.attr("disabled", "false");
                    this.ui.dataTypeSelector.attr("disabled", "false");
                    this.ui.dataTypeSelector.attr("disabled", "false");
                    //this.ui.multiValueSelect.hide();
                    this.ui.dataTypeSelector.val(this.parentView.attrDetails.attrTypeName);
                    if (this.parentView.attrDetails.attrTypeName == "string") {
                        this.ui.stringLengthContainer.show();
                        this.ui.stringLengthValue.val(this.parentView.attrDetails.maxStrLength);
                    } else {
                        this.ui.stringLengthContainer.hide();
                    }

                    _.each(this.parentView.attrDetails.attrEntityType, function(valName) {
                        that.ui.entityTypeSelector.find('option').each(function(o) {
                            var $el = $(this)
                            if ($el.data("name") === valName) {
                                $el.attr("data-allowremove", "false");
                            }
                        })
                    });
                    this.ui.entityTypeSelector.val(this.parentView.attrDetails.attrEntityType).trigger('change');
                    if (this.parentView.attrDetails && this.parentView.attrDetails.attrTypeName) {
                        var typeName = this.parentView.attrDetails.attrTypeName;
                        if (typeName != "string" && typeName != "boolean" && typeName != "byte" && typeName != "short" && typeName != "int" && typeName != "float" && typeName != "double" && typeName != "long" && typeName != "date") {
                            this.ui.enumTypeSelector.attr("disabled", "false");
                            this.ui.dataTypeSelector.val("enumeration").trigger('change');
                            this.ui.enumTypeSelector.val(typeName).trigger('change');
                        }
                    }
                    // if (this.parentView.attrDetails.multiValued) {
                    //     this.ui.multiValueSelect.show();
                    //     $(this.ui.multiValueSelectStatus).prop('checked', true).trigger('change');
                    //     this.ui.multiValueSelectStatus.attr("disabled", "false");
                    // }
                }
            },
            showEnumValues: function(enumName) {
                var enumValues = '',
                    selectedValues = [],
                    selectedEnum = this.parentView.enumDefCollection.fullCollection.findWhere({ name: enumName }),
                    selectedEnumValues = selectedEnum ? selectedEnum.get('elementDefs') : null,
                    savedValues = [];
                _.each(selectedEnumValues, function(enumVal, index) {
                    selectedValues.push(_.unescape(enumVal.value));
                    enumValues += "<option>" + enumVal.value + "</option>";
                });
                this.ui.enumValueSelector.empty();
                this.ui.enumValueSelector.append(enumValues);
                this.ui.enumValueSelector.val(selectedValues);
                this.ui.enumValueSelector.select2({
                    placeholder: "Select Enum value",
                    allowClear: false,
                    tags: false,
                    multiple: true
                });
                this.model.set({ "enumValues": this.ui.enumValueSelector.val() });
            },
            emumTypeSelectDisplay: function() {
                var enumTypes = '';
                this.parentView.enumDefCollection.fullCollection.each(function(model, index) {
                    enumTypes += "<option>" + _.escape(model.get('name')) + "</option>";
                });
                this.ui.enumTypeSelector.empty();
                this.ui.enumTypeSelector.append(enumTypes);
                this.ui.enumTypeSelector.val('');
                this.ui.enumTypeSelector.select2({
                    placeholder: "Select Enum name",
                    tags: false,
                    allowClear: true,
                    multiple: false
                });
            },
            onCreateUpdateEnum: function(e) {
                var that = this;
                require(["views/name_space/EnumCreateUpdateItemView", "modules/Modal"], function(EnumCreateUpdateItemView, Modal) {
                    var view = new EnumCreateUpdateItemView({
                            onUpdateEnum: function() {
                                that.ui.enumValueSelectorContainer.hide();
                                that.emumTypeSelectDisplay();
                                that.ui.enumValueSelector.empty();
                            },
                            closeModal: function() {
                                modal.trigger("cancel");
                                that.parentView.enumDefCollection.fetch({
                                    success: function() {
                                        that.ui.enumTypeSelector.val(that.model.get('typeName')).trigger('change');
                                    }
                                });
                            },
                            enumDefCollection: that.parentView.enumDefCollection,
                            nameSpaceCollection: that.parentView.options.nameSpaceCollection
                        }),
                        modal = new Modal({
                            title: "Create/ Update Enum",
                            content: view,
                            cancelText: "Cancel",
                            okCloses: false,
                            okText: "Update",
                            allowCancel: true,
                            showFooter: false
                        }).open();
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            },
            onCloseButton: function() {
                var tagName = this.parentView.$el.find('[data-id="tagName"]').val();
                if (this.parentView.collection.models.length > 0) {
                    this.model.destroy();
                }
                if (this.parentView.collection.models.length == 0 && tagName != "") {
                    this.parentView.$el.parent().next().find('button.ok').removeAttr("disabled");
                }
            }
        });
});