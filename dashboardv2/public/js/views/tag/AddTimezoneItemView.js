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
    'hbs!tmpl/tag/AddTimezoneView_tmpl',
    'moment',
    'utils/Globals',
    'moment-timezone',
    'daterangepicker'
], function(require, Backbone, AddTimezoneViewTmpl, moment, Globals) {
    'use strict';

    return Backbone.Marionette.ItemView.extend(
        /** @lends GlobalExclusionListView */
        {

            template: AddTimezoneViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                close: "[data-id='close']",
                startTime: '[data-id="startTime"]',
                endTime: '[data-id="endTime"]',
                timeZone: '[data-id="timeZone"]'
            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["change " + this.ui.startTime] = function(e) {
                    this.model.set({ "startTime": this.ui.startTime.val() });
                    this.buttonActive({ isButtonActive: true });
                };
                events["change " + this.ui.endTime] = function(e) {
                    this.model.set({ "endTime": this.ui.endTime.val() });
                    this.buttonActive({ isButtonActive: true });
                };
                events["change " + this.ui.timeZone] = function(e) {
                    this.model.set({ "timeZone": this.ui.timeZone.val() });
                    this.buttonActive({ isButtonActive: true });
                };
                events["click " + this.ui.close] = 'onCloseButton';
                return events;
            },

            /**
             * intialize a new GlobalExclusionComponentView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'parentView', 'model', 'tagModel'));
            },
            onRender: function() {
                var that = this,
                    tzstr = '<option selected="selected" disabled="disabled">-- Select Timezone --</option>',
                    dateConfig = {
                        "singleDatePicker": true,
                        "showDropdowns": true,
                        "timePicker": true,
                        "timePicker24Hour": true,
                        "timePickerSeconds": true,
                        "startDate": new Date(),
                        "autoApply": true,
                        "autoUpdateInput": false,
                        "locale": {
                            format: 'YYYY/MM/DD hh:mm:ss',
                            cancelLabel: 'Clear'
                        },
                    },
                    startDateObj = _.extend({}, dateConfig),
                    endDateObj = _.extend({}, dateConfig);

                this.ui.timeZone.html(tzstr);
                this.ui.timeZone.select2({
                    data: Globals.userLogedIn.response.timezones
                });

                if (!_.isEmpty(this.model.get('startTime')) || !_.isEmpty(this.model.get('endTime')) || !_.isEmpty(this.model.get('timeZone'))) {
                    if (_.isEmpty(this.model.get('startTime'))) {
                        startDateObj["autoUpdateInput"] = false;
                    } else {
                        startDateObj["autoUpdateInput"] = true;
                        startDateObj["startDate"] = this.model.get('startTime');
                    }
                    if (_.isEmpty(this.model.get('endTime'))) {
                        endDateObj["autoUpdateInput"] = false;
                        endDateObj["minDate"] = this.model.get('startTime');
                    } else {
                        endDateObj["autoUpdateInput"] = true;
                        endDateObj["minDate"] = this.model.get('startTime');
                        endDateObj["startDate"] = this.model.get('endTime');
                    }
                    if (!_.isEmpty(this.model.get('timeZone'))) {
                        this.ui.timeZone.val(this.model.get('timeZone')).trigger("change", { 'manual': true });
                    }
                } else {
                    this.model.set('startTime', that.ui.startTime.val());
                    this.model.set('endTime', that.ui.endTime.val());
                }
                this.ui.startTime.daterangepicker(startDateObj).on('apply.daterangepicker', function(ev, picker) {
                    that.ui.startTime.val(picker.startDate.format('YYYY/MM/DD hh:mm:ss'));
                    _.extend(endDateObj, { "minDate": that.ui.startTime.val() })
                    that.endDateInitialize(endDateObj);
                    that.model.set('startTime', that.ui.startTime.val());
                    that.buttonActive({ isButtonActive: true });
                }).on('cancel.daterangepicker', function(ev, picker) {
                    that.ui.startTime.val('');
                    delete endDateObj.minDate;
                    that.endDateInitialize(endDateObj);
                    that.model.set('startTime', that.ui.startTime.val());
                });
                this.endDateInitialize(endDateObj);
                this.buttonActive({ isButtonActive: true });
            },
            buttonActive: function(option) {
                var that = this;
                if (option && option.isButtonActive && that.tagModel) {
                    var isButton = option.isButtonActive;
                    this.parentView.modal.$el.find('button.ok').attr("disabled", isButton === true ? false : true);
                }
            },
            onCloseButton: function() {
                if (this.tagModel) {
                    this.buttonActive({ isButtonActive: true });
                }
                if (this.parentView.collection.models.length > 0) {
                    this.model.destroy();
                }
                if (this.parentView.collection.models.length <= 0) {
                    this.parentView.ui.timeZoneDiv.hide();
                    this.parentView.ui.checkTimeZone.prop('checked', false);
                }
            },
            endDateInitialize: function(option) {
                var that = this;
                this.ui.endTime.daterangepicker(option).on('apply.daterangepicker', function(ev, picker) {
                    that.ui.endTime.val(picker.startDate.format('YYYY/MM/DD hh:mm:ss'));
                    that.model.set('endTime', that.ui.endTime.val());
                    that.buttonActive({ isButtonActive: true });
                }).on('cancel.daterangepicker', function(ev, picker) {
                    that.ui.endTime.val('');
                    that.model.set('endTime', that.ui.endTime.val());
                });
            }
        });
});