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
    'moment-timezone',
    'daterangepicker'
], function(require, Backbone, AddTimezoneViewTmpl, moment) {
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
                startTime: 'input[name="startTime"]',
                endTime: 'input[name="endTime"]',
                timeZone: '[data-id="timeZone"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
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
                    dateObj = {
                        "singleDatePicker": true,
                        "showDropdowns": true,
                        "timePicker": true,
                        "startDate": new Date(),
                        "timePickerIncrement": 30,
                        "locale": {
                            format: 'YYYY/MM/DD HH:MM:SS'
                        }
                    },
                    tzstr = '<option selected="selected" disabled="disabled">-- Select Timezone --</option>';

                if (this.model.get('startTime') !== "") {
                    this.ui.startTime.daterangepicker({
                        "singleDatePicker": true,
                        "showDropdowns": true,
                        "timePicker": true,
                        "startDate": this.model.get('startTime'),
                        "timePickerIncrement": 30,
                        "locale": {
                            format: 'YYYY/MM/DD HH:MM:SS'
                        }
                    });
                    this.ui.endTime.daterangepicker({
                        "singleDatePicker": true,
                        "showDropdowns": true,
                        "timePicker": true,
                        "startDate": this.model.get('endTime'),
                        "timePickerIncrement": 30,
                        "locale": {
                            format: 'YYYY/MM/DD HH:MM:SS'
                        }
                    });
                    this.ui.timeZone.select2({
                        data: moment.tz.names()
                    });
                    this.ui.timeZone.val(this.model.get('timeZone')).trigger("change", { 'manual': true });
                } else {
                    this.ui.startTime.daterangepicker(dateObj);
                    this.ui.endTime.daterangepicker(dateObj);
                    this.ui.timeZone.html(tzstr);
                    this.ui.timeZone.select2({
                        placeholder: "Select TimeZone",
                        allowClear: true,
                        data: moment.tz.names()
                    });
                }
            },
            buttonActive: function(option) {
                var that = this;
                if (option && option.isButtonActive && that.tagModel) {
                    var isButton = option.isButtonActive;
                    this.parentView.modal.$el.find('button.ok').attr("disabled", isButton === true ? false : true);
                }
            },
            onCloseButton: function() {
                if (this.parentView.collection.models.length > 0) {
                    this.model.destroy();
                }
                if (this.parentView.collection.models.length <= 0) {
                    this.parentView.ui.timeZoneDiv.hide();
                    this.parentView.ui.checkTimeZone.prop('checked',false);
                    this.parentView.modal.$el.find('button.ok').attr("disabled",true);
                }
            }
        });
});