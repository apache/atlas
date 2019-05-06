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
    'hbs!tmpl/site/Statistics_tmpl',
    'hbs!tmpl/site/Statistics_Notification_table_tmpl',
    'hbs!tmpl/site/entity_tmpl',
    'modules/Modal',
    'models/VCommon',
    'utils/UrlLinks',
    'collection/VTagList',
    'utils/CommonViewFunction',
    'utils/Enums',
    'moment',
    'utils/Utils',
    'moment-timezone'
], function(require, Backbone, StatTmpl, StatsNotiTable, EntityTable, Modal, VCommon, UrlLinks, VTagList, CommonViewFunction, Enums, moment, Utils) {
    'use strict';

    var StatisticsView = Backbone.Marionette.LayoutView.extend(
        /** @lends AboutAtlasView */
        {
            template: StatTmpl,

            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                entityHeader: "[data-id='entity'] .count",
                serverCard: "[data-id='server-card']",
                connectionCard: "[data-id='connection-card']",
                notificationCard: "[data-id='notification-card']",
                statsNotificationTable: "[data-id='stats-notification-table']",
                notificationSmallCard: "[data-id='notification-small-card']",
                entityCard: "[data-id='entity-card']"
            },
            /** ui events hash */
            events: function() {},
            /**
             * intialize a new AboutAtlasView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, options);
                var that = this;
                var modal = new Modal({
                    title: 'Statistics',
                    content: this,
                    okCloses: true,
                    okText: "Close",
                    showFooter: true,
                    allowCancel: false,
                    width: "60%",
                    headerButtons: [{
                        title: "Refresh Data",
                        btnClass: "fa fa-refresh",
                        onClick: function() {
                            modal.$el.find('.header-button .fa-refresh').tooltip('hide').prop('disabled', true).addClass('fa-spin');
                            that.fetchMetricData({ update: true });
                        }
                    }]
                }).open();

                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
                this.modal = modal;
            },
            bindEvents: function() {},
            fetchMetricData: function(options) {
                var that = this,
                    entityCountCollection = new VTagList();
                entityCountCollection.url = UrlLinks.entityCountApi();
                entityCountCollection.modelAttrName = "data";
                entityCountCollection.fetch({
                    success: function(data) {
                        var data = _.first(data.toJSON());
                        that.renderStats({ valueObject: data.general.stats, dataObject: data.general });
                        that.renderEntities({ data: data });
                        that.$('.statsContainer,.statsNotificationContainer').removeClass('hide');
                        that.$('.statsLoader,.statsNotificationLoader').removeClass('show');
                        if (options && options.update) {
                            that.modal.$el.find('.header-button .fa-refresh').prop('disabled', false).removeClass('fa-spin');
                            Utils.notifySuccess({
                                content: "Metric data is refreshed"
                            })
                        }
                    }
                });
            },
            onRender: function() {
                this.fetchMetricData();
            },
            genrateStatusData: function(stateObject) {
                var that = this,
                    stats = {};
                _.each(stateObject, function(val, key) {
                    var keys = key.split(":"),
                        key = keys[0],
                        subKey = keys[1];
                    if (stats[key]) {
                        stats[key][subKey] = val;
                    } else {
                        stats[key] = {};
                        stats[key][subKey] = val;
                    }
                });
                return stats;
            },
            renderEntities: function(options) {
                var that = this,
                    data = options.data,
                    entityData = data.entity,
                    activeEntities = entityData.entityActive || {},
                    deletedEntities = entityData.entityDeleted || {},
                    stats = {},
                    activeEntityCount = 0,
                    deletedEntityCount = 0,
                    createEntityData = function(opt) {
                        var entityData = opt.entityData,
                            type = opt.type;
                        _.each(entityData, function(val, key) {
                            var intVal = _.isUndefined(val) ? 0 : val;
                            if (type == "active") {
                                activeEntityCount += intVal;
                            } else {
                                deletedEntityCount += intVal;
                            }
                            intVal = _.numberFormatWithComa(intVal)
                            if (stats[key]) {
                                stats[key][type] = intVal;
                            } else {
                                stats[key] = {};
                                stats[key][type] = intVal;
                            }
                        })
                    };
                createEntityData({
                    "entityData": activeEntities,
                    "type": "active"
                })
                createEntityData({
                    "entityData": deletedEntities,
                    "type": "deleted"
                });
                if (!_.isEmpty(stats)) {
                    that.ui.entityCard.html(
                        EntityTable({
                            "data": _.pick(stats, (_.keys(stats).sort())),
                        })
                    );
                    that.$('[data-id="activeEntity"]').html("&nbsp;(" + _.numberFormatWithComa(activeEntityCount) + ")");
                    that.$('[data-id="deletedEntity"]').html("&nbsp;(" + _.numberFormatWithComa(deletedEntityCount) + ")");
                    that.ui.entityHeader.html("&nbsp;(" + _.numberFormatWithComa(data.general.entityCount) + ")");
                }
            },
            renderStats: function(options) {
                var that = this,
                    data = this.genrateStatusData(options.valueObject),
                    generalData = options.dataObject,
                    createTable = function(obj) {
                        var tableBody = '',
                            enums = obj.enums,
                            data = obj.data;
                        _.each(data, function(value, key, list) {
                            tableBody += '<tr><td>' + key + '</td><td class="">' + that.getValue({
                                "value": value,
                                "type": enums[key]
                            }) + '</td></tr>';
                        });
                        return tableBody;
                    };
                if (data.Notification) {
                    var tableCol = [{
                                label: "Total <br> (from " + (that.getValue({
                                    "value": data.Server["startTimeStamp"],
                                    "type": Enums.stats.Server["startTimeStamp"],
                                })) + ")",
                                key: "total"
                            },
                            {
                                label: "Current Hour <br> (from " + (that.getValue({
                                    "value": data.Notification["currentHourStartTime"],
                                    "type": Enums.stats.Notification["currentHourStartTime"],
                                })) + ")",
                                key: "currentHour"
                            },
                            { label: "Previous Hour", key: "previousHour" },
                            {
                                label: "Current Day <br> (from " + (that.getValue({
                                    "value": data.Notification["currentDayStartTime"],
                                    "type": Enums.stats.Notification["currentDayStartTime"],
                                })) + ")",
                                key: "currentDay"
                            },
                            { label: "Previous Day", key: "previousDay" }
                        ],
                        tableHeader = ["count", "AvgTime", "EntityCreates", "EntityUpdates", "EntityDeletes", "Failed"];
                    that.ui.notificationCard.html(
                        StatsNotiTable({
                            "enums": Enums.stats.Notification,
                            "data": data.Notification,
                            "tableHeader": tableHeader,
                            "tableCol": tableCol,
                            "getTmplValue": function(argument, args) {
                                var pickValueFrom = argument.key.concat(args);
                                if (argument.key == "total" && args == "EntityCreates") {
                                    pickValueFrom = "totalCreates";
                                } else if (argument.key == "total" && args == "EntityUpdates") {
                                    pickValueFrom = "totalUpdates";
                                } else if (argument.key == "total" && args == "EntityDeletes") {
                                    pickValueFrom = "totalDeletes";
                                } else if (args == "count") {
                                    pickValueFrom = argument.key;
                                }
                                var returnVal = data.Notification[pickValueFrom];
                                return returnVal ? _.numberFormatWithComa(returnVal) : 0;
                            }
                        })
                    );

                    that.ui.notificationSmallCard.html(
                        createTable({
                            "enums": Enums.stats.Notification,
                            "data": _.pick(data.Notification, 'lastMessageProcessedTime', 'offsetCurrent', 'offsetStart')
                        })
                    );
                }

                if (data.Server) {
                    that.ui.serverCard.html(
                        createTable({
                            "enums": _.extend(Enums.stats.Server, Enums.stats.ConnectionStatus, Enums.stats.generalData),
                            "data": _.extend(
                                _.pick(data.Server, 'startTimeStamp', 'activeTimeStamp', 'upTime', 'statusBackendStore', 'statusIndexStore'),
                                _.pick(generalData, 'collectionTime'))
                        })
                    );
                }
            },
            getValue: function(options) {
                var value = options.value,
                    type = options.type;
                if (type == 'time') {
                    return Utils.millisecondsToTime(value);
                } else if (type == 'day') {
                    return moment.tz(value, moment.tz.guess()).format("MM/DD/YYYY h:mm A z");
                } else if (type == 'number') {
                    return _.numberFormatWithComa(value);
                } else if (type == 'millisecond') {
                    return _.numberFormatWithComa(value) + " millisecond/s";
                } else if (type == "status-html") {
                    return '<span class="connection-status ' + value + '"></span>';
                } else {
                    return value;
                }
            }
        });
    return StatisticsView;
});