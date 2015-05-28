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

'use strict';

angular.module('dgc.lineage').controller('LineageController', ['$element', '$scope', '$state', '$stateParams', 'lodash', 'LineageResource', 'd3',
    function($element, $scope, $state, $stateParams, _, LineageResource, d3) {

   
        var metaData = {
            "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
            "typeName":"__tempQueryResultStruct5",
            "values":{
                "vertices":{
                    "2318d240-481e-421c-a614-843347d03941":{
                        "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                        "typeName":"__tempQueryResultStruct4",
                        "values":{
                            "vertexId":{
                                "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                                "typeName":"__IdType",
                                "values":{
                                    "guid":"2318d240-481e-421c-a614-843347d03941",
                                    "typeName":"Table"
                                }
                            },
                            "name":"sales_fact_daily_mv"
                        }
                    },
                    "2e2ab719-842e-4150-95bd-c9f684b3e3bf":{
                        "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                        "typeName":"__tempQueryResultStruct4",
                        "values":{
                            "vertexId":{
                                "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                                "typeName":"__IdType",
                                "values":{
                                    "guid":"2e2ab719-842e-4150-95bd-c9f684b3e3bf",
                                    "typeName":"Table"
                                }
                            },
                            "name":"time_dim"
                        }
                    },
                    "d3991d56-4600-415f-acdb-6f1b0c8079b2":{
                        "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                        "typeName":"__tempQueryResultStruct4",
                        "values":{
                            "vertexId":{
                                "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                                "typeName":"__IdType",
                                "values":{
                                    "guid":"d3991d56-4600-415f-acdb-6f1b0c8079b2",
                                    "typeName":"Table"
                                }
                            },
                            "name":"sales_fact_monthly_mv"
                        }
                    },
                    "7b516348-d8bb-4624-b330-3082ae87e705":{
                        "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                        "typeName":"__tempQueryResultStruct4",
                        "values":{
                            "vertexId":{
                                "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                                "typeName":"__IdType",
                                "values":{
                                    "guid":"7b516348-d8bb-4624-b330-3082ae87e705",
                                    "typeName":"Table"
                                }
                            },
                            "name":"sales_fact"
                        }
                    }, "7b516348-d8bb-4624-b330-3082ae87e706":{
                        "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                        "typeName":"__tempQueryResultStruct4",
                        "values":{
                            "vertexId":{
                                "jsonClass":"org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$_Struct",
                                "typeName":"__IdType",
                                "values":{
                                    "guid":"7b516348-d8bb-4624-b330-3082ae87e705",
                                    "typeName":"Table"
                                }
                            },
                            "name":"sales_fact_2"
                        }
                    }
                },
                "edges":{
                    "2318d240-481e-421c-a614-843347d03941":[
                        "0f45a2df-9bd5-4579-88ec-6ad0ff251d6f"
                    ],
                    "0f45a2df-9bd5-4579-88ec-6ad0ff251d6f":[
                        "7b516348-d8bb-4624-b330-3082ae87e705",
                        "2e2ab719-842e-4150-95bd-c9f684b3e3bf"
                    ],
                    "d3991d56-4600-415f-acdb-6f1b0c8079b2":[
                        "3710a18a-6116-4625-bf99-57670a7a90a8"
                    ],
                    "3710a18a-6116-4625-bf99-57670a7a90a8":[
                        "2318d240-481e-421c-a614-843347d03941"
                    ],
                    "7b516348-d8bb-4624-b330-3082ae87e706": [
                        "2318d240-481e-421c-a614-843347d03941"
                    ]
                }
            }
        };

        function transformData(metaData){
            var nodes = [];
            var name, guid;
            var nodeGuids = Object.keys(metaData.values.vertices);
            for (var index in nodeGuids) {
                name = metaData.values.vertices[nodeGuids[index]].values.name;
                guid = nodeGuids[index];
                nodes.push({ guid: guid, label: name, shape: "rect" });
            }

            var edges = [];
            var parent;
            var child;
            var edgesParents = Object.keys(metaData.values.edges)
            for(var index in edgesParents){
                parent =  edgesParents[index];
                for(var j = 0; j < metaData.values.edges[parent].length; j++ ) {
                    child = metaData.values.edges[parent][j];
                    if(!metaData.values.vertices.hasOwnProperty(child)) {
                        nodes.push({guid: child, label: 'Load Process', shape: "circle"});
                    }
                    edges.push({parent: parent, child: child});
                }
            }
            return {nodes: nodes, edges: edges};
        }




        function renderGraph(data, element){

            // Create a new directed graph
            var g = new dagreD3
                .graphlib
                .Graph()
                .setGraph({rankdir: "LR"});

// Automatically label each of the nodes
//g.setNode("DB (sales)", { label: "Sales DB",  width: 144, height: 100 })
//states.forEach(function(state) { g.setNode(state, { label: state }); });

            _.forEach(data.nodes, function (node) {
                g.setNode(node.guid, { label: node.label, shape: node.shape });
            });

            _.forEach(data.edges, function(edge) {
                g.setEdge(edge.parent, edge.child, {label: ""});
            });

// Set some general styles
            g.nodes().forEach(function(v) {
                var node = g.node(v);
                node.rx = node.ry = 5;
            });

            var inner = element.select("g");

// Create the renderer
            var render = new dagreD3.render();

// Run the renderer. This is what draws the final graph.
            render(inner, g);

// Center the graph
            var initialScale = 0.75;
            zoom
                .translate([(element.attr("width") - g.graph().width * initialScale) / 2, 20])
                .scale(initialScale)
                .event(element);
            element.attr('height', g.graph().height * initialScale + 90);
        }

        renderGraph(transformData(metaData), d3.select($element[0]).select('svg'));

    }
]);
