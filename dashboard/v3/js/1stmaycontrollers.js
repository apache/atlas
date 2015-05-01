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

var DgcControllers = angular.module("DgcControllers", []);

 DgcControllers.service('sharedProperties', function () {
        var property="";
		var Query="";
		
        return {
            getProperty: function () {
                return property;
            },
            setProperty: function(value) {
                property = value;
            },
			getQuery: function () {
                return Query;
            },setQuery: function(value) {
                Query = value;
            }
        };
});



DgcControllers.controller("headerController", ['$scope', '$window', '$location', '$stateParams', function($scope, $window, $location,$stateParams)
    {
		$scope.executeSearch=function executeSearch() {
            $window.location.href = "#Search/" + $scope.query;
			
			
        }
		$scope.query=$stateParams.searchid;
    }]
);

DgcControllers.controller("footerController", ['$scope','$http', function($scope, $http)
    {
        $http.get('/api/metadata/admin/version')
            .success(function (data) {
                $scope.iserror1=false;
                $scope.apiVersion=data.Version;

            })
            .error(function (e) {
                $scope.iserror1=true;
                $scope.error1=e;
            });

    }]
);


DgcControllers.controller("NavController", ['$scope','$http', '$filter', 'sharedProperties', function($scope, $http, $filter, sharedProperties)
{

    $http.get('/api/metadata/types/traits/list')
        .success(function (data) {
            $scope.iserror1=false;
            $scope.leftnav=angular.fromJson(data.results);

        })
        .error(function (e) {
            $scope.iserror1=true;
            $scope.error1=e;
        });
        //Nav to textbox

         $scope.updateVar = function (event) {
         $scope.$$prevSibling.query = angular.element(event.target).text();
        
    };


}]
);


DgcControllers.controller("ListController", ['$scope','$http', '$filter','$stateParams', 'sharedProperties', function($scope, $http, $filter, $stateParams, sharedProperties)
    {


        $scope.isUndefined = function (strval) {

            return (typeof strval === "undefined");
        }
		
		$scope.StoreJson = function (strval) {
            sharedProperties.setProperty(strval);			
        }

        $scope.Showpaging = function(itemlength)
        {

            return (itemlength > 1);
        }

        $scope.isString=function isString(value){
            return typeof value === 'string';
        }

        $scope.isObject=function isObject(value){

            return typeof value === 'object';
        }
        $scope.Storeqry=function Storeqry(value){

            return typeof value === 'object';
        }



        $scope.executeSearchForleftNav = function executeSearchForleftNav(strSearch){
            $scope.query=strSearch;
			 sharedProperties.setQuery(strSearch);
            //$scope.executeSearch();
        }

        console.log($stateParams.searchid);
           $scope.SearchQuery=$stateParams.searchid;
            $scope.reverse = false;
            $scope.filteredItems = [];
            $scope.groupedItems = [];
            $scope.itemsPerPage = 10;
            $scope.pagedItems = [];
            $scope.currentPage = 0;
            $scope.itemlength=0;
            $scope.configdata=[];
            $scope.results=[];
            $scope.datatype="";
            $http.get('js/config.json').success(function(data){
                $scope.configdata=data.Search;

            });


            $http.get('/api/metadata/discovery/search?query='+$scope.SearchQuery)
                .success(function (data) {
                    $scope.iserror=false;
                    $scope.entities=angular.fromJson(data.results.rows);
                    if(!$scope.isUndefined($scope.entities)){
                        $scope.itemlength=$scope.entities.length;
                        $scope.datatype=data.results.dataType.typeName;

                        var i=0;
                        angular.forEach($scope.configdata, function(values, key) {
                            if (key === data.results.dataType.typeName) {
                                i=1;
                            }
                        });
                            if(i===0){
                                var tempdataType="__tempQueryResultStruct";
                                //console.log(tempdataType);
                                var datatype1=$scope.datatype.substring(0,tempdataType.length);
                               // console.log(datatype1);
                                if(datatype1===tempdataType){
                                    $scope.datatype=tempdataType;
                                }

                            }

                        sharedProperties.setProperty($scope.datatype);
                    }

               //     console.log($scope.entities);


                    // to get value based on config but not use (used in view directly)
                  /*  angular.forEach($scope.configdata, function(values, key) {
                        if(key===data.results.dataType.typeName)
                        {
                            $scope.entities.forEach(function(k,v){
                                    angular.forEach(values, function(value, key1) {
                                        var obj = {};
                                        obj[value] = k[value];
                                    $scope.results.push(obj);
                                });
                            });
                        }
                    });
                    */

                    $scope.currentPage = 0;
                    // now group by pages
                    $scope.groupToPages();


                });
                // .error(function (e) {
                //     alert("failed");
                //     $scope.iserror=true;
                //     $scope.error=e;


                // });

//click value to textbox

       $scope.updateVars = function (event) {
        var appElement = document.querySelector('[ng-model=query]');
    var $scope = angular.element(appElement).scope();
     $scope.query = angular.element(event.target).text();
    // $scope.$apply(function() {
    //   $scope.query = angular.element(event.target).text();
    // });

        
         console.log("test");
        console.log(angular.element(event.target).text());
         console.log("testingFact");
    };
    //click value to textbox
        $scope.getGuidName=function getGuidName(val){
            $http.get('/api/metadata/entities/definition/'+val)
                .success(function (data) {
                    $scope.iserror1=false;
                    if(!$scope.isUndefined(data.results)){
                        $scope.gname=angular.fromJson(data.results);
                        console.log(angular.fromJson(data.results));
                        // $scope.gname=data.results.name;
                    }

                })
                .error(function (e) {
                    $scope.iserror1=true;
                    $scope.error1=e;
                });
            //return $scope.gname;
        }


        // calculate page in place
        $scope.groupToPages = function () {
            $scope.pagedItems = [];

            for (var i = 0; i < $scope.itemlength; i++) {
                if (i % $scope.itemsPerPage === 0) {
                    $scope.pagedItems[Math.floor(i / $scope.itemsPerPage)] = [ $scope.entities[i] ];
                } else {
                    $scope.pagedItems[Math.floor(i / $scope.itemsPerPage)].push($scope.entities[i]);
                }
            }

        };

        $scope.range = function (start, end) {
            var ret = [];
            if (!end) {
                end = start;
                start = 0;
            }
            for (var i = start; i < end; i++) {
                ret.push(i);
            }
            return ret;
        };

        $scope.prevPage = function () {
            if ($scope.currentPage > 0) {
                $scope.currentPage--;
            }
        };

        $scope.nextPage = function () {
            if ($scope.currentPage < $scope.pagedItems.length - 1) {
                $scope.currentPage++;
            }
        };


        $scope.firstPage = function () {
            if ($scope.currentPage > 0) {
                $scope.currentPage = 0;
            }
        };

        $scope.lastPage = function () {
            if ($scope.currentPage < $scope.pagedItems.length - 1) {
                $scope.currentPage = $scope.pagedItems.length-1;
            }
        };
        $scope.setPage = function () {
            $scope.currentPage = this.n;
        };

    }]
);


DgcControllers.controller("DefinitionController", ['$scope','$http', '$stateParams', 'sharedProperties','$q', function($scope, $http, $stateParams, sharedProperties, $q)
    {

					$scope.guidName="";
					$scope.ids=[];
					$scope.isUndefined = function (strval) {
						return (typeof strval === "undefined");
					}
					
					$scope.isString=function isString(value){								
					 return typeof value === 'string' || getType(value)==='[object Number]';
					}
					
					var getType = function (elem) {
					return Object.prototype.toString.call(elem);
					};
					
					$scope.isObject=function isObject(value){
					 return typeof value === 'object';
					}
//onclick to textbox

       $scope.updateDetailsVariable = function (event) {
        var appElement = document.querySelector('[ng-model=query]');
    var $scope = angular.element(appElement).scope();
     $scope.query = angular.element(event.target).text();
    // $scope.$apply(function() {
    //   $scope.query = angular.element(event.target).text();
    // });

        
         console.log("test");
        console.log(angular.element(event.target).text());
         console.log("testing");
    };
//onclick to textbox
					$scope.getGuidName=function getGuidName(val){
					$http.get('/api/metadata/entities/definition/'+val)
						.success(function (data) {
						$scope.iserror1=false;
							if(!$scope.isUndefined(data.results)){								
							$scope.gname=angular.fromJson(data.results);
							//console.log(angular.fromJson(data.results));
                               // $scope.gname=data.results.name;
							}

						})
						   .error(function (e) {
							$scope.iserror1=true;
							$scope.error1=e;
						});
				return true;
					}
		
        $scope.Name=$stateParams.Id;
        $scope.searchqry=sharedProperties.getQuery();
        $scope.datatype1=sharedProperties.getProperty();

        $http.get('/api/metadata/entities/definition/'+$stateParams.Id)
                .success(function (data) {
                    $scope.iserror1=false;
                $scope.details=  angular.fromJson(data.results);
                if(!$scope.isUndefined( $scope.details)) {
                 //   console.log($scope.details['name']);
                     $scope.datatype1=$scope.details["$typeName$"];
                    $scope.getSchema($scope.details['name']);
                    $scope.getLinegae($scope.details['name']);
					$scope.getLinegaeforinput($scope.details['name']);
                }
            })
               .error(function (e) {
                $scope.iserror1=true;
                $scope.error1=e;
            });


        $scope.getSchema= function (tableName) {

            $http.get('/api/metadata/lineage/hive/schema/'+tableName)
                .success(function (data) {
                    $scope.iserror1=false;
                    $scope.schema=  angular.fromJson(data.results.rows);
                  //  console.log(tableName);


                })
                .error(function (e) {
                    $scope.iserror1=true;
                    $scope.error1=e;
                });
        }

$scope.getLinegae= function (tableName) {

//            $scope.width = 900;
//            $scope.height = 900;
			var arr=[];
            var arrmyalias=[];
			   var datatypes=[];
			   var tags=[];
            $http.get('/api/metadata/lineage/hive/outputs/'+tableName)
                .success(function (data) {
                    $scope.iserror1=false;
                    $scope.lineage=  angular.fromJson(data.results.rows);
					 
                    $scope.vts = [];
                    $scope.edges1 = [];
                    $scope.listguid = [];
                    angular.forEach($scope.lineage, function(lineage1){
                    var level = 0;
                    angular.forEach(lineage1.path, function(item, index){
                      //  if ($scope.listguid.indexOf(index) == -1) {
                       //     $scope.listguid.push(index);

                        $scope.vts.push({"Name": item.guid,"Id" :index,"hasChild":"True","type":item.typeName});
                        $scope.edges1.push({source: index, target: (index+1)});

                       // }
                    });

                  });


                    var newarr = [];
                    var unique = {};

                    angular.forEach($scope.edges1, function(item) {
                        if (!unique[item.source]) {
                            newarr.push(item);
                            unique[item.source] = item;
                            //console.log(newarr);
                        }
                    });

                    var newarrvts = [];
                    var uniquevts = {};

                    angular.forEach($scope.vts, function(item) {
                        if (!uniquevts[item.Name]) {
                            newarrvts.push(item);
                            uniquevts[item.Name] = item;

							  var url="/api/metadata/entities/definition/"+item.Name;
							   arr.push($http.get(url)); 
                        }
                    });


					 $q.all(arr).then(function(ret){
                    //console.log("Result guid list length="+ret.length);
                    for(var i=0;i<ret.length;i++){
                        var f=angular.fromJson(ret[i].data.results);
                        //console.log(i+"Their Names="+angular.toJson(f));
                        //console.log(i+"Their Names="+f.name);
                        arrmyalias[i]=f.name;
					 
								datatypes[i]=f['$typeName$']; 
							 
						if(f['$typeName$']==="Table")
						{
							angular.forEach(f['$traits$'], function(key, value) {
								tags[i]=value;
							 
								  });
						}
						else{
								tags[i]=f.queryText;
						}
					

                    }
if(arrmyalias.length>1){
                                                 doMakeStaticJson(arrmyalias);
                                             }
                                             else{
                                                 $scope.errornodata="";
                                             }
//                    loadjsonRealv2(arrmyalias);
                   // doMakeStaticJson(arrmyalias);

                });

				  })
                .error(function (e) {
                    $scope.iserror1=true;
                    $scope.error1=e;
                });

            function doMakeStaticJson(arrmyalias){

                var toparr=[];
			console.log(arrmyalias.length);
                var rootobj=new Object();
                rootobj.name=arrmyalias[0];
                rootobj.alias=arrmyalias[0];
				rootobj.query=tags[0];
				rootobj.datatype=datatypes[0];
                rootobj.parent="null";

                toparr[0]=rootobj;

//start first object
                var child1obj=new Object();
                child1obj.alias=arrmyalias[1];
                child1obj.name=arrmyalias[1];
				child1obj.query=tags[1];
				child1obj.datatype=datatypes[1];
                child1obj.parent=arrmyalias[0];

                    //start
                    var childsub1obj=new Object();
                    childsub1obj.name=arrmyalias[2];
                    childsub1obj.alias=arrmyalias[2];
					   childsub1obj.query=tags[2];
					   childsub1obj.datatype=datatypes[2];
                    childsub1obj.parent=arrmyalias[1];
					if(arrmyalias.length>2){
                    var arraychildren1=[];
                    arraychildren1.push(childsub1obj);
                    child1obj.children=arraychildren1;
					}
                        //start
                        var childsub2obj=new Object();
                        childsub2obj.name=arrmyalias[3];
                        childsub2obj.alias=arrmyalias[3];
							childsub2obj.query=tags[3];
							childsub2obj.datatype=datatypes[3];
                        childsub2obj.parent=arrmyalias[2];
						if(arrmyalias.length>3){
                        var arraychildren2=[];
                        arraychildren2.push(childsub2obj);
                        childsub1obj.children=arraychildren2;
						}

                            //start
                            var childsub3obj=new Object();
                            childsub3obj.name=arrmyalias[4];
                            childsub3obj.alias=arrmyalias[4];
							childsub3obj.query=tags[4];
							childsub3obj.datatype=datatypes[4];
                            childsub3obj.parent=arrmyalias[3];
							if(arrmyalias.length>4){
                            var arraychildren3=[];
                            arraychildren3.push(childsub3obj);
                            childsub2obj.children=arraychildren3;
							}

///end first objects



                /*var arraychildren2=[];
                arraychildren2.push(child1obj2);
//                arraychildren2.push(childsub2obj2);


                child1obj2.children=arraychildren2;


                //end second objects
                */

                var array1=[];
                array1[0]=child1obj;
//                array1[1]=child1obj2;


                rootobj.children=array1;


                //console.info("MITH SEE THIS="+angular.toJson(toparr));

                root = toparr[0];
		console.log(root);
                update(root);
            }


                    //Width and height
                 var width = 700,
                     height = 500,
                     root;
//image intitializer
 var mitharr=["img/tableicon.png","img/process.png","img/tableicon.png","img/process.png","img/tableicon.png"];



                 var force = d3.layout.force()
                     .gravity(0)
                     .friction(0.7)
                     .charge(-90)
					 .linkDistance(120)
					 .size([width, height])
                      .on("tick", tick);

                 var svg = d3.select("svg")
//                 .attr("transform", "translate(" + (width/2) +
//                          "," + (height/2) + ")")

                          .attr('transform-origin', '-419 -530')
                          .attr("viewBox", "10 -300 1000 1000")
                            .attr("preserveAspectRatio", "xMidYMid meet");
//                               .append("g")
//                               .attr("transform", "translate(" + d.x + "," + d.y +") rotate(180) scale(-1, -1)");


//                 .attr("preserveAspectRatio", "xMidYMid slice");

                 var link = svg.selectAll(".link"),
                     node = svg.selectAll(".node");

						var tip = d3.tip()
                        .attr('class', 'd3-tip')
                        .offset([-10, 0])
                        .html(function(d) {
                            return "<pre class='alert alert-success' style='max-width:400px;'>" + d.query + "</pre>";
                        });
                    svg.call(tip);

                        function update(source) {
 var nodes = flatten(root),
      links = d3.layout.tree().links(nodes);

  // Restart the force layout.
  force
      .nodes(nodes)
      .links(links)

      .start();

  // Update links.
  link = link.data(links, function(d) { return d.target.id; });

  link.exit().remove();

  link.enter().insert("line", ".node")
      .attr("class", "link");

  // Update nodes.
  node = node.data(nodes, function(d) { return d.id; });

  node.exit().remove();

    svg.append("svg:pattern").attr("id","processICO").attr("width",1).attr("height",1)
                        .append("svg:image").attr("xlink:href","./img/process.png").attr("x",-5.5).attr("y",-4).attr("width",42).attr("height",42);
                    svg.append("svg:pattern").attr("id","textICO").attr("width",1).attr("height",1)
                        .append("svg:image").attr("xlink:href","./img/tableicon.png").attr("x",2).attr("y",2).attr("width",25).attr("height",25);


//arrow
 svg.append("svg:defs").append("svg:marker").attr("id", "arrow").attr("viewBox", "0 0 10 10").attr("refX", 36).attr("refY", 5).attr("markerUnits", "strokeWidth").attr("markerWidth", 8).attr("markerHeight", 8).attr("orient", "auto").append("svg:path").attr("d", "M 0 0 L 10 5 L 0 10 z");
//arrow
  var nodeEnter = node.enter().append("g")
      .attr("class", "nodeTrans")
	  .on("mouseover", tip.show)
      .on("mouseout", tip.hide) 
      .call(force.drag);




  nodeEnter.append("circle")
      .attr("r", function(d) { return 15; });

    link.attr("marker-end", "url(#arrow)"); //also added attribute for arrow at end

  nodeEnter.append("text")
      .style("text-anchor", "middle")


      .attr("dy", "-1em")

       .attr("text-anchor", function(d) {
      		  return d.children || d._children ? "end" : "start"; })
      	  .text(function(d) {
                                      return d.alias;
                                      //return d.name;
                              })

      	  .style("fill-opacity", 1);


// nodeEnter.select("circle")
//      .attr("xlink:href", function(d) {
//                                            //return d.icon;
//                                            return mitharr[d.depth];
//                                      })
//      .attr("x", "-12px")
//      .attr("y", "-12px")
//      .attr("width", "24px")
//      .attr("height", "24px");


  node.select("circle")
//    .attr("xlink:href", function(d) {
//                                              //return d.icon;
//                                              return mitharr[d.depth];
//                                        })
      .style("fill", function(d, i) {
                               if(d.datatype==="Table"){
                                return "url('#textICO')";

                            }else{
                                 return "url('#processICO')";
                            }
                            return colors(i);
                        });
//force.stop();
//force.resume();

}

//force.stop();
function tick() {
  link.attr("x1", function(d) { return d.source.x; })
      .attr("y1", function(d) { return d.source.y; })
      .attr("x2", function(d) { return d.target.x; })
      .attr("y2", function(d) { return d.target.y; });


    node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")  " });

}

//node[0].x = width / 2;
//    node[1].y = height / 2;
d3.select(window).on('resize', update);

function color(d) {
  return d._children ? "#3182bd" // collapsed package
      : d.children ? "#c6dbef" // expanded package
      : "#fd8d3c"; // leaf node
}

// Toggle children on click.
function click(d) {
  if (d3.event.defaultPrevented) return; // ignore drag
  if (d.children) {
    d._children = d.children;
    d.children = null;
  } else {
    d.children = d._children;
    d._children = null;
  }
  update();

}

// Returns a list of all nodes  the root.
function flatten(root) {
  var nodes = [], i = 0;

  function recurse(node) {
    if (node.children) node.children.forEach(recurse);
    if (!node.id) node.id = ++i;
    nodes.push(node);
  }

  recurse(root);
  return nodes;

}


//
//                })
//                .error(function (e) {
//                    $scope.iserror1=true;
//                    $scope.error1=e;
//                });


        }



		
		
$scope.getLinegaeforinput= function (tableName) {

//            $scope.width = 900;
//            $scope.height = 900;
			var arr=[];
            var arrmyalias=[];
			   var datatypes=[];
			   var tags=[];
            $http.get('/api/metadata/lineage/hive/inputs/'+tableName)
                .success(function (data) {
                    $scope.iserror1=false;
                    $scope.lineage=  angular.fromJson(data.results.rows);
								 
                    $scope.vts = [];
                    $scope.edges1 = [];
                    $scope.listguid = [];
                    angular.forEach($scope.lineage, function(lineage1){
                    var level = 0;
                    angular.forEach(lineage1.path, function(item, index){
                      //  if ($scope.listguid.indexOf(index) == -1) {
                       //     $scope.listguid.push(index);

                        $scope.vts.push({"Name": item.guid,"Id" :index,"hasChild":"True","type":item.typeName});
                        $scope.edges1.push({source: index, target: (index+1)});
				
                       // }
                    });

                  });

                    var newarr = [];
                    var unique = {};

                    angular.forEach($scope.edges1, function(item) {
                        if (!unique[item.source]) {
                            newarr.push(item);
                            unique[item.source] = item;
                            //console.log(newarr);
                        }
                    });

                    var newarrvts = [];
                    var uniquevts = {};

                    angular.forEach($scope.vts, function(item) {
                        if (!uniquevts[item.Name]) {
                            newarrvts.push(item);
                            uniquevts[item.Name] = item;

							  var url="/api/metadata/entities/definition/"+item.Name;
							   arr.push($http.get(url));

                            //getLienageGuidName(item.Name);
                          
                        }
                    });
				console.log(item.Name);

					 $q.all(arr).then(function(ret){
                    //console.log("Result guid list length="+ret.length);
                    for(var i=0;i<ret.length;i++){
                        var f=angular.fromJson(ret[i].data.results);
                        //console.log(i+"Their Names="+angular.toJson(f));
                        //console.log(i+"Their Names="+f.name);
                        arrmyalias[i]=f.name;
				 
								datatypes[i]=f['$typeName$'];
					 
						if(f['$typeName$']==="Table")
						{
							angular.forEach(f['$traits$'], function(key, value) {
								tags[i]=value;
							   console.log(value);
								  });
						}
						else{
								tags[i]=f.queryText;
								   console.log(f.queryText);
						}
					

                    }
					  console.log(arrmyalias);
					
if(arrmyalias.length>1){
                                                 doMakeStaticJson(arrmyalias);
                                             }
                                             else{
                                                 $scope.errornodata1="";
                                             }
//                    loadjsonRealv2(arrmyalias);
                   // doMakeStaticJson(arrmyalias);

                });

				  })
                .error(function (e) {
                    $scope.iserror1=true;
                    $scope.error1=e;
                });

            function doMakeStaticJson(arrmyalias){

                var toparr=[];

                var rootobj=new Object();
                rootobj.name=arrmyalias[0];
                rootobj.alias=arrmyalias[0];
				rootobj.query=tags[0];
				rootobj.datatype=datatypes[0];
                rootobj.parent="null";

                toparr[0]=rootobj;

//start first object
                var child1obj=new Object();
                child1obj.alias=arrmyalias[1];
                child1obj.name=arrmyalias[1];
				child1obj.query=tags[1];
				child1obj.datatype=datatypes[1];
                child1obj.parent=arrmyalias[0];

                    //start
                    var childsub1obj=new Object();
                    childsub1obj.name=arrmyalias[2];
                    childsub1obj.alias=arrmyalias[2];
					   childsub1obj.query=tags[2];
					   childsub1obj.datatype=datatypes[2];
                    childsub1obj.parent=arrmyalias[1];
					
					if(arrmyalias.length>2){
                    var arraychildren1=[];
                    arraychildren1.push(childsub1obj);
                    child1obj.children=arraychildren1;
					}
                        //start
                        var childsub2obj=new Object();
                        childsub2obj.name=arrmyalias[3];
                        childsub2obj.alias=arrmyalias[3];
							childsub2obj.query=tags[3];
							childsub2obj.datatype=datatypes[3];
                        childsub2obj.parent=arrmyalias[2];
						if(arrmyalias.length>3){
                        var arraychildren2=[];
                        arraychildren2.push(childsub2obj);
                        childsub1obj.children=arraychildren2;
						}

                            //start
                            var childsub3obj=new Object();
                            childsub3obj.name=arrmyalias[4];
                            childsub3obj.alias=arrmyalias[4];
							childsub3obj.query=tags[4];
							childsub3obj.datatype=datatypes[4];
                            childsub3obj.parent=arrmyalias[3];
							
								if(arrmyalias.length>4){
                            var arraychildren3=[];
                            arraychildren3.push(childsub3obj);
                            childsub2obj.children=arraychildren3;
								}


///end first objects



                /*var arraychildren2=[];
                arraychildren2.push(child1obj2);
//                arraychildren2.push(childsub2obj2);


                child1obj2.children=arraychildren2;


                //end second objects
                */

                var array1=[];
                array1[0]=child1obj;
//                array1[1]=child1obj2;


                rootobj.children=array1;


                //console.info("MITH SEE THIS="+angular.toJson(toparr));

                root = toparr[0];
				console.log("test input");
	console.log(root);
                update(root);
            }


                    //Width and height
                 var width = 700,
                     height = 500,
                     root;
//image intitializer
 var mitharr=["img/tableicon.png","img/process.png","img/tableicon.png","img/process.png","img/tableicon.png"];



                 var force = d3.layout.force()
                     .gravity(0)
                     .friction(0.7)
                     .charge(-90)
					 .linkDistance(120)
					 .size([width, height])
                      .on("tick", tick);

                 var svg = d3.select("svg1").append("svg")
//                 .attr("transform", "translate(" + (width/2) +
//                          "," + (height/2) + ")")

                          .attr('transform-origin', '-419 -530')
                          .attr("viewBox", "10 -300 1000 1000")
                            .attr("preserveAspectRatio", "xMidYMid meet");
//                               .append("g")
//                               .attr("transform", "translate(" + d.x + "," + d.y +") rotate(180) scale(-1, -1)");


//                 .attr("preserveAspectRatio", "xMidYMid slice");

                 var link = svg.selectAll(".link"),
                     node = svg.selectAll(".node");

						var tip = d3.tip()
                        .attr('class', 'd3-tip')
                        .offset([-10, 0])
                        .html(function(d) {
                            return "<pre class='alert alert-success' style='max-width:400px;'>" + d.query + "</pre>";
                        });
						
						if(svg){
							  svg.call(tip);
						}
                  

                        function update(source) {
 var nodes = flatten(root),
      links = d3.layout.tree().links(nodes);

  // Restart the force layout.
  force
      .nodes(nodes)
      .links(links)

      .start();

  // Update links.
  link = link.data(links, function(d) { return d.target.id; });

  link.exit().remove();

  link.enter().insert("line", ".node")
      .attr("class", "link");

  // Update nodes.
  node = node.data(nodes, function(d) { return d.id; });

  node.exit().remove();

    svg.append("svg:pattern").attr("id","processICO1").attr("width",1).attr("height",1)
                        .append("svg:image").attr("xlink:href","./img/process.png").attr("x",-5.5).attr("y",-4).attr("width",42).attr("height",42);
                    svg.append("svg:pattern").attr("id","textICO1").attr("width",1).attr("height",1)
                        .append("svg:image").attr("xlink:href","./img/tableicon.png").attr("x",2).attr("y",2).attr("width",25).attr("height",25);


//arrow
 svg.append("svg:defs").append("svg:marker").attr("id", "arrow1").attr("viewBox", "0 0 10 10").attr("refX", 60).attr("refY", 5).attr("markerUnits", "strokeWidth").attr("markerWidth", 8).attr("markerHeight", 8).attr("orient", "auto").append("svg:path").attr("d", "M 0 0 L 10 5 L 0 10 z");
//arrow
  var nodeEnter = node.enter().append("g")
      .attr("class", "nodeTrans")
	  .on("mouseover", tip.show)
      .on("mouseout", tip.hide) 
      .call(force.drag);




  nodeEnter.append("circle")
      .attr("r", function(d) { return 15; });

    link.attr("marker-end", "url(#arrow1)"); //also added attribute for arrow at end

  nodeEnter.append("text")
      .style("text-anchor", "middle")


      .attr("dy", "-1em")

       .attr("text-anchor", function(d) {
      		  return d.children || d._children ? "end" : "start"; })
      	  .text(function(d) {
                                      return d.alias;
                                      //return d.name;
                              })

      	  .style("fill-opacity", 1);


// nodeEnter.select("circle")
//      .attr("xlink:href", function(d) {
//                                            //return d.icon;
//                                            return mitharr[d.depth];
//                                      })
//      .attr("x", "-12px")
//      .attr("y", "-12px")
//      .attr("width", "24px")
//      .attr("height", "24px");


  node.select("circle")
//    .attr("xlink:href", function(d) {
//                                              //return d.icon;
//                                              return mitharr[d.depth];
//                                        })
      .style("fill", function(d) {
                            if(d.datatype==="Table"){
                                return "url('#textICO1')";

                            }else{
                                 return "url('#processICO1')";
                            }
                            return colors(i);
                        });
//force.stop();
//force.resume();

}

//force.stop();
function tick() {
  link.attr("x1", function(d) { return d.source.x; })
      .attr("y1", function(d) { return d.source.y; })
      .attr("x2", function(d) { return d.target.x; })
      .attr("y2", function(d) { return d.target.y; });


    node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")  " });

}

//node[0].x = width / 2;
//    node[1].y = height / 2;
d3.select(window).on('resize', update);

function color(d) {
  return d._children ? "#3182bd" // collapsed package
      : d.children ? "#c6dbef" // expanded package
      : "#fd8d3c"; // leaf node
}

// Toggle children on click.
function click(d) {
  if (d3.event.defaultPrevented) return; // ignore drag
  if (d.children) {
    d._children = d.children;
    d.children = null;
  } else {
    d.children = d._children;
    d._children = null;
  }
  update();

}

// Returns a list of all nodes  the root.
function flatten(root) {
  var nodes = [], i = 0;

  function recurse(node) {
    if (node.children) node.children.forEach(recurse);
    if (!node.id) node.id = ++i;
    nodes.push(node);
  }

  recurse(root);
  return nodes;

}


//
//                })
//                .error(function (e) {
//                    $scope.iserror1=true;
//                    $scope.error1=e;
//                });


        }
		
		

      //  console.log( $scope.vts);

        $scope.reverse = function(array) {
            var copy = [].concat(array);
            return copy.reverse();
        }

        // function back()
        //  {
        //   $window.history.back();
        //   myModule.run(function ($rootScope, $location) {

        //       var history = [];

        //       $rootScope.$on('$routeChangeSuccess', function() {
        //           history.push($location.$$path);
        //       });

        //       $rootScope.back = function () {
        //           var prevUrl = history.length > 1 ? history.splice(-2)[0] : "/";
        //           $location.path(prevUrl);
        //       };

        //   });
        //  }




    }]
);


DgcControllers.controller("GuidController", ['$scope','$http', '$filter','$stateParams', 'sharedProperties', function($scope, $http, $filter, $stateParams, sharedProperties)
    {



$scope.getGuidName=function getGuidName(val){
  
        $scope.gnew=[];
                    $http.get('/api/metadata/entities/definition/'+val)
                        .success(function (data) {
                        $scope.iserror1=false;
                            if(!$scope.isUndefined(data.results)){  
                                           
                            $scope.gname=angular.fromJson(data.results);
                             var data1=angular.fromJson(data.results);
                             //$scope.gnew({"id" : val,"name" : data1['name']});

                           $scope.gnew= $scope.gname.name;
                           // $scope.$watch($scope.gnew, true);
                            
}
                               //dddd
                         

                        })
                           .error(function (e) {
                            $scope.iserror1=true;
                            $scope.error1=e;
                        });

                //return $scope.gnew;
                    }


 }]
);

