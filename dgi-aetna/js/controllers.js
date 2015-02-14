var DgcControllers = angular.module("DgcControllers", []);

DgcControllers.controller("ListController", ['$scope','$http', function($scope, $http)
		{    
			
			$scope.executeSearch = function executeSearch() {
				 $scope.SearchQuery=$scope.query;   
				$scope.iswiki=false;	
				//$http.get('http://162.249.6.76:21000/api/metadata/entities/list/'+$scope.query)
		var searchQry=$scope.query.split(",");
				$http.get('http://162.249.6.76:21000/api/metadata/discovery/search/fulltext?depth=1&'+searchQry[0]+'&'+searchQry[1])
				.success(function (data) {

					$scope.iserror=false;
					 $scope.entities=angular.fromJson(data.vertices);
					var count=0;
					angular.forEach(data.vertices, function(v, index) {
						count++;
					});			
					$scope.matchingResults=count;
				
				})
				.error(function (e) {
				    $scope.iserror=true;
				    $scope.error=e;
				   $scope.matchingResults=0;
				});

    			}

		}]
	);

DgcControllers.controller("DefinitionController", ['$scope','$http','$routeParams', function($scope, $http, $routeParams)
		{    
					$scope.isString=function isString(value){
					 return typeof value === 'string';
					}
					$scope.Guid=$routeParams.Id;
					$scope.iswiki=true;
					$scope.selectedDefination={
					"path":"wiki.html"
					};
					$http.get('http://162.249.6.76:21000/api/metadata/entities/definition/'+$routeParams.Id)
					.success(function (data) {
						$scope.iserror1=false;						
						 $scope.details=angular.fromJson(data.definition);
						

					})
					.error(function (e) {
					    $scope.iserror1=true;
					    $scope.error1=e;
					});

		}]
	);


DgcControllers.controller("LineageController", ['$scope','$http','$routeParams', function($scope, $http, $routeParams)
		{    
				

		$scope.width = 1110;
                $scope.height = 400;
          
            //Initialize a default force layout, using the nodes and edges in dataset
          
            


$http.get('http://162.249.6.76:21000/api/metadata/discovery/search/relationships/'+$routeParams.Id+'?depth=3&&edgesToFollow=HiveLineage.sourceTables.0,HiveLineage.sourceTables.1,HiveLineage.tableName')
					.success(function (data) {
						$scope.iserror1=false;						
						 $scope.lineage=angular.fromJson(data);
							 $scope.vertices = data.vertices; // response data
					    $scope.vts =  [];
					 $scope.egs =  [];
					 $scope.ids =  [];
						
					    angular.forEach(data.vertices, function(v, index) {	
				     if(v["hive_table.name"]==undefined){
					$scope.vts.push({"name" :index,"values":v["HiveLineage.query"],"guid":v["guid"],"hasChild":"True"});
					$scope.ids.push({"Id" :index,"Name":index,"values":v["HiveLineage.query"],"guid":v["guid"],"hasChild":"True"});
					}else{

						$scope.vts.push({"name" :v["hive_table.name"],"values":v["hive_table.description"],"guid":v["guid"],"hasChild":"False"});
					$scope.ids.push({"Id" :index,"Name":v["hive_table.name"],"values":v["hive_table.description"],"guid":v["guid"],"hasChild":"False"});
					}
						
								     
					    });
 					
				
					angular.forEach(data.edges, function(e, index) {	
				    	$scope.egs.push({"source" :e["head"],"target":e["tail"]});
								     
					    });


var edges2 = [];
  $scope.egs.forEach(function(e) { 
    var sourceNode = $scope.ids.filter(function(n) { return n.Id === e.source; })[0],
    targetNode = $scope.ids.filter(function(n) { return n.Id === e.target; })[0];
 if(sourceNode==undefined){
	sourceNode=e.source;
$scope.vts.push({"name" :e.source+", Missing Node","values":e.source+", Missing Node","guid":$scope.ids["guid"],"hasChild":"False"});
$scope.ids.push({"Id" :e.source,"Name":e.source+", Missing Node","values":e.source+", Missing Node","guid":$scope.ids["guid"],"hasChild":"False"});
  }

 if(targetNode==undefined){
	targetNode=e.target;
$scope.vts.push({"name" :e.target+", Missing Node","values":e.target+", Missing Node","guid":$scope.ids["guid"],"hasChild":"False"});
$scope.ids.push({"Id" :e.target,"Name":e.target+", Missing Node","values":e.target+", Missing Node","guid":$scope.ids["guid"],"hasChild":"False"});
  }

    edges2.push({source: sourceNode, target: targetNode});
    });

var edges1 = [];
  $scope.egs.forEach(function(e) { 
    var sourceNode = $scope.ids.filter(function(n) { return n.Id === e.source; })[0],
    targetNode = $scope.ids.filter(function(n) { return n.Id === e.target; })[0];
    edges1.push({source: sourceNode, target: targetNode});
    });

		  //Width and height
				    var w = 1110;
				    var h = 400;
  			var force = d3.layout.force()
                                 .nodes($scope.ids)
                                 .links(edges1)
                                 .size([w, h])
                                 .linkDistance([180])
                                 .charge([-250])
                                 .start();

            var colors = d3.scale.category10();

            //Create SVG element
            var svg = d3.select("svg")
                        .attr("width", w)
                        .attr("height", h);

var tip = d3.tip()
  .attr('class', 'd3-tip')
  .offset([-10, 0])
  .html(function(d) {
    return "<pre class='alert alert-success' style='max-width:400px;'>" + d.values + "</pre>";
  });
svg.call(tip);
            //Create edges as lines
            var edges = svg.selectAll("line")
                .data(edges1)
                .enter()
                .append("line")
                .style("stroke", "#23A410")
                .style("stroke-width", 3);
            
          var node = svg.selectAll(".node")
                .data($scope.ids)
                .enter().append("g")
                .attr("class", "node")
		
		.on("mouseover", tip.show)
                .on("mouseout", tip.hide) 
                .on("click", function(d){
			tip.hide();
			if(d.guid==undefined){
			
			}
else
{
location.href="#/Search/"+d.guid;
}
		

	})
                .call(force.drag);
          
	svg.append("svg:pattern").attr("id","processICO").attr("width",1).attr("height",1)
		.append("svg:image").attr("xlink:href","img/process.png").attr("x",-5.5).attr("y",-4).attr("width",41).attr("height",42);
           svg.append("svg:pattern").attr("id","textICO").attr("width",1).attr("height",1)
		.append("svg:image").attr("xlink:href","img/text.ico").attr("x",2).attr("y",2).attr("width",25).attr("height",25);

// define arrow markers for graph links
svg.append('svg:defs').append('svg:marker')
    .attr('id', 'end-arrow')
    .attr('viewBox', '0 -5 10 10')
    .attr('refX', 10)
    .attr('markerWidth', 5)
    .attr('markerHeight', 5)
    .attr('orient', 'auto')
  .append('svg:path')
    .attr('d', 'M0,-5L10,0L0,5')
    .attr('fill', '#7B7A7A');

svg.append('svg:defs').append('svg:marker')
    .attr('id', 'start-arrow')
    .attr('viewBox', '0 -5 10 10')
    .attr('refX', 4)
    .attr('markerWidth', 3)
    .attr('markerHeight', 3)
    .attr('orient', 'auto')
  .append('svg:path')
    .attr('d', 'M10,-5L0,0L10,5')
    .attr('fill', '#000');


// handles to link and node element groups
var path = svg.append('svg:g').selectAll('path')
    .data(force.links())
  .enter().append("svg:path")
    .attr("class", "link")
    .attr('marker-end','url(#end-arrow)');
        //Create nodes as circles
          //var nodes = svg.selectAll("circle")
          //.data(dataset.nodes)
          //    .enter()
          node.append("circle")
                .attr("r", function(d, i) {
			if(d.hasChild=="True"){
			return 15;
			}else{
			 return 15;
			}
                    return 10;
                })
		.attr("cursor","pointer")
                .style("fill", function(d, i) {
			if(d.hasChild=="True"){
			return "url('#processICO')";
			}else{
			 return "url('#textICO')";
			}
                    return colors(i);
                })
                .attr("class","circle");
            //.call(force.drag);
          
            //Add text
             node.append("text")
              .attr("x", 12)
              .attr("dy", ".35em")
              .text(function(d) { return d.Name;         });

            //Every time the simulation "ticks", this will be called
            force.on("tick", function() {

                edges.attr("x1", function(d) { return d.source.x; })
                     .attr("y1", function(d) { return d.source.y; })
                     .attr("x2", function(d) { return d.target.x; })
                     .attr("y2", function(d) { return d.target.y; });
            

path.attr('d', function(d) {
    var deltaX = d.target.x - d.source.x,
        deltaY = d.target.y - d.source.y,
        dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
        normX = deltaX / dist,
        normY = deltaY / dist,
        sourcePadding = d.left ? 17 : 12,
        targetPadding = d.right ? 17 : 12,
        sourceX = d.source.x + (sourcePadding * normX),
        sourceY = d.source.y + (sourcePadding * normY),
        targetX = d.target.x - (targetPadding * normX),
        targetY = d.target.y - (targetPadding * normY);
    return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
  });


              //node.attr("cx", function(d) { return d.x; })
              //.attr("cy", function(d) { return d.y; });
              node
                .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; }); 
			 })

function mouseover(d) {
  d3.select(this).select("circle").transition()
      .duration(750)
      .attr("r", 16);
}

function mouseout() {
  d3.select(this).select("circle").transition()
      .duration(750)
      .attr("r", 10);
}
   
 });    	
		}]
	);
