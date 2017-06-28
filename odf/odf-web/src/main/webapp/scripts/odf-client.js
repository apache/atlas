/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
require("bootstrap/dist/css/bootstrap.min.css");

var $ = require("jquery");
var bootstrap = require("bootstrap");

var React = require("react");
var ReactDOM = require("react-dom");
var LinkedStateMixin = require('react-addons-linked-state-mixin');
var ReactBootstrap = require("react-bootstrap");

var Nav = ReactBootstrap.Nav;
var NavItem = ReactBootstrap.NavItem;
var Navbar = ReactBootstrap.Navbar;
var NavDropdown = ReactBootstrap.NavDropdown;
var Button = ReactBootstrap.Button;
var Grid = ReactBootstrap.Grid;
var Row = ReactBootstrap.Row;
var Col = ReactBootstrap.Col;
var Table = ReactBootstrap.Table;
var Modal = ReactBootstrap.Modal;
var Alert = ReactBootstrap.Alert;
var Panel = ReactBootstrap.Panel;
var Label = ReactBootstrap.Label;
var Input = ReactBootstrap.Input;
var Jumbotron = ReactBootstrap.Jumbotron;
var Image = ReactBootstrap.Image;
var Dropdown = ReactBootstrap.Dropdown;
var DropdownButton = ReactBootstrap.DropdownButton;
var CustomMenu = ReactBootstrap.CustomMenu;
var MenuItem = ReactBootstrap.MenuItem;
var Tooltip = ReactBootstrap.Tooltip;
var OverlayTrigger = ReactBootstrap.OverlayTrigger;
var Glyphicon = ReactBootstrap.Glyphicon;

var ODFGlobals = require("./odf-globals.js");
var OdfAnalysisRequest = require("./odf-analysis-request.js");
var NewAnalysisRequestButton = OdfAnalysisRequest.NewCreateAnnotationsButton;
var ODFBrowser = require("./odf-metadata-browser.js");
var Utils = require("./odf-utils.js");
var AtlasHelper = Utils.AtlasHelper;
var AJAXCleanupMixin = require("./odf-mixins.js");
var UISpec = require("./odf-ui-spec.js");


var knownAnnotations = {
	"Default": [{value : "annotationType", style : "primary", label: "Unknown"}],
	"ColumnAnalysisColumnAnnotation" : [{value: "jsonProperties.inferredDataClass.className", style: "danger" , label: "Class name"}, {value: "jsonProperties.inferredDataType.type", style: "info", label :"Datatype"}],
	"DataQualityColumnAnnotation": [{style: "warning", value: "jsonProperties.qualityScore" , label: "Data quality score"}],
    "MatcherAnnotation": [{style: "success", value: "jsonProperties.termAssignments", label: "Matching terms"}]
};

////////////////////////////////////////////////////////////////
// toplevel navigation bar

const constants_ODFNavBar = {
  odfDataLakePage: "navKeyDataLakePage",
  odfTermPage: "navKeyTermPage"
}

var ODFNavBar = React.createClass({
   render: function() {
       return (
         <Navbar inverse>
           <Navbar.Header>
             <Navbar.Brand>
               <b>Shop for Data Application, powered by Open Discovery Framework</b>
             </Navbar.Brand>
             <Navbar.Toggle />
           </Navbar.Header>
           <Navbar.Collapse>
             <Nav pullRight activeKey={this.props.activeKey} onSelect={this.props.selectCallback}>
               <NavItem eventKey={constants_ODFNavBar.odfDataLakePage} href="#">Data Lake Browser</NavItem>
               <NavItem eventKey={constants_ODFNavBar.odfTermPage} href="#">Glossary</NavItem>
             </Nav>
           </Navbar.Collapse>
         </Navbar>
       );
   }
});

var ODFAnnotationLegend = React.createClass({

	render : function(){
		var items = [];
		$.each(knownAnnotations, function(key, val){
			$.each(val, function(key2, item){
				items.push(<Label key={key + "_" + key2} bsStyle={item.style}>{item.label}</Label>);
			});
		});

		return <div>{items}</div>;
	}

});

var ODFAnnotationMarker = React.createClass({

	render : function(){
		var annotationKey = "Default";
		var annotationLabels = [];
		if(this.props.annotation && knownAnnotations[this.props.annotation.annotationType]){
			annotationKey = this.props.annotation.annotationType;
			var tooltip = <Tooltip id={this.props.annotation.annotationType}>{this.props.annotation.annotationType}<br/>{this.props.annotation.summary}</Tooltip>
			$.each(knownAnnotations[annotationKey], function(key, val){
				var style = val.style;
				var value = ODFGlobals.getPathValue(this.props.annotation, val.value);
				if (annotationKey === "MatcherAnnotation") {
					value = value[0].matchingString; // if no abbreviation matches this will be the term; ideally it should be based on the OMBusinessTerm reference
				}
				else if(value && !isNaN(value)){
					value = Math.round(value*100) + " %";
				}
				annotationLabels.push(<OverlayTrigger key={key} placement="top" overlay={tooltip}><Label style={{margin: "5px"}} bsStyle={style}>{value}</Label></OverlayTrigger>);
			}.bind(this));
		}else{
			var tooltip = <Tooltip id={this.props.annotation.annotationType}>{this.props.annotation.annotationType}<br/>{this.props.annotation.summary}</Tooltip>
			annotationLabels.push(<OverlayTrigger key="unknownAnnotation" placement="top" overlay={tooltip}><Label style={{margin: "5px"}} bsStyle={knownAnnotations[annotationKey][0].style}>{this.props.annotation.annotationType}</Label></OverlayTrigger>);
		}

		return <div style={this.props.style}>{annotationLabels}</div>;
	}
});


var AnnotationsColumn = React.createClass({
	mixins : [AJAXCleanupMixin],

	getInitialState : function(){
		return {annotations: []};
	},

	componentDidMount : function() {
		if(this.props.annotations){
			this.setState({loadedAnnotations : this.props.annotations});
			return;
		}

		if(this.props.annotationReferences){
			this.loadColumnAnnotations(this.props.annotationReferences);
		}
	},

	componentWillReceiveProps : function(nextProps){
		if(!this.isMounted()){
			return;
		}

		if(nextProps.annotations){
			this.setState({loadedAnnotations : nextProps.annotations});
			return;
		}
	},

	render : function(){
		if(this.state){
			var annotations = this.state.loadedAnnotations;
			if(!annotations || annotations.length > 0 && annotations[0].repositoryId){
				return <noscript/>;
			}

			var processedTypes = [];
			var colAnnotations = [];
			$.each(annotations, function(key, val){
				if(processedTypes.indexOf(val.annotationType) == -1){
					processedTypes.push(val.annotationType);
					var style = {float: "left"};
					if(key % 6 == 0){
						style = {clear: "both"};
					}

					var summary = (val.summary ? val.summary : "");
					colAnnotations.push(<ODFAnnotationMarker style={style} key={key} annotation={val}/>);
				}
			});

			return <div>{colAnnotations}</div>;
		}
		return <noscript/>;
	}

});

var QualityScoreFilter = React.createClass({

	getInitialState : function(){
		return {key: "All", val : "0", showMenu : false};
	},

	onSelect : function(obj, key){

		if(obj.target.tagName != "INPUT"){
			this.setState({key: key});
			var equation = "All";
			if(key != "All"){
				if(this.refs.numberInput.getValue().trim() == ""){
					return;
				}
				equation = key + this.refs.numberInput.getValue();
			}
			this.props.onFilter(equation);
		}
	},

	textChange : function(event){
		var equation = "All";
		if(this.state.key != "All"){
			if(this.refs.numberInput.getValue().trim() == ""){
				return;
			}
			equation = this.state.key + this.refs.numberInput.getValue();
		}
		this.props.onFilter(equation);
	},

	render : function(){
		var items = [];
		var values = ["<", "<=", "==", ">=", ">", "!=", "All"];
		$.each(values, function(key, val){
			items.push(<MenuItem onSelect={this.onSelect} id={val} key={key} eventKey={val}>{val}</MenuItem>)
		}.bind(this));

		var menu = <div bsRole="menu" className={"dropdown-menu"}>
			<h5 style={{float: "left", marginLeft: "15px"}}><Label ref="typeLabel">{this.state.key}</Label></h5>
			<Input style={{width: "100px"}} ref="numberInput" onChange={this.textChange} type="number" defaultValue="1"/>
			{items}
		</div>;

		return <div style={this.props.style}  >
			<Dropdown id="quality score select" onSelect={this.onSelect} open={this.state.showMenu} onToggle={function(){}}>
				<Button bsRole="toggle" onClick={function(){this.setState({showMenu: !this.state.showMenu})}.bind(this)}>Qualityscore filter</Button>
				{menu}
			</Dropdown>
		</div>;
	}
});

var DataClassFilter = React.createClass({

	defaultClasses : ["US Zip", "Credit Card"],

	render : function(){
		var items = [];
		var classes = (this.props.dataClasses ? this.props.dataClasses.slice() : this.defaultClasses);
		classes.push("All");
		$.each(classes, function(key, val){
			items.push(<MenuItem id={val} key={key} eventKey={val}>{val}</MenuItem>)
		});

		return <div style={this.props.style}>
			<DropdownButton id="Data class filter" onSelect={function(obj, key){this.props.onFilter(key)}.bind(this)} title="Data Class filter">
				{items}
			</DropdownButton>
		</div>;
	}
});


var FilterMenu = React.createClass({

	getInitialState : function(){
		return {showMenu : false, dataClassFilter: "All", qualityScoreFilter: "All"};
	},

	onQualityScoreFilter: function(param){
		this.setState({qualityScoreFilter: param});
		if(this.props.onFilter){
			this.props.onFilter({dataClassFilter: this.state.dataClassFilter, qualityScoreFilter: param});
		}
	},

	onDataClassFilter : function(param){
		this.setState({dataClassFilter: param});
		if(this.props.onFilter){
			this.props.onFilter({dataClassFilter: param, qualityScoreFilter: this.state.qualityScoreFilter});
		}
	},

	render : function(){
		var menu = <div bsRole="menu" className={"dropdown-menu"}>
			<QualityScoreFilter onFilter={this.onQualityScoreFilter}/>
			<br />
			<DataClassFilter dataClasses={this.props.dataClasses} onFilter={this.onDataClassFilter}  />
		</div>;

		return <div style={this.props.style}  >
			<Dropdown id="filter menu" open={this.state.showMenu} onToggle={function(){}}>
				<Button bsRole="toggle" onClick={function(){this.setState({showMenu: !this.state.showMenu})}.bind(this)}>Filter annotations</Button>
				{menu}
			</Dropdown>
		</div>;
	}

});


var SelectCheckbox = React.createClass({

	getInitialState : function(){
		return {selected : this.props.asset.isSelected};
	},

	componentWillReceiveProps : function(nextProps){
		if(!this.isMounted()){
			return;
		}
		if(nextProps.asset.reference.id != this.props.asset.reference.id){
			this.setState({selected : nextProps.asset.isSelected});
		}
	},

	onChange : function(selected){
		if(this.props.onChange){
			this.props.onChange(selected);
		}
		this.setState({selected : selected});
	},

	render : function(){
		return <div><Input style={{marginTop: "-6px"}} type="checkbox" label=" " checked={this.state.selected} onChange={function(e){
			this.onChange($(e.target).prop("checked"));
		}.bind(this)}/></div>;
	}

});

var ODFDataLakePage = React.createClass({

	columnAnnotations : {},

	getInitialState : function(){
		return {
			ajaxAborts : [],
			sourceLoading: false,
			columns: [],
			dataClasses: [],
			qualityScoreFilter: "All",
			dataClassFilter: "All",
			importFeedback: {msg: null, style: "primary"}
		};
	},

	componentDidMount : function() {
		this.loadSources();
	},

	loadSources : function(){
		this.searchAtlasMetadata("from RelationalDataSet", function(data){
			 $.each(data, function(key, source){
				 source.isSelected = false;
			  });
			this.setState({filteredSources: data, sources: data});
		}.bind(this));
	},

	searchAtlasMetadata : function(query, successCallback, errorCallback) {
		var url = ODFGlobals.metadataUrl + "/search?" + $.param({query: query});
		$.ajax({
			url: url,
			dataType: 'json',
			type: 'GET',
			success: function(data) {
				successCallback(data);
			},
			error: function(xhr, status, err) {
				console.error(url, status, err.toString());
				var msg = "Error while loading metadata: " + err.toString();
				if(errorCallback){
					errorCallback(msg);
				}
			}
		});
	 },

	load : function(assetRef){
		$.each(this.state.ajaxAborts, function(key, abort){
			if(abort && abort.call){
				abort.call();
			}
		});
		this.setState({ajaxAborts : []});

		var req = AtlasHelper.loadAtlasAsset(assetRef, function(data){
			var source = data;
			var refresh = false;
			if(this.state == null || this.state.selectedTable == null || this.state.selectedTable.reference.id != source.reference.id){
				console.log("set state source " + new Date().toLocaleString());
				this.setState({selectedTable: source});
				if(source.annotations == null){
					source.annotations = [];
				}
				if(source.columns == null){
					source.columns = [];
				}
			}else{
				source.annotations = this.state.selectedTable.annotations;
				refresh = true;
			}

			this.loadSourceAnnotations(source, refresh);
			this.loadColumns(source, refresh);
		}.bind(this), function(){

		});
	},

	loadSourceAnnotations : function(source, refresh){
		if(!refresh || !source.loadedAnnotations){
			source.loadedAnnotations = [];
		}
        var reqs = AtlasHelper.loadMostRecentAnnotations(source.reference, function(annotationList){
            if (refresh) {
            	var newAnnotations = [];
            	if(source.loadedAnnotations.length > 0){
            		$.each(annotationList, function(key, val){
            			if(!this.atlasAssetArrayContains(source.loadedAnnotations, val)){
            				newAnnotations.push(val);
            			}
            		}.bind(this));
            	}else{
            		newAnnotations = annotationList;
            	}
                source.loadedAnnotations = newAnnotations;
            }else{
            	source.loadedAnnotations = annotationList;
            }
            console.log("set state source anns " + new Date().toLocaleString());
            this.setState({selectedTable: source});
        }.bind(this), function(){

        });

        var ajaxAborts = [];
		$.each(reqs, function(key, req){
			ajaxAborts.push(req.abort);
		}.bind(this))
		this.setState({ajaxAborts : ajaxAborts});
	},

	atlasAssetArrayContains : function(array, obj){
		for(var no = 0; no < array.length; no++){
			var val = array[no];
			if(val && val.reference && obj && obj.reference && val.reference.id == obj.reference.id){
				return true;
			}
		}
		return false;
	},

	loadColumns : function(dataSet, refresh){
		var columns = [];
		if(refresh){
			columns = this.state.columns;
		}
		var reqs = AtlasHelper.loadRelationalDataSet(dataSet, function(result){
			var foundAnnotations = false;
			if(!refresh){
				$.each(result, function(key, col){
					if(col.annotations && col.annotations.length > 0){
						foundAnnotations = true;
					}
					if(col.isSelected == null || col.isSelected == undefined){
						col.isSelected = false;
					}
					columns.push(col);
				});
			}else{
				//if result size is different, reset completely
				if(result.length != columns.length){
					columns = [];
				}
				//if the old array contains any column that is not in the new columns, reset completely
				$.each(columns, function(key, col){
					if(!this.atlasAssetArrayContains(result, col)){
						columns = [];
					}
				}.bind(this));
				$.each(result, function(key, col){
					//only add new columns
					if(!this.atlasAssetArrayContains(columns, col)){
						columns.push(col);
					}
					if(col.annotations && col.annotations.length > 0){
						for(var no = 0; no < columns.length; no++){
							if(columns[no] == null || columns[no] == undefined){
								col.isSelected = false;
							}
							if(columns[no].reference.id == col.reference.id){
								columns[no].annotations = col.annotations;
								break;
							}
						}
						foundAnnotations = true;
					}
				}.bind(this));
			}

			if(!foundAnnotations){
				if(!Utils.arraysEqual(this.state.columns, columns)){
					console.log("set state columns " + new Date().toLocaleString());
					this.setState({currentlyLoading : false, columns: columns, filteredColumns: columns});
				}else{
					console.log("columns same, no annotations, dont update");
				}
			}else{
				this.loadColumnAnnotations(columns, refresh);
			}
		}.bind(this), function(){

		});

        var ajaxAborts = [];
		$.each(reqs, function(key, req){
			ajaxAborts.push(req.abort);
		}.bind(this))
		this.setState({ajaxAborts : ajaxAborts});
	},

	loadColumnAnnotations : function(columns, refresh){
		var annotationRefs = [];
		$.each(columns, function(key, col){
			if(!refresh || !col.loadedAnnotations){
				col.loadedAnnotations = [];
			}
		});

		var requests = [];
		var annotationsChanged = false;
		var dataClasses = [];
		$.each(columns, function(key, column){
			var req = AtlasHelper.loadMostRecentAnnotations(column.reference, function(annotations){
				$.each(annotations, function(key, annotation){
					if(!this.atlasAssetArrayContains(column.loadedAnnotations, annotation)){
						annotationsChanged = true;
						column.loadedAnnotations.push(annotation);
					}
					if(annotation &&
							annotation.inferredDataClass && dataClasses.indexOf(annotation.inferredDataClass.className) == -1){
						dataClasses.push(annotation.inferredDataClass.className);
					}
				}.bind(this));
			}.bind(this));
			requests.push(req);
		}.bind(this));

		$.when.apply(undefined, requests).done(function(){
			if(annotationsChanged){
				console.log("set state column anns " + new Date().toLocaleString());
				this.setState({currentlyLoading : false, columns: columns, filteredColumns: columns, dataClasses: dataClasses});
			}else{
				if(!Utils.arraysEqual(this.state.columns, columns)){
					console.log("set state column anns " + new Date().toLocaleString());
					this.setState({currentlyLoading : false, columns: columns, filteredColumns: columns});
				}else{
					console.log("columns same, annotations same, dont update");
				}
			}
		}.bind(this));

        var ajaxAborts = [];
		$.each(requests, function(key, req){
			ajaxAborts.push(req.abort);
		}.bind(this));
		this.setState({ajaxAborts : ajaxAborts});
	},

	storeColumnAnnotation : function(columnId, annotation){
		if(!this.columnAnnotations[columnId]){
			this.columnAnnotations[columnId] = [];
		}
		if(!this.atlasAssetArrayContains(this.columnAnnotations[columnId], annotation)){
			this.columnAnnotations[columnId].push(annotation);
		}
	},

	componentWillUnmount : function() {
		if(this.refreshInterval){
			clearInterval(this.refreshInterval);
		}
   	},

	referenceClick : function(asset){
		if(this.state == null || this.state.selectedTable == null || this.state.selectedTable.reference.id != asset.reference.id){
			if(this.refreshInterval){
				clearInterval(this.refreshInterval);
			}
			this.setState({currentlyLoading : true, selectedTable: null, filteredColumns : [], columns: []});
			this.load(asset.reference);
			this.refreshInterval = setInterval(function(){this.load(asset.reference)}.bind(this), 15000);
		}
	},

	doFilter : function(params){
		var columns = this.state.columns.slice();
		var filteredColumns = this.filterOnDataQualityScore(columns, params.qualityScoreFilter);
		filteredColumns = this.filterOnDataClass(filteredColumns, params.dataClassFilter);
		this.setState({filteredColumns: filteredColumns});
	},

	filterOnDataQualityScore : function(columns, equation){
		if(equation.indexOf("All")>-1){
			return columns;
		}

		var columns = columns.slice();
		var matchedColumns = [];
		$.each(columns, function(index, col){
			var match = false;
			$.each(col.loadedAnnotations, function(k, annotation){
				if(equation && annotation.qualityScore){
						if(eval("annotation.qualityScore" + equation)){
							if(matchedColumns.indexOf(col) == -1){
								matchedColumns.push(col);
							}
						}
				}
			}.bind(this));
		}.bind(this));

		return matchedColumns;
	},

	filterOnDataClass : function(columns, key){
		if(key == "All"){
			return columns;
		}
		var matchedColumns = [];
		$.each(columns, function(index, col){
			var match = false;
			$.each(col.loadedAnnotations, function(k, annotation){
				if(annotation.inferredDataClass &&
						annotation.inferredDataClass.className == key){
					if(matchedColumns.indexOf(col) == -1){
						matchedColumns.push(col);
					}
				}
			});
		});

		return matchedColumns;
	},

	doImport : function(){
		var params = {
				jdbcString : this.refs.jdbcInput.getValue(),
				user: this.refs.userInput.getValue(),
				password : this.refs.passInput.getValue(),
				database :this.refs.dbInput.getValue(),
				schema : this.refs.schemaInput.getValue(),
				table : this.refs.sourceInput.getValue()
		};

		this.setState({importingTable : true, tableWasImported : true, });

		$.ajax({
		      url: ODFGlobals.importUrl,
		      contentType: "application/json",
		      dataType: 'json',
		      type: 'POST',
		      data: JSON.stringify(params),
		      success: function(data) {
		    	  this.setState({importFeedback: {msg: "Registration successful!", style: "primary"}, importingTable: false});
		      }.bind(this),
		      error: function(xhr, status, err) {
				  if(this.isMounted()){
					var errorMsg = status;
					if(xhr.responseJSON && xhr.responseJSON.error){
		    				errorMsg = xhr.responseJSON.error;
		    		  	}
				    	var msg = "Table could not be registered: " + errorMsg + ", " + err.toString();
			    	  	this.setState({importFeedback: {msg: msg, style: "warning"}, importingTable: false});
				  }
		      }.bind(this)
		    });
	},

	closeImportingDialog : function(){
		if(this.state.importingTable){
			return;
		}

		var newState = {tableWasImported: false, showImportDialog : false, importFeedback: {msg: null}};
		if(this.state.tableWasImported){
			this.loadSources();
			newState.sources = null;
			newState.filteredSources = null;
		}
		this.setState(newState);
	},

	shopData : function(){
		var selectedColumns = [];
		var selectedSources = [];

		$.each(this.state.columns, function(key, col){
			if(col.isSelected){
				selectedColumns.push(col);
			}
		});

		$.each(this.state.sources, function(key, src){
			if(src.isSelected){
				selectedSources.push(src);
			}
		});

		console.log("Do something with the selected columns!")
		console.log(selectedColumns);
		console.log(selectedSources);
	},

	filterSources : function(e){
		var value = $(e.target).val();
		var filtered = [];
		if(value.trim() == ""){
			filtered = this.state.sources;
		}else{
			$.each(this.state.sources, function(key, source){
				if(source.name.toUpperCase().indexOf(value.toUpperCase()) > -1){
					filtered.push(source);
				}
			});
		}
		this.setState({filteredSources : filtered});
	},

	storeImportDialogDefaults: function() {
		var defaultValues = {
		   "jdbcInput": this.refs.jdbcInput.getValue(),
		   "userInput": this.refs.userInput.getValue(),
		   "passInput": this.refs.passInput.getValue(),
		   "dbInput": this.refs.dbInput.getValue(),
		   "schemaInput": this.refs.schemaInput.getValue(),
		   "sourceInput": this.refs.sourceInput.getValue(),
		};
		localStorage.setItem("odf-client-defaults", JSON.stringify(defaultValues) );
	},

	render : function(){
		var columnRows = [];
		var sourceHead = null;
		var sourceList = null;
		var columnsGridHeader = <thead><tr><th>Column</th><th>Datatype</th><th>Annotations</th></tr></thead>;
		var currentlyLoadingImg = null;
		if(this.state){
			var sourceListContent = null;
			if(this.state.sources){
				var sourceSpec =  {

						attributes: [
						       {key: "isSelected", label: "",
								func: function(val, asset){
									return <SelectCheckbox onChange={function(selected){
										asset.isSelected = selected;
									}.bind(this)} asset={asset} />

								}},
								{key: "icon", label: "", func:
						    	   function(val, asset){
							    	   if(asset && asset.type && UISpec[asset.type] && UISpec[asset.type].icon){
							    		   return UISpec[asset.type].icon;
							    	   }
							    	   return UISpec["DefaultDocument"].icon;
						       		}
						       },
							   {key: "name", label: "Name"},
			                   {key: "type", label: "Type"},
			                   {key: "annotations", label: "Annotations",
					        	  func: function(val){
					        		  if(!val){
					        			  return 0;
					        			  }
					        		  return val.length;
					        		}
			                   }
			            ]};

				sourceListContent = <ODFBrowser.ODFPagingTable rowAssets={this.state.filteredSources} onRowClick={this.referenceClick} spec={sourceSpec}/>;
			}else{
				sourceListContent = <Image src="img/lg_proc.gif" rounded />;
			}

			var sourceImportBtn = <Button style={{float:"right"}} onClick={function(){this.setState({showImportDialog: true});}.bind(this)}>Register new data set</Button>;
			var sourceImportingImg = null;
			if(this.state.importingTable){
				sourceImportingImg = <Image src="img/lg_proc.gif" rounded />;
			}

			var importFeedback = <h3><Label style={{whiteSpace: "normal"}} bsStyle={this.state.importFeedback.style}>{this.state.importFeedback.msg}</Label></h3>

			var storedDefaults = null;
			try {
			   storedDefaults = JSON.parse(localStorage.getItem("odf-client-defaults"));
			} catch(e) {
				console.log("Couldnt parse defaults from localStorage: " + e);
				storedDefaults = {};
			}
			if (!storedDefaults) {
				storedDefaults = {};
			}
			console.log("Stored defaults: " + storedDefaults);

			var sourceImportDialog =  <Modal show={this.state.showImportDialog} onHide={this.closeImportingDialog}>
								          <Modal.Header closeButton>
								             <Modal.Title>Register new JDBC data set</Modal.Title>
								          </Modal.Header>
								          <Modal.Body>
								          	{importFeedback}
								            <form>
								          	 <Input type="text" ref="jdbcInput" defaultValue={storedDefaults.jdbcInput} label="JDBC string" />
								             <Input type="text" ref="userInput" defaultValue={storedDefaults.userInput} label="Username" />
								             <Input type="password" ref="passInput" defaultValue={storedDefaults.passInput} label="Password" />
								             <Input type="text" ref="dbInput" defaultValue={storedDefaults.dbInput} label="Database" />
								             <Input type="text" ref="schemaInput" defaultValue={storedDefaults.schemaInput} label="Schema" />
								             <Input type="text" ref="sourceInput" defaultValue={storedDefaults.sourceInput} label="Table" />
								             </form>
								             {sourceImportingImg}
								         </Modal.Body>
								         <Modal.Footer>
								         <Button onClick={this.storeImportDialogDefaults}>Store values as defaults</Button>
								         <Button bsStyle="primary" onClick={this.doImport}>Register</Button>
								         <Button onClick={this.closeImportingDialog}>Close</Button>
								         </Modal.Footer>
									</Modal>;
			sourceList = <Panel style={{float:"left", marginRight: 30, maxWidth:600, minHeight: 550}}>
								{sourceImportDialog}
								<h3 style={{float: "left", marginTop: "5px"}}>
									Data sets
								</h3>
								{sourceImportBtn}<br style={{clear: "both"}}/>
								<Input onChange={this.filterSources} addonBefore={<Glyphicon glyph="search" />} label=" " type="text" placeholder="Filter ..." />
								<br/>
								{sourceListContent}
							</Panel>;
			if(this.state.currentlyLoading){
				currentlyLoadingImg = <Image src="img/lg_proc.gif" rounded />;
			}
			var panel = <div style={{float: "left"}}>{currentlyLoadingImg}</div>;

			if(this.state.selectedTable){
				var source = this.state.selectedTable;
				var sourceAnnotations = [];
				if(source.loadedAnnotations){
					//reverse so newest is at front
					var sourceAnns = source.loadedAnnotations.slice();
					sourceAnns.reverse();
					var processedTypes = [];
					$.each(sourceAnns, function(key, val){
						if(processedTypes.indexOf(val.annotationType) == -1){
							processedTypes.push(val.annotationType);
							var summary = (val.summary ? ", " + val.summary : "");
							sourceAnnotations.push(<ODFAnnotationMarker key={key} annotation={val}/>);
						}
					});
				}

				var hasColumns = (source.columns && source.columns.length > 0 ? true : false);
				var columnsString = (hasColumns ? "Columns: " + source.columns.length : null);
				var annotationsFilter = (hasColumns ? <FilterMenu onFilter={this.doFilter} dataClasses={this.state.dataClasses} style={{float: "right"}} /> : null);

				sourceHead = <div>
								<h3>{source.name} </h3>
									<div style={{}}>
										<NewAnalysisRequestButton dataSetId={this.state.selectedTable.reference.id} />
									</div>
								<br/>
								Description: {source.description}
								<br/>
								{columnsString}
								<br/>Annotations:{sourceAnnotations}
								<br/>
								{annotationsFilter}
								</div>;

				panel = <Panel style={{float: "left", width: "50%"}} header={sourceHead}>
							{currentlyLoadingImg}
						</Panel>;
			}
			var columnsTable = null;
			var filteredColumns = (this.state.filteredColumns ? this.state.filteredColumns : []).slice();

			if(filteredColumns.length > 0){
				var colSpec = {attributes: [{key: "isSelected", label: "Select",
					func: function(val, col){
						return <SelectCheckbox onChange={function(selected){
							col.isSelected = selected;
						}.bind(this)} asset={col} />

					}},
	               {key: "name", label: "Name", sort: true},
		           {key: "dataType", label: "Datatype"},
		           {key: "loadedAnnotations", label: "Annotations",
			        	  func: function(annotations, obj){
			        		  return <AnnotationsColumn annotations={annotations} />;
			        	  }
			          }]};
				columnsTable = <div><ODFBrowser.ODFPagingTable ref="columnsTable" rowAssets={filteredColumns} assetType={"columns"} spec={colSpec}/><br/><ODFAnnotationLegend /></div>;
				panel = (<Panel style={{float:"left", width: "50%"}} header={sourceHead}>
							{columnsTable}
						</Panel>);
			}
		}

		var contentComponent = <Jumbotron>
	      <div>
	         <h2>Welcome to your Data Lake</h2>
	         	<Button bsStyle="success" onClick={this.shopData}>
	         		Shop selected data  <Glyphicon glyph="shopping-cart" />
         		</Button>
	         	<br/>
	         	<br/>
		         {sourceList}
		         {panel}
		        <div style={{clear: "both"}} />
         </div>
       </Jumbotron>;

		return <div>{contentComponent}</div>;
	}
});

var ODFTermPage = React.createClass({

  getInitialState() {
    return {terms: []};
  },

  loadTerms : function() {
    // clear alert
    this.props.alertCallback({type: "", message: ""});
    var req = AtlasHelper.searchAtlasMetadata("from BusinessTerm",

        function(data){
		   	if(!this.isMounted()){
				return;
			}
			this.setState({terms: data});
        }.bind(this),

        function() {
        }.bind(this)
    );
  },

  componentDidMount() {
    this.loadTerms();
  },

  render: function() {
     var terms = $.map(
        this.state.terms,
        function(term) {
          return <tr style={{cursor: 'pointer'}} key={term.name} title={term.example} onClick={function(){
        	  var win = window.open(term.originRef, '_blank');
        	  win.focus();}
          }>
                  <td>
                     {term.name}
                  </td>
                  <td>
                	{term.description}
                  </td>
                 </tr>
        }.bind(this)
       );

     return (
       <div className="jumbotron">
       <h2>Glossary</h2>
       <br/>
       <br/>
       <Panel>
       	  <h3>Terms</h3>
          <Table>
          	 <thead>
          	 	<tr>
          	 	<th>Name</th>
          	 	<th>Description</th>
          	 	</tr>
          	 </thead>
             <tbody>
                {terms}
             </tbody>
          </Table>
          </Panel>
       </div>
     )
   }
});

var ODFClient = React.createClass({

   componentDidMount: function() {
     $(window).bind("hashchange", this.parseUrl);
     this.parseUrl();
   },

   parseUrl : function(){
    var target = constants_ODFNavBar.odfDataLakePage;
    var navAddition = null;
    var hash = document.location.hash;
    if(hash && hash.length > 1){
      hash = hash.split("#")[1];
      var split = hash.split("/");
      var navHash = split[0];
      if(split.length > 0){
        navAddition = split.slice(1);
      }
      if(constants_ODFNavBar[navHash]){
        target = constants_ODFNavBar[navHash];
      }
    }
    this.setState({
      activeNavBarItem: target,
        navAddition: navAddition}
    );
  },

  getInitialState: function() {
    return ({
        activeNavBarItem: constants_ODFNavBar.odfDataLakePage,
        navAddition: null,
        globalAlert: {
          type: "",
          message: ""
        }
    });
  },

  handleNavBarSelection: function(selection) {
    $.each(constants_ODFNavBar, function(key, ref){
      if(ref == selection){
        document.location.hash = key;
      }
    });
    this.setState({ activeNavBarItem: selection });
  },

  handleAlert: function(alertInfo) {
    this.setState({ globalAlert: alertInfo });
  },

  render: function() {
    var alertComp = null;
    if (this.state.globalAlert.type != "") {
       alertComp = <Alert bsStyle={this.state.globalAlert.type}>{this.state.globalAlert.message}</Alert>;
    }

    var contentComponent = <ODFDataLakePage alertCallback={this.handleAlert}/>;
    if (this.state.activeNavBarItem == constants_ODFNavBar.odfDataLakePage) {
       contentComponent = <ODFDataLakePage alertCallback={this.handleAlert}/>;
    } else if (this.state.activeNavBarItem == constants_ODFNavBar.odfTermPage) {
       contentComponent = <ODFTermPage alertCallback={this.handleAlert}/>;
    }

    var divStyle = {
//      marginLeft: "80px",
//      marginRight: "80px"
    };

    return (
        <div>
           <ODFNavBar activeKey={this.state.activeNavBarItem} selectCallback={this.handleNavBarSelection}></ODFNavBar>
           <div style={divStyle}>
              {alertComp}
              {contentComponent}
           </div>
        </div>
    );
  }
});

var div = $("#odf-toplevel-div")[0];
ReactDOM.render(<ODFClient/>, div);
