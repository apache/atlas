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
var $ = require("jquery");
var React = require("react");
var ReactBootstrap = require("react-bootstrap");

var Panel = ReactBootstrap.Panel;
var Table = ReactBootstrap.Table;
var Label = ReactBootstrap.Label;
var Image = ReactBootstrap.Image;
var Modal = ReactBootstrap.Modal;
var Button = ReactBootstrap.Button;
var FormControls = ReactBootstrap.FormControls;
var ListGroup = ReactBootstrap.ListGroup;
var ListGroupItem = ReactBootstrap.ListGroupItem;

var ODFGlobals = require("./odf-globals.js");
var UISpec = require("./odf-ui-spec.js");
var AJAXCleanupMixin = require("./odf-mixins.js");
var Utils = require("./odf-utils.js")
var AtlasHelper = Utils.AtlasHelper;
var URLHelper = Utils.URLHelper;

var ODFBrowser = {

	//set rowReferences property and pass an array of atlas references {id : ..., repositoryId: ...}, these rows will then be fetched
	//or set rowAssets property and pass an array of data that is supposed to be displayed as is
	ODFPagingTable : React.createClass({

		mixins : [AJAXCleanupMixin],

		getInitialState : function(){
			var pageSize = (this.props.pageSize ? this.props.pageSize : 5);
			var rowReferences = this.props.rowReferences;
			var rowAssets = this.props.rowAssets;
			var max = (rowReferences ? rowReferences.length : (rowAssets ? rowAssets.length : 0));
			var pageRows = (rowAssets ? rowAssets.slice(0, pageSize) : null);

			return {
				pageRows : pageRows,
				max : 0, tablePage : 0,
				pageSize : pageSize,
				max: max,
				tablePage: 0,
				rowReferenceLoadingAborts : []
			};
		},

		componentDidMount : function() {
			if(this.props.rowReferences){
				var pagerowReferences = this.props.rowReferences.slice(0, this.state.pageSize);
				this.loadRows(pagerowReferences);
			}
		},

		componentWillReceiveProps : function(nextProps){
			if(!this.isMounted()){
				return;
			}
			this.setStateFromProps(nextProps);
		},

		setStateFromProps : function(nextProps){
			if(nextProps.rowReferences && !Utils.arraysEqual(this.props.rowReferences, nextProps.rowReferences)){
				this.setState({max: nextProps.rowReferences.length, tablePage: 0});
				var pagerowReferences = nextProps.rowReferences.slice(0, this.state.pageSize);
				this.loadRows(pagerowReferences);
			}else if(nextProps.rowAssets && !Utils.arraysEqual(this.props.rowAssets, nextProps.rowAssets)){
				var rows = nextProps.rowAssets.slice(0, this.state.pageSize);
				this.setState({pageRows : rows, max: nextProps.rowAssets.length, tablePage: 0});
			}
		},

		getType : function(){
			if(this.props.assetType){
				return this.props.assetType;
			}else if(this.state.pageRows && this.state.pageRows.length > 0 && this.state.pageRows[0].type){
				return this.state.pageRows[0].type;
			}
		},

		getUISpec : function(){
			if(this.props.spec){
				return this.props.spec;
			}
			return UISpec[this.getType()];
		},

		loadRows : function(rowReferences){
			$.each(this.state.rowReferenceLoadingAborts, function(key, abort){
				if(abort && abort.call){
					abort.call();
				}
			});

			this.setState({pageRows: [], rowReferenceLoadingAborts: []});

			var reqs = AtlasHelper.loadAtlasAssets(rowReferences,
				function(rowAsset){
					var rowData = this.state.pageRows;
					rowData.push(rowAsset);
					if(this.isMounted()){
						this.setState({pageRows : rowData});
					}
					if(rowReferences && rowData && rowData.length == rowReferences.length && this.props.onLoad){
						this.props.onLoad(rowData);
					}
				}.bind(this),
				function(err){

				}
			);
			var aborts = [];
			$.each(reqs, function(key, val){
				var aborts = this.state.rowReferenceLoadingAborts;
				aborts.push(val.abort);
				this.setState({rowReferenceLoadingAborts: aborts});
			}.bind(this));

			this.storeAbort(aborts);
		},

		previousPage : function(){
			if(this.state.tablePage > -1){
				var tablePage = this.state.tablePage - 1;
				this.setState({tablePage : tablePage});
				if(this.props.rowAssets){
					var rows = this.props.rowAssets.slice(tablePage * this.state.pageSize, (tablePage + 1) * this.state.pageSize)
					this.setState({pageRows : rows});
				}else if(this.props.rowReferences){
					var rowRefs = this.props.rowReferences.slice(tablePage * this.state.pageSize, (tablePage + 1) * this.state.pageSize)
					this.loadRows(rowRefs);
				}
			}
		},

		nextPage : function(){
			var max = this.state.max;
			if((this.state.tablePage * this.state.pageSize) < max){
				var tablePage = this.state.tablePage + 1;
				this.setState({tablePage : tablePage});
				if(this.props.rowAssets){
					var rows = this.props.rowAssets.slice(tablePage * this.state.pageSize, (tablePage + 1) * this.state.pageSize)
					this.setState({pageRows : rows})
				}else if(this.props.rowReferences){
					var rows = this.props.rowReferences.slice(tablePage * this.state.pageSize, (tablePage + 1) * this.state.pageSize)
					this.loadRows(rows);
				}
			}
		},

		sortAlphabetical : function(a, b){
			if(this.getUISpec() && this.getUISpec().attributes){
				var attrs = this.getUISpec().attributes;
				var sortProp = null;
				for(var no = 0; no < attrs.length; no++){
					if(attrs[no].sort == true){
						sortProp = attrs[no].key;
						var aProp = a[sortProp].toLowerCase();
						var bProp = b[sortProp].toLowerCase();
						return ((aProp < bProp) ? -1 : ((aProp > bProp) ? 1 : 0));
					}
				}
			}
			return 0;
		},

		onRowClick : function(rowData){
			if(this.props.onRowClick){
				var type = this.getType();
				if(type){
					//new type is singular of list type ...
					type = type.substring(0, type.length - 1);
				}
				this.props.onRowClick(rowData, type);
			}
		},

		parseValue : function(value){
		    if (value) {
		        if(Array.isArray(value)){
		            return value.length;
		        }else if(typeof value === "object" && value.id && value.url && value.repositoryId){
		            return <a href={value.url}>{value.id}</a>;
		        }else if(typeof value === "object"){
		            return JSON.stringify(value);
		        }
		    }
			return value;
		},

		render : function(){
			var loadingImg = <Image src="img/lg_proc.gif" rounded />;
			var contentRows = [];
			var pageIndicator = "(0-0)";
			if(this.state.pageRows){
				loadingImg = null;
				this.state.pageRows.sort(this.sortAlphabetical);
				$.each(this.state.pageRows, function(key, rowData){
					var displayProperties = [];
					var icon = null;
					if(this.getUISpec()){
						displayProperties = this.getUISpec().attributes;
						if(this.getUISpec().icon){
							icon = this.getUISpec().icon;
						}
					}else{
						$.each(rowData, function(propName, val){
							var label = propName;
							if(label && label[0]){
								label = label[0].toUpperCase() + label.slice(1);
							}
							displayProperties.push({key: propName, label: label});
						});
					}

					var colCss = {};
					if(this.props.actions){
						colCss.paddingTop = "26px";
					}
					var columns = [<td style={colCss} key={"iconCol" + key}>{icon}</td>];
					$.each(displayProperties,
						function(key, propObj){
							//properties can be a path such as prop1.prop2
					        var value = ODFGlobals.getPathValue(rowData, propObj.key);

							if(propObj.func){
								value = propObj.func(value, rowData);
							}else{
								value = this.parseValue(value);
							}

							var col = <td style={colCss} key={propObj.key}>{value}</td>;
							columns.push(col);
						}.bind(this)
					);

					if(this.props.actions){
						var btns = [];
						$.each(this.props.actions, function(key, obj){
							if(obj.assetType.indexOf(this.getType()) > -1){
									$.each(obj.actions, function(actKey, action){
										if((action.filter && action.filter(rowData)) || !action.filter){
											var btn = <div key={actKey}><Button onClick={function(e){e.stopPropagation(); action.func(rowData);}}>{action.label}</Button><br/></div>;
											btns.push(btn);
										}
									});
							}
						}.bind(this));
						columns.push(<td key={"actionBtns"}>{btns}</td>);
					}

					var rowCss = {};
					if(this.props.onRowClick){
						rowCss.cursor = "pointer";
					}

					var row = <tr style={rowCss} onClick={function(){this.onRowClick(rowData);}.bind(this)} key={key}>
								{columns}
							  </tr>;
					contentRows.push(row);
				}.bind(this));

				var max = this.state.max;
				var min = (max > 0 ? (this.state.tablePage * this.state.pageSize + 1)  : 0);
				pageIndicator = "(" + min + "-";
				if((this.state.tablePage + 1) * this.state.pageSize >= max){
					pageIndicator += max + ")";
				}else{
					pageIndicator += (this.state.tablePage + 1) * this.state.pageSize + ")";
				}
			}

			var header = [];
			var lbls = [""];

			if(this.getUISpec()){
				$.each(this.getUISpec().attributes, function(key, propObj){
					lbls.push(propObj.label);
				});
			}else if(this.state.pageRows && this.state.pageRows.length > 0){
				$.each(this.state.pageRows[0], function(key, val){
					lbls.push(key[0].toUpperCase() + key.slice(1));
				});
			}
			if(this.props.actions){
				lbls.push("Actions");
			}

			$.each(lbls, function(key, val){
					var headerCss = null;
					if(val == "Actions"){
						headerCss = {paddingLeft: "38px"};
					}
					var th = <th style={headerCss} key={key}>{val}</th>;
					header.push(th);
				});

			return <div style={this.props.style}>
					<div style={{minHeight:250}}>
					<Table responsive>
						<thead>
							<tr>
								{header}
							</tr>
						</thead>
						<tbody>
							{contentRows}
						</tbody>
					</Table>
					</div>
					<Button disabled={(this.state.pageRows==null || this.state.tablePage <= 0 )} onClick={this.previousPage}>previous</Button>
					<span>
						{pageIndicator}
					</span>
					<Button disabled={(this.state.pageRows==null || (this.state.tablePage + 1) * this.state.pageSize >= this.state.max)} onClick={this.nextPage}>next</Button>
				</div>;
		}
	}),

	ODFAssetDetails : React.createClass({

		mixins : [AJAXCleanupMixin],

		onHide : function(){
			if(this.props.onHide){
				this.props.onHide();
			}
			if(this.isMounted()){
				this.setState({show: true});
			}
		},

		getInitialState : function(){
			return {
					show : true
					};
		},

		getType : function(){
			if(this.props.assetType){
				return this.props.assetType;
			}else if(this.props.asset && this.props.asset.type){
				return this.props.asset.type;
			}
			return null;
		},

		getUISpec : function(asset){
			return UISpec[this.getType()];
		},

		getPropertiesByType : function(srcObject, uiSpecAttributes){
			var properties = [];
			var references = [];
			var lists = [];
			var objects = [];
			if (uiSpecAttributes) {
	            var label = null;
	            var func = null;
	            var key = null;
	            $.each(uiSpecAttributes, function(index, property){
	                var value = ODFGlobals.getPathValue(srcObject, property.key);
	                if (value) {
	                    if(property.func){
	                        value = property.func(value, srcObject);
	                    }
	                    var obj = property;
	                    obj.value = value;
	                    if(value && Array.isArray(value)){
	                        lists.push(obj);
	                    }else if(value && value.id && value.repositoryId){
	                        references.push(obj);
	                    } /*else if(typeof value === "object"){
	                        objects.push(obj);
	                    } */else{
	                        properties.push(obj);
	                    }
	                }
	            }.bind(this) );
			}
	        return {lists: lists, properties: properties, references: references, objects: objects};
		},

		sortPropsByLabelPosition : function(properties, uiSpecAttributes){
			if(uiSpecAttributes){
				properties.sort(function(val1, val2){
					var index1 = -1;
					var index2 = -1;
					for(var no = 0; no < uiSpecAttributes.length; no++){
						if(uiSpecAttributes[no].label == val1.label){
							index1 = no;
						}else if(uiSpecAttributes[no].label == val2.label){
							index2 = no
						}
						if(index1 != -1 && index2 != -1){
							break;
						}
					}
					if(index1 > index2){
						return 1;
					}else if(index1 < index2){
						return -1;
					}
					return 0;
				});
			}
		},

		createPropertiesJSX : function(properties){
			var props = [];
			$.each(properties, function(key, val){
				var value = val.value;
				if(value){
					var prop = <FormControls.Static key={key} label={val.label} standalone>{val.value}</FormControls.Static>
					props.push(prop);
				}
			}.bind(this));
			return props;
		},

		createReferenceJSX : function(references){
			var refs = [];
			$.each(references, function(key, val){
				var prop = <a key={key} href={val.value.url}>{val.label}</a>
				refs.push(prop);
			}.bind(this));
			return refs;
		},

		createObjectJSX : function(objects){
			var objs = [];
			$.each(objects, function(key, val){
				var obj = <span key={key}>{JSON.stringify(val.value)}</span>;
				objs.push(obj);
			}.bind(this));

			return objs;
		},

		createTableJSX : function(lists){
			var tables = [];
			$.each(lists, function(key, val){
				var isRemote = false;
				var first = val.value[0];
				var rowReferences = null;
				var rowAssets = null;
				if(first && first.id && first.repositoryId){
					rowReferences = val.value;
				}else{
					rowAssets = val.value;
				}

				var spec = null;
				var label = val.label.toLowerCase();
				var type = label;
				if(val.uiSpec){
					spec = UISpec[val.uiSpec];
				}else{
					spec = UISpec[type];
				}

				var table = <div key={val.label + "_" + key}>
								<h3>{val.label}</h3>
								<ODFBrowser.ODFPagingTable rowAssets={rowAssets} assetType={type} rowReferences={rowReferences} onRowClick={this.props.onReferenceClick} spec={spec}/>
							</div>;
				tables.push(table);
			}.bind(this));

			return tables;
		},

		render : function(){
			var loadingOverlay = <div style={{position:"absolute", width:"100%", height:"100%", left:"50%", top: "30%"}}><Image src="img/lg_proc.gif" rounded /></div>;
			if(!this.props.loading){
				loadingOverlay = null;
			}

			var tablesPanel = <Panel collapsible defaultExpanded={false} header="References">
	          </Panel>;
			var propertiesPanel = <Panel collapsible defaultExpanded={false} header="Properties">
	          </Panel>;

			if(this.props.asset){
				var uiSpecAttrs = this.getUISpec(this.props.asset).attributes;
				if(!uiSpecAttrs){
					uiSpecAttrs = [];
					$.each(this.props.asset, function(propName, val){
						var label = propName;
						if(label && label[0]){
							label = label[0].toUpperCase() + label.slice(1);
						}
						uiSpecAttrs.push({key: propName, label: label});
					});
				}
				var allProps = this.getPropertiesByType(this.props.asset, uiSpecAttrs);

				var properties = allProps.properties;
				var references = allProps.references;
				var objects = allProps.objects;
				var lists = allProps.lists;

				var props = [];
				var refs = [];
				var objs = [];
				var tables = [];

				this.sortPropsByLabelPosition(properties, uiSpecAttrs);
				props = this.createPropertiesJSX(properties);
				refs = this.createReferenceJSX(references);
				objs = this.createObjectJSX(objects);
				tables = this.createTableJSX(lists);

				if(props.length > 0 || refs.length > 0 || objs.length > 0){
					propertiesPanel = <Panel collapsible defaultExpanded={true} header="Properties">
							     		{props}
							     		{refs}
							     		{objs}
							     	  </Panel>;
				}

				if(tables.length > 0){
					tablesPanel = <Panel collapsible defaultExpanded={true} header="References">
						     		{tables}
						          </Panel>;
				}
			}

			var icon = null;
			if(this.getUISpec(this.props.asset) && this.getUISpec(this.props.asset).icon){
				icon = this.getUISpec(this.props.asset).icon;
			}

		    var title = <span>{icon} Details</span>;
		    if(this.props.asset && this.props.asset.reference){
		          title = <div>{title} <a target="_blank" href={this.props.asset.reference.url}>( {this.props.asset.reference.id} )</a></div>;
		    }
			return <Modal show={this.props.show} onHide={this.onHide}>
			        	<Modal.Header closeButton>
			           <Modal.Title>{title}</Modal.Title>
				        </Modal.Header>
				        <Modal.Body>
					    	{loadingOverlay}
					        {propertiesPanel}
					        {tablesPanel}
						</Modal.Body>
				       <Modal.Footer>
				       <Button onClick={function(){this.onHide();}.bind(this)}>Close</Button>
				       </Modal.Footer>
					</Modal>
		}

	}),

	//Atlas Metadata browser: either pass an atlas query in the query property in order to execute the query and display the results
	ODFMetadataBrowser : React.createClass({

		mixins : [AJAXCleanupMixin],

		getInitialState : function() {
			return ({
				assets: null,
				loadingAssetDetails: false
				});
		},

		componentWillMount : function() {
			if(this.props.selection){
				this.loadSelectionFromAtlas(this.props.selection);
			}
		},

		referenceClick: function(val, type){
			if(!type || (type && val.type)){
				type = val.type;
			}
			var selectedAsset = {id: val.reference.id, repositoryId: val.reference.repositoryId, type: type};
			URLHelper.setUrlHash(selectedAsset);
		},

		loadSelectionFromAtlas : function(selection){
			if(selection){
				this.setState({showAssetDetails: true, loadingAssetDetails: true});
				var sel = selection;
				if(!sel.id){
					sel = JSON.parse(decodeURIComponent(sel));
				}

				var loading = false;
				if(sel.id && sel.repositoryId){
					if(!this.state.assetDetails || !this.state.assetDetails.reference || this.state.assetDetails.reference &&
							(this.state.assetDetails.reference.id != sel.id ||
							this.state.assetDetails.reference.repositoryId != sel.repositoryId)){
						loading = true;
						var req = AtlasHelper.loadAtlasAsset(sel,
								function(data){
									if(!data.type && sel.type){
										data.type = sel.type;
									}
									var state = {
											assetDetails: data,
											loadingAssetDetails: false};
									this.setState(state);
								}.bind(this),
								function(){

								}
						);
						this.storeAbort(req.abort);
					}
				}
				if(!loading && this.state.loadingAssetDetails){
					this.setState({loadingAssetDetails: false});
				}
			}
		},

		componentWillReceiveProps : function(nextProps){
			if(!this.isMounted()){
				return;
			}
			var newState = {};
			if(nextProps.selection && this.props.selection != nextProps.selection){
				this.loadSelectionFromAtlas(nextProps.selection);
			}else if(nextProps.selection == null){
				newState.assetDetails = null;
				newState.showAssetDetails = false;
			}
			this.setState(newState);
		},

		render : function(){
			var loadingImg = null;
			var list = null;
			if(this.props.assets){
				list = <ODFBrowser.ODFPagingTable actions={this.props.actions} rowAssets={this.props.assets} onRowClick={this.referenceClick} assetType={this.props.type}/>;
			}else{
				loadingImg = <Image src="img/lg_proc.gif" rounded />;
			}

			return <div>{list}
						{loadingImg}
						<ODFBrowser.ODFAssetDetails show={this.state.assetDetails != null || this.state.loadingAssetDetails} loading={this.state.loadingAssetDetails} key={(this.state.assetDetails ? this.state.assetDetails.id : "0")} onReferenceClick={this.referenceClick} asset={this.state.assetDetails} onHide={function(){URLHelper.setUrlHash(); this.setState({assetDetails : null})}.bind(this)} />
					</div>;
		}
	})
}

module.exports = ODFBrowser;
