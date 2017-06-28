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

var ODFAssetDetails = require("./odf-metadata-browser.js").ODFAssetDetails;
var ODFPagingTable = require("./odf-metadata-browser.js").ODFPagingTable;
var ODFGlobals = require("./odf-globals.js");
var AtlasHelper = require("./odf-utils.js").AtlasHelper;
var URLHelper = require("./odf-utils.js").URLHelper;
var AJAXCleanupMixin = require("./odf-mixins.js");

var Image = ReactBootstrap.Image;

var ODFRequestBrowser = React.createClass({

	mixins : [AJAXCleanupMixin],

	getInitialState : function(){
		return {assetDetails : null, loadingAssetDetails: false};
	},

	getDiscoveryServiceNameFromId(id) {
		if(!this.props.registeredServices){
			return id;
		}
		var servicesWithSameId = this.props.registeredServices.filter(
	         function(dsreg) {
	             return dsreg.id == id;
	         }
		);
		if (servicesWithSameId.length > 0) {
			return servicesWithSameId[0].name;
		}
		return id;
	},

	loadSelectedRequestStatus: function(requestId){
		if(requestId){
			this.setState({showAssetDetails: true, loadingAssetDetails: true});

			var req = $.ajax({
	            url: ODFGlobals.analysisUrl + "/" + requestId,
	            contentType: "application/json",
	            dataType: 'json',
	            type: 'GET',
	            success: function(data) {
	            	$.each(data.serviceRequests, function(key, request){
	            		var serviceName = this.getDiscoveryServiceNameFromId(request.discoveryServiceId);
	            		request.discoveryServiceName = serviceName;
            		}.bind(this));

	               this.setState({assetType: "request", assetDetails: data, loadingAssetDetails: false});
	            }.bind(this),
	            error: function(data){
	            	this.setState({loadingAssetDetails: false});
	            }.bind(this)
	        });
		    this.storeAbort(req.abort);

			if(this.state.loadingAssetDetails){
				this.setState({loadingAssetDetails: false});
			}
		}
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
										assetDetails: data, assetType: data.type, loadingAssetDetails: false};
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
		if((nextProps.selection && this.props.selection && this.props.selection.id != nextProps.selection.id) || (nextProps.selection && this.props.selection == null)){
			if(nextProps.selection.id && nextProps.selection.repositoryId){
				this.loadSelectionFromAtlas(nextProps.selection);
			}else{
				this.loadSelectedRequestStatus(nextProps.selection);
			}
		}else if(nextProps.selection == null){
			newState.assetDetails = null;
		}
		this.setState(newState);
	},

	rowClick : function(val, type){
		if(!type || (type && val.type)){
			type = val.type;
		}
		if(val && val.reference && val.reference.id){
			var selectedAsset = {id: val.reference.id, repositoryId: val.reference.repositoryId, type: type};
			URLHelper.setUrlHash(JSON.stringify(selectedAsset));
		}else if(val && val.request && val.request.id){
			URLHelper.setUrlHash(JSON.stringify({requestId : val.request.id}));
		}
	},

	render : function(){
		return <div>
					<ODFPagingTable actions={this.props.actions} rowAssets={this.props.assets} onRowClick={this.rowClick} assetType="requests"/>
					<ODFAssetDetails show={this.state.assetDetails != null || this.state.loadingAssetDetails} loading={this.state.loadingAssetDetails} key={(this.state.assetDetails ? this.state.assetDetails.id : "0")} onReferenceClick={this.rowClick} asset={this.state.assetDetails} assetType={this.state.assetType} onHide={function(){URLHelper.setUrlHash(); this.setState({showAssetDetails : false})}.bind(this)} />
				</div>;
	}
});

module.exports = ODFRequestBrowser;
