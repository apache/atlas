"""
Comprehensive tests for DataContract delete SDK methods.
Tests ContractDelete helper and ContractClient.delete / delete_latest_version.
"""
import sys
sys.path.insert(0, "/Users/shiva.hanumanthula/yes/atlan-data-quality-studio-app/.venv/lib/python3.13/site-packages")

import pytest
from unittest.mock import MagicMock, patch, call

from pyatlan.client.contract import ContractClient
from pyatlan.client.common.contract import ContractDelete, ContractInit
from pyatlan.client.constants import DELETE_ENTITIES_BY_GUIDS
from pyatlan.model.enums import AtlanDeleteType
from pyatlan.model.response import AssetMutationResponse


# ============================================================
# ContractDelete helper tests
# ============================================================

class TestContractDeleteHelper:

    def test_prepare_request_single_guid(self):
        params = ContractDelete.prepare_request("guid-123")
        assert params == {"deleteType": "HARD", "guid": ["guid-123"]}

    def test_prepare_request_list_of_guids(self):
        params = ContractDelete.prepare_request(["guid-1", "guid-2", "guid-3"])
        assert params == {"deleteType": "HARD", "guid": ["guid-1", "guid-2", "guid-3"]}

    def test_prepare_request_empty_list(self):
        params = ContractDelete.prepare_request([])
        assert params == {"deleteType": "HARD", "guid": []}

    def test_prepare_request_default_is_hard(self):
        params = ContractDelete.prepare_request("guid-x")
        assert params["deleteType"] == "HARD"

    def test_prepare_request_explicit_hard(self):
        params = ContractDelete.prepare_request("guid-x", AtlanDeleteType.HARD)
        assert params["deleteType"] == "HARD"

    def test_prepare_request_explicit_purge(self):
        params = ContractDelete.prepare_request("guid-x", AtlanDeleteType.PURGE)
        assert params["deleteType"] == "PURGE"

    def test_process_response_returns_mutation_response(self):
        raw = {"mutatedEntities": {"DELETE": [{"typeName": "DataContract", "guid": "g1"}]}}
        result = ContractDelete.process_response(raw)
        assert isinstance(result, AssetMutationResponse)

    def test_process_response_empty_mutations(self):
        raw = {"mutatedEntities": {}}
        result = ContractDelete.process_response(raw)
        assert isinstance(result, AssetMutationResponse)

    def test_header_constant(self):
        assert ContractDelete.CONTRACT_DELETE_SCOPE_HEADER == "x-atlan-contract-delete-scope"


# ============================================================
# ContractClient fixture
# ============================================================

@pytest.fixture
def mock_api_client():
    client = MagicMock()
    client._request_params = {"headers": {"authorization": "Bearer test-token"}}
    client._call_api = MagicMock(return_value={"mutatedEntities": {}})
    return client


@pytest.fixture
def contract_client(mock_api_client):
    with patch("pyatlan.client.contract.isinstance", return_value=True):
        return ContractClient(client=mock_api_client)


# ============================================================
# ContractClient.delete() tests
# ============================================================

class TestContractClientDelete:

    def test_delete_calls_correct_api(self, contract_client, mock_api_client):
        contract_client.delete.__wrapped__(contract_client, "guid-123")

        mock_api_client._call_api.assert_called_once()
        args, kwargs = mock_api_client._call_api.call_args
        assert args[0] == DELETE_ENTITIES_BY_GUIDS

    def test_delete_sends_hard_delete_type(self, contract_client, mock_api_client):
        contract_client.delete.__wrapped__(contract_client, "guid-123")

        _, kwargs = mock_api_client._call_api.call_args
        assert kwargs["query_params"]["deleteType"] == "HARD"

    def test_delete_sends_guid_as_list(self, contract_client, mock_api_client):
        contract_client.delete.__wrapped__(contract_client, "guid-abc")

        _, kwargs = mock_api_client._call_api.call_args
        assert kwargs["query_params"]["guid"] == ["guid-abc"]

    def test_delete_returns_mutation_response(self, contract_client, mock_api_client):
        mock_api_client._call_api.return_value = {
            "mutatedEntities": {"DELETE": [{"typeName": "DataContract", "guid": "guid-123"}]}
        }
        result = contract_client.delete.__wrapped__(contract_client, "guid-123")
        assert isinstance(result, AssetMutationResponse)

    def test_delete_does_not_set_scope_header(self, contract_client, mock_api_client):
        contract_client.delete.__wrapped__(contract_client, "guid-123")
        assert "x-atlan-contract-delete-scope" not in mock_api_client._request_params["headers"]

    def test_delete_does_not_set_scope_header_during_call(self, contract_client, mock_api_client):
        """Verify the scope header is never present during the API call for delete()."""
        captured = {}
        def capture(*args, **kwargs):
            captured.update(dict(mock_api_client._request_params["headers"]))
            return {"mutatedEntities": {}}
        mock_api_client._call_api.side_effect = capture

        contract_client.delete.__wrapped__(contract_client, "guid-123")
        assert "x-atlan-contract-delete-scope" not in captured


# ============================================================
# ContractClient.delete_latest_version() tests
# ============================================================

class TestContractClientDeleteLatestVersion:

    def test_delete_latest_calls_correct_api(self, contract_client, mock_api_client):
        contract_client.delete_latest_version.__wrapped__(contract_client, "guid-456")

        mock_api_client._call_api.assert_called_once()
        args, kwargs = mock_api_client._call_api.call_args
        assert args[0] == DELETE_ENTITIES_BY_GUIDS

    def test_delete_latest_sends_hard_delete_type(self, contract_client, mock_api_client):
        contract_client.delete_latest_version.__wrapped__(contract_client, "guid-456")

        _, kwargs = mock_api_client._call_api.call_args
        assert kwargs["query_params"]["deleteType"] == "HARD"

    def test_delete_latest_sends_guid_as_list(self, contract_client, mock_api_client):
        contract_client.delete_latest_version.__wrapped__(contract_client, "guid-456")

        _, kwargs = mock_api_client._call_api.call_args
        assert kwargs["query_params"]["guid"] == ["guid-456"]

    def test_delete_latest_returns_mutation_response(self, contract_client, mock_api_client):
        mock_api_client._call_api.return_value = {
            "mutatedEntities": {"DELETE": [{"typeName": "DataContract", "guid": "guid-456"}]}
        }
        result = contract_client.delete_latest_version.__wrapped__(contract_client, "guid-456")
        assert isinstance(result, AssetMutationResponse)

    def test_delete_latest_sets_scope_header_during_call(self, contract_client, mock_api_client):
        """The scope header must be 'single' when the API call is made."""
        captured = {}
        def capture(*args, **kwargs):
            captured.update(dict(mock_api_client._request_params["headers"]))
            return {"mutatedEntities": {}}
        mock_api_client._call_api.side_effect = capture

        contract_client.delete_latest_version.__wrapped__(contract_client, "guid-456")
        assert captured.get("x-atlan-contract-delete-scope") == "single"

    def test_delete_latest_cleans_up_header_after_success(self, contract_client, mock_api_client):
        contract_client.delete_latest_version.__wrapped__(contract_client, "guid-456")
        assert "x-atlan-contract-delete-scope" not in mock_api_client._request_params["headers"]

    def test_delete_latest_cleans_up_header_after_error(self, contract_client, mock_api_client):
        """Header must be cleaned up even if the API call raises."""
        mock_api_client._call_api.side_effect = Exception("API failure")

        with pytest.raises(Exception, match="API failure"):
            contract_client.delete_latest_version.__wrapped__(contract_client, "guid-err")

        assert "x-atlan-contract-delete-scope" not in mock_api_client._request_params["headers"]

    def test_delete_latest_does_not_corrupt_existing_headers(self, contract_client, mock_api_client):
        """Existing headers must remain untouched after the call."""
        mock_api_client._request_params["headers"]["custom-header"] = "keep-me"

        contract_client.delete_latest_version.__wrapped__(contract_client, "guid-456")

        assert mock_api_client._request_params["headers"]["authorization"] == "Bearer test-token"
        assert mock_api_client._request_params["headers"]["custom-header"] == "keep-me"
        assert "x-atlan-contract-delete-scope" not in mock_api_client._request_params["headers"]


# ============================================================
# Cross-method isolation tests
# ============================================================

class TestCrossMethodIsolation:

    def test_delete_after_delete_latest_no_header_leak(self, contract_client, mock_api_client):
        """Calling delete() after delete_latest_version() must not carry over the scope header."""
        contract_client.delete_latest_version.__wrapped__(contract_client, "guid-1")

        captured = {}
        def capture(*args, **kwargs):
            captured.update(dict(mock_api_client._request_params["headers"]))
            return {"mutatedEntities": {}}
        mock_api_client._call_api.side_effect = capture

        contract_client.delete.__wrapped__(contract_client, "guid-2")
        assert "x-atlan-contract-delete-scope" not in captured

    def test_sequential_delete_latest_calls(self, contract_client, mock_api_client):
        """Multiple delete_latest_version calls should each set and clean up header."""
        for i in range(3):
            captured = {}
            def capture(*args, **kwargs):
                captured.update(dict(mock_api_client._request_params["headers"]))
                return {"mutatedEntities": {}}
            mock_api_client._call_api.side_effect = capture

            contract_client.delete_latest_version.__wrapped__(contract_client, f"guid-{i}")
            assert captured.get("x-atlan-contract-delete-scope") == "single"
            assert "x-atlan-contract-delete-scope" not in mock_api_client._request_params["headers"]


# ============================================================
# Contract method accessibility from AtlanClient
# ============================================================

class TestAtlanClientIntegration:

    def test_contracts_property_exists(self):
        import pyatlan.client.atlan as atlan_module
        assert hasattr(atlan_module.AtlanClient, "contracts")

    def test_contract_client_has_all_methods(self):
        assert hasattr(ContractClient, "generate_initial_spec")
        assert hasattr(ContractClient, "delete")
        assert hasattr(ContractClient, "delete_latest_version")

    def test_delete_uses_hard_not_purge(self):
        """HARD delete triggers processDelete preprocessor; PURGE bypasses it."""
        params = ContractDelete.prepare_request("test-guid")
        assert params["deleteType"] == "HARD", (
            "Must use HARD (not PURGE) so backend ContractPreProcessor.processDelete fires"
        )


# ============================================================
# Existing ContractInit unchanged
# ============================================================

class TestContractInitUnchanged:

    def test_contract_init_still_importable(self):
        from pyatlan.client.common.contract import ContractInit
        assert hasattr(ContractInit, "prepare_request")
        assert hasattr(ContractInit, "process_response")

    def test_generate_initial_spec_still_exists(self):
        assert hasattr(ContractClient, "generate_initial_spec")
