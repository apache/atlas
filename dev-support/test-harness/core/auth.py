"""Authentication providers: OAuth2 TokenManager + BasicAuth."""

import os
import threading
import time

import requests
from requests.auth import HTTPBasicAuth


class TokenManager:
    """OAuth2 client-credentials with thread-safe auto-refresh."""

    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = 0
        self._lock = threading.Lock()

    def get_token(self):
        if not self.access_token or time.time() >= self.token_expiry:
            with self._lock:
                if not self.access_token or time.time() >= self.token_expiry:
                    self._refresh_token()
        return self.access_token

    def force_refresh(self):
        with self._lock:
            self._refresh_token()

    def _refresh_token(self):
        payload = {
            "client_id": self.client_id,
            "grant_type": "client_credentials",
            "client_secret": self.client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        resp = requests.post(self.token_url, headers=headers, data=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        expires_in = data.get("expires_in", 60)
        self.token_expiry = int(time.time()) + expires_in - 30


class AuthProvider:
    """Wraps TokenManager (OAuth2) or BasicAuth for use by AtlasClient."""

    def __init__(self, token_manager=None, basic_auth=None):
        self.token_manager = token_manager
        self.basic_auth = basic_auth

    def get_headers(self):
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token_manager:
            headers["Authorization"] = f"Bearer {self.token_manager.get_token()}"
            # Required by staging/preprod for endpoints like /direct/search
            headers["x-atlan-client-origin"] = "test-harness"
        return headers

    def get_requests_auth(self):
        """Return requests auth object for basic auth, or None for OAuth2."""
        return self.basic_auth if self.basic_auth else None

    def handle_401(self):
        """Force-refresh token on 401. Returns True if retry makes sense."""
        if self.token_manager:
            self.token_manager.force_refresh()
            return True
        return False


def load_creds(creds_file, tenant):
    """Load client_secret from creds.yaml for a given tenant."""
    try:
        import yaml
    except ImportError:
        raise RuntimeError("pyyaml required for --tenant mode: pip install pyyaml")

    if not os.path.exists(creds_file):
        raise FileNotFoundError(f"Creds file not found: {creds_file}")

    with open(creds_file, "r") as f:
        creds = yaml.safe_load(f)

    creds_list = creds.get("creds", []) if isinstance(creds, dict) else []
    if isinstance(creds_list, list):
        for entry in creds_list:
            entry_tenant = entry.get("tenant", "")
            if entry_tenant == tenant or entry_tenant == f"{tenant}.atlan.com":
                secret = entry.get("client_secret") or entry.get("secret")
                if secret:
                    return secret

    if isinstance(creds, dict) and tenant in creds:
        entry = creds[tenant]
        if isinstance(entry, dict):
            return entry.get("client_secret") or entry.get("secret")

    available = [e.get("tenant") for e in creds_list] if isinstance(creds_list, list) else []
    raise RuntimeError(
        f"No credentials for tenant '{tenant}' in {creds_file}. Available: {available}"
    )


def build_auth_provider(config) -> AuthProvider:
    """Build an AuthProvider from HarnessConfig."""
    if config.tenant:
        token_url = (
            f"https://{config.tenant}.atlan.com"
            f"/auth/realms/default/protocol/openid-connect/token"
        )
        client_secret = load_creds(config.creds_file, config.tenant)
        tm = TokenManager(token_url, "atlan-argo", client_secret)
        return AuthProvider(token_manager=tm)
    else:
        return AuthProvider(basic_auth=HTTPBasicAuth(config.user, config.password))
