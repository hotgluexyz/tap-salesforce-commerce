"""TapDynamicsFinance Authentication."""


from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
import requests
from singer_sdk.helpers._util import utc_now
from typing import Optional

import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.streams import Stream as RESTStreamBase
from datetime import datetime
import base64

# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class SalesForceAuth(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for TapDynamicsFinance."""

    def __init__(
        self,
        stream: RESTStreamBase,
        auth_endpoint: Optional[str] = None,
        oauth_scopes: Optional[str] = None,
        default_expiration: Optional[int] = None,
        config_file: Optional[str] = None,
    ) -> None:
        super().__init__(stream=stream)
        self._auth_endpoint = f"https://{self.config.get('sf_domain', self.config.get('domain'))}.dx.commercecloud.salesforce.com/dw/oauth2/access_token"
        self._default_expiration = default_expiration
        self._oauth_scopes = oauth_scopes
        self._config_file = config_file
        self._tap = stream._tap

        # Initialize internal tracking attributes
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.last_refreshed: Optional[datetime] = None
        self.expires_in: Optional[int] = None

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the TapDynamicsFinance API."""
        return {"grant_type": "urn:demandware:params:oauth:grant-type:client-id:dwsid:dwsecuretoken"}

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        client_id = self.config.get("client_id")
        auth_str = f"{self.config['username']}:{self.config['password']}:{self.config['client_secret']}"
        auth_header = base64.b64encode(auth_str.encode("ascii")).decode("ascii")

        auth_credentials = {
            "Authorization": f"Basic {auth_header}"
        }
        token_response = requests.post(
            self.auth_endpoint,
            data=auth_request_payload,
            headers= auth_credentials,
            params= {"client_id": client_id},
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json.get("expires_in", self._default_expiration)
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time

    @classmethod
    def create_for_stream(cls, stream) -> "SalesForceAuth":
        return cls(
            stream=stream,
        )
