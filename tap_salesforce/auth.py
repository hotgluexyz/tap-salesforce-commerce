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

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the TapDynamicsFinance API."""
        return {"grant_type": "client_credentials"}

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload

        token_response = requests.post(
            self.auth_endpoint,
            data=auth_request_payload,
            auth=(self.config["client_id"], self.config["client_secret"]),
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
            auth_endpoint="https://account.demandware.com/dw/oauth2/access_token",
        )
    
class SalesForceUsernameAuth(SalesForceAuth):
    """Authenticator class for TapDynamicsFinance."""

    # Authentication and refresh
    def update_access_token(self) -> None:
        domain = self.config.get("sf_domain", self.config.get("domain"))
        client_id = self.config["client_id"]
        auth_str = f"{self.config['username']}:{self.config['password']}:{self.config['client_secret']}"
        auth_header = base64.b64encode(auth_str.encode("ascii")).decode("ascii")

        r = requests.post(
            f"https://{domain}.dx.commercecloud.salesforce.com/dw/oauth2/access_token?client_id={client_id}",
            headers={"Authorization": f"Basic {auth_header}"},
            data={
                "grant_type": "urn:demandware:params:oauth:grant-type:client-id:dwsid:dwsecuretoken"
            },
        )
        auth_payload = r.json()
        self.access_token = auth_payload["access_token"]

