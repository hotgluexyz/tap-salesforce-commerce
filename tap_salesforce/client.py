"""REST client handling, including SalesforceStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class SalesforceStream(RESTStream):
    """Salesforce stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        domain = self.config["domain"]
        url_base = f"https://{domain}.dx.commercecloud.salesforce.com/s/-/dw/data/v23_1"
        return url_base

    records_jsonpath = "$[*]" 
    next_page_token_jsonpath = "$.next" 

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("access_token")
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
            headers["Accept"] = "application/json"
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if response.status_code not in [404]:
            res_json = response.json()
            if "next" in res_json and res_json["next"]:
                previous_token = previous_token or 0
                res_json = response.json()
                count = res_json["count"]
                next_page_token = previous_token + count
                return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        return params
    
    def validate_response(self, response: requests.Response) -> None:
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500 and response.json()["fault"]["type"] != "ProductNotFoundException":
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response.status_code not in [404]:
            yield from extract_jsonpath(self.records_jsonpath, input=response.json())