"""REST client handling, including SalesforceStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from memoization import cached
from tap_salesforce.auth import SalesForceAuth


class SalesforceStream(RESTStream):
    """Salesforce stream class."""

    api_version = "v23_1"
    access_token = None
    expires_in = None
    last_refreshed = None
    params = {}
    product_ids = []

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        full_domain = self.config.get("full_domain")
        domain = self.config.get("sf_domain", self.config.get("domain"))
        site_id = self.config["site_id"]

        if self.name in ["products", "product_variations", "prices", "orders", "products_search"]:
             url_base = f"{full_domain}/s/{site_id}/dw/shop/{self.api_version}" if full_domain is not None else f"https://{domain}.dx.commercecloud.salesforce.com/s/{site_id}/dw/shop/{self.api_version}"
        else:
            # Non site specific URL
            url_base = f"{full_domain}/s/-/dw/data/{self.api_version}" if full_domain is not None else f"https://{domain}.dx.commercecloud.salesforce.com/s/-/dw/data/{self.api_version}"
        return url_base

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.next"

    @property
    @cached
    def authenticator(self) -> SalesForceAuth:
        """Return a new authenticator object."""
        return SalesForceAuth.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
            headers["Accept"] = "application/json"
        headers["x-dw-client-id"] = str(self.config.get("client_id"))
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
        params = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.name == "products":
            #send expand params to get extra values
            params["expand"] = "prices"
        if hasattr(self,"select"):
            params["select"] = self.select
        if hasattr(self,"expand"):
            params["expand"] = self.expand
        if hasattr(self,"include_all"):
            params["include_all"] = self.include_all
        if hasattr(self,"count"):
            params["count"] = self.count
        if self.name == "products_search":
            params["refine"] = f"cgid={context.get('root_category')}"
            params["client_id"] = self.config.get("client_id")
        return params

    def validate_response(self, response: requests.Response) -> None:
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif (
            400 <= response.status_code < 500
            and response.json()["fault"]["type"] != "ProductNotFoundException"
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response.status_code not in [404]:
            yield from extract_jsonpath(self.records_jsonpath, input=response.json())
