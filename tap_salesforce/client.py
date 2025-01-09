"""REST client handling, including SalesforceStream base class."""

import requests
from typing import Any, Dict, Optional, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from memoization import cached
from tap_salesforce.auth import SalesForceAuth, SalesForceUsernameAuth
from pendulum import parse
from bs4 import BeautifulSoup


def extract_text_from_html(content: str) -> str:
    soup = BeautifulSoup(content, 'html.parser')
    text = '\n'.join(soup.stripped_strings)
    return text


class SalesforceStream(RESTStream):
    """Salesforce stream class."""

    api_version = "v23_1"
    access_token = None
    expires_in = None
    last_refreshed = None
    params = {}
    product_ids = []
    SITE_SPECIFIC_STREAMS = ["products", "product_variations", "prices", "orders", "all_orders", "products_search", "order_notes"]

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        full_domain = self.config.get("full_domain")
        domain = self.config.get("sf_domain", self.config.get("domain"))
        site_id = "{site_id}"

        if self.name in self.SITE_SPECIFIC_STREAMS:
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
        if self.name == "order_notes":
            return SalesForceUsernameAuth.create_for_stream(self)
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

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if self.name in self.SITE_SPECIFIC_STREAMS:
            site_ids = self.config.get("site_id").replace(" ", "").split(",")
            for site_id in site_ids:
                if context is None:
                    context = {}
                context.update({"site_id": site_id})
                yield from super().get_records(context)
        else:
            yield from super().get_records(context)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if response.status_code not in [404, 204]:
            res_json = response.json()
            if "next" in res_json and res_json["next"]:
                previous_token = previous_token or 0
                res_json = response.json()
                count = res_json["count"]
                next_page_token = previous_token + count
                return next_page_token
    
    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

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
        try:
            res_json = response.json()
        except Exception as exc:
            resp_text = extract_text_from_html(response.text)
            error_message = f"Error decoding JSON response. Status:{response.status_code} for url:{response.request.url} with response:\n{resp_text}\nException [{type(exc)}]: {exc}"
            raise FatalAPIError(error_message) from None
        if (
            400 <= response.status_code < 500
            and res_json.get("fault", {}).get("type") != "ProductNotFoundException"
        ):
            error_message = f"Status:{response.status_code} for url:{response.request.url} with response: {response.text}"
            self.logger.warn(error_message)
            raise FatalAPIError(error_message)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response != []:
            if response.status_code not in [404]:
                yield from extract_jsonpath(self.records_jsonpath, input=response.json())
