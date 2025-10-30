"""REST client handling, including SalesforceStream base class."""

import requests
from typing import Any, Dict, Optional, Iterable

from tap_hotglue_sdk.helpers.jsonpath import extract_jsonpath
from tap_hotglue_sdk.helpers._typing import to_json_compatible
from tap_hotglue_sdk.helpers._state import write_starting_replication_value, STARTING_MARKER
from tap_hotglue_sdk.streams import RESTStream
from tap_hotglue_sdk.exceptions import FatalAPIError, RetriableAPIError
from tap_hotglue_sdk.streams.core import REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED
from memoization import cached
from tap_salesforce.auth import SalesForceAuth, SalesForceUsernameAuth
from pendulum import parse
from bs4 import BeautifulSoup
import copy
from tap_salesforce.utils import cover_access_token
import singer
import backoff

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
    max_dates = []
    start_date = None
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        full_domain = self.config.get("full_domain")
        full_domain = "https://" + full_domain if not full_domain.startswith("http") else full_domain
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
    def parallelization_limit(self) -> int:
        if hasattr(self, "parent_stream_type") and self.parent_stream_type is not None:
            return 25
        return 1

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

    def _increment_stream_state(self, latest_record: Dict[str, Any], *, context: Optional[dict] = None):
        if self.name in self.SITE_SPECIFIC_STREAMS and "," in self.config.get("site_id", ""):
            self.__increment_stream_state(latest_record, context = context)
        else:
            super()._increment_stream_state(latest_record, context = context)
            
    def _write_starting_replication_value(self, context: Optional[dict]) -> None:
        if self.name in self.SITE_SPECIFIC_STREAMS and "," in self.config.get("site_id", ""):
            self.__write_starting_replication_value(context)
        else:
            super()._write_starting_replication_value(context)

    def __increment_stream_state(
        self, latest_record: Dict[str, Any], context: Optional[dict] = None
    ) -> None:
        def increment_state(
            stream_or_partition_state: dict,
            latest_record: dict,
            replication_key: str,
            is_sorted: bool,
        ) -> None:
            """Update the state using data from the latest record.

            Raises InvalidStreamSortException if is_sorted=True and unsorted
            data is detected in the stream.
            """
            progress_dict = stream_or_partition_state
            old_rk_value = to_json_compatible(progress_dict.get("replication_key_value"))
            new_rk_value = to_json_compatible(latest_record[replication_key])
            if old_rk_value is None or new_rk_value >= old_rk_value:
                progress_dict["replication_key"] = replication_key
                progress_dict["replication_key_value"] = new_rk_value
                return
        
        state_dict = self.get_context_state(context)
        if latest_record:
            if self.replication_method in [
                REPLICATION_INCREMENTAL,
                REPLICATION_LOG_BASED,
            ]:
                if not self.replication_key:
                    raise ValueError(
                        f"Could not detect replication key for '{self.name}' stream"
                        f"(replication method={self.replication_method})"
                    )
                treat_as_sorted = self.is_sorted
                if not treat_as_sorted and self.state_partitioning_keys is not None:
                    # Streams with custom state partitioning are not resumable.
                    treat_as_sorted = False
                increment_state(
                    state_dict,
                    replication_key=self.replication_key,
                    latest_record=latest_record,
                    is_sorted=treat_as_sorted,
                )

    def __write_starting_replication_value(self, context: Optional[dict]) -> None:
        """Write the starting replication value, if available.

        Args:
            context: Stream partition or context dictionary.
        """
        value = None
        state = self.get_context_state(context)

        if self.replication_key:
            replication_key_value = state.get("replication_key_value")
            if replication_key_value and self.replication_key == state.get(
                "replication_key"
            ):
                value = replication_key_value

            elif "start_date" in self.config:
                value = self.config["start_date"]
        if STARTING_MARKER not in state:
            write_starting_replication_value(state, value)

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if self.name in self.SITE_SPECIFIC_STREAMS and "," in self.config.get("site_id", ""):
            site_ids = self.config.get("site_id").replace(" ", "").split(",")
            for site_id in site_ids:
                if context is None:
                    context = {}
                context = context.copy()
                context.update({"site_id": site_id})
                self._write_starting_replication_value(context)
                yield from super().get_records(context)
        else:
            yield from super().get_records(context)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if previous_token is None:
            self.start_date = None

        if response.status_code in [204, 404]:
            return None

        res_json = response.json()
        if "next" in res_json and res_json["next"]:
            previous_token = previous_token or 0
            res_json = response.json()
            count = res_json["count"]
            next_page_token = previous_token + count
            # For order_search endpoints, Salesforce has a 10000 record limit for pagination
            # When we hit that limit, we need to use the latest replication key value 
            # to filter and restart pagination from 0
            pagination_limit_streams = ["orders"] #it seems that this is the only endpoint that has this limit so far.
            pagination_limit = 10000
            if self.name in pagination_limit_streams and self.replication_key and next_page_token is not None and next_page_token >= pagination_limit:
                
                max_date = self.stream_state.get("progress_markers", {}).get(
                    "replication_key_value"
                )

                if max_date:
                    max_date = parse(max_date)
                else:
                    self.logger.warn("No replication key value found, not possible to continue pagination")
                    return None

                if max_date:
                    if max_date in self.max_dates:
                        self.logger.warn("Date based pagination loop detected")
                        return None
                    self.max_dates.append(max_date)
                    self.start_date = max_date

                next_page_token = 0

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
            count = self.config.get("order_page_size") if hasattr(self.config,"order_page_size") else self.count if hasattr(self,"count") else 200
            self.count = int(count / 2)
            raise RetriableAPIError(msg, response)
        try:
            res_json = response.json()
        except Exception as exc:
            resp_text = extract_text_from_html(response.text)
            error_message = f"Error decoding JSON response. Status:{response.status_code} for url:{response.request.url} with response:\n{resp_text}\nException [{type(exc)}]: {exc}"
            raise RetriableAPIError(error_message) from None
        if (
            400 <= response.status_code < 500
            and res_json.get("fault", {}).get("type") != "ProductNotFoundException"
        ):  
            resp_text = cover_access_token(response.text)
            error_message = f"Status:{response.status_code} for url:{response.request.url} with response: {resp_text}"
            self.logger.warn(error_message)
            raise FatalAPIError(error_message)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response != []:
            if response.status_code not in [404]:
                yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                # any child stream with no replication key doesn't need to be partitioned
                if tap_state["bookmarks"][stream_name].get("partitions") and not self.replication_key:
                    tap_state["bookmarks"][stream_name] = {"partitions": []}

        singer.write_message(singer.StateMessage(value=tap_state))
    
    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException, RetriableAPIError), max_tries=10)
    def _make_request(self, context: Optional[dict], next_page_token: Optional[Any]) -> requests.Response:
        prepared_request = self.prepare_request(
            context, next_page_token=next_page_token
        )
        resp = self._request(prepared_request, context)
        return resp

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._make_request)

        while not finished:
            resp = decorated_request(context, next_page_token)
            yield from self.parse_response(resp)
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = next_page_token is None

