"""Stream type classes for tap-salesforce."""

from singer_sdk import typing as th
from typing import Iterable, Optional, cast, Dict, Any
from tap_salesforce.client import SalesforceStream
import requests
from simplejson.scanner import JSONDecodeError 
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from datetime import datetime
from singer_sdk.helpers.jsonpath import extract_jsonpath
import copy

class InventoryListsStream(SalesforceStream):
    """Define custom stream."""

    name = "inventory_lists"
    path = "/inventory_lists"
    primary_keys = ["id"]
    records_jsonpath = "$.data[*]"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("resource_state", th.StringType),
        th.Property("id", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "inventory_id": record["id"],
        }


class ProductInventoryRecords(SalesforceStream):
    """Define custom stream."""

    name = "product_inventory_records"
    path = "/inventory_lists/{inventory_id}/product_inventory_records"
    primary_keys = ["product_id"]
    records_jsonpath = "$.data[*]"
    parent_stream_type = InventoryListsStream
    replication_key = None

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("product_id", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # add product_ids to a global env to use them in products/{product_id}
        res_json = response.json()
        product_ids = [{"product_id": prod["product_id"]} for prod in res_json.get("data", [])]
        SalesforceStream.product_ids = SalesforceStream.product_ids + product_ids
        # parse_response as usual
        yield from extract_jsonpath(self.records_jsonpath, input=res_json)


class CatalogsStream(SalesforceStream):
    """Define custom stream."""

    name = "catalogs"
    path = "/catalogs"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"
    select = "(**)"

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("id", th.StringType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("description", th.CustomType({"type": ["object", "string"]})),
        th.Property("online", th.BooleanType),
        th.Property("start_maintenance", th.DateTimeType),
        th.Property("end_maintenance", th.DateTimeType),
        th.Property("creation_date", th.DateTimeType),
        th.Property("is_master_catalog", th.BooleanType),
        th.Property("is_storefront_catalog", th.BooleanType),
        th.Property("root_category", th.StringType),
        th.Property("category_count", th.NumberType),
        th.Property("owned_product_count", th.NumberType),
        th.Property("assigned_product_count", th.NumberType),
        th.Property("recommendation_count", th.NumberType),
        th.Property("assigned_sites", th.CustomType({"type": ["array", "string"]})),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "catalog_id": record["id"],
        }
    
class CatalogsByIdStream(SalesforceStream):
    """Define custom stream."""

    name = "catalogs_by_id"
    path = "/catalogs/{catalog_id}"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$"
    parent_stream_type = CatalogsStream

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("assigned_sites",
            th.ArrayType(
                th.ObjectType(
                    th.Property("_type", th.StringType),
                    th.Property("cartridges", th.StringType),
                    th.Property("creation_date", th.DateTimeType),
                    th.Property("customer_list_link", th.ObjectType(					
                        th.Property("_type", th.StringType),
                        th.Property("customer_list_id", th.StringType),
                        th.Property("link", th.StringType),
                    )),
                    th.Property("display_name",th.ObjectType(					
                        th.Property("default", th.StringType),
                    )),
                    th.Property("id", th.StringType),
                    th.Property("in_deletion", th.BooleanType),
                    th.Property("last_modified", th.DateTimeType),
                    th.Property("link", th.StringType),
                    th.Property("storefront_status", th.StringType),
        ))),
        th.Property("category_count", th.NumberType),
        th.Property("creation_date", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("last_modified", th.DateTimeType),
        th.Property("link", th.StringType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("online", th.BooleanType),
        th.Property("recommendation_count", th.NumberType),
        th.Property("root_category", th.StringType),
        th.Property("c_cdc-personalization-lead-time", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "root_category": record["root_category"],
        }
    
class ProductSearchStream(SalesforceStream):
    """Define custom stream."""

    name = "products_search"
    path = "/product_search"
    primary_keys = ["product_id"]
    replication_key = None
    records_jsonpath = "$.hits[*]"
    parent_stream_type = CatalogsByIdStream
    count = 200

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("hit_type", th.StringType),
        th.Property("link", th.StringType),
        th.Property("product_id", th.StringType),
        th.Property("product_type" ,th.ObjectType(					
            th.Property("_type", th.StringType),
            th.Property("master", th.BooleanType),
        )),
        th.Property("represented_product",th.ObjectType(					
            th.Property("_type", th.StringType),
            th.Property("id", th.StringType),
            th.Property("link", th.StringType),
        )),
        th.Property("c_position", th.NumberType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # add product_ids to a global env to use them in products/{product_id}
        res_json = response.json()
        product_ids = [{"product_id": prod["product_id"]} for prod in res_json.get("hits", [])]
        SalesforceStream.product_ids = SalesforceStream.product_ids + product_ids
        # parse_response as usual
        yield from extract_jsonpath(self.records_jsonpath, input=res_json)


class AllProductsIdsStream(SalesforceStream):
    """Define custom stream."""

    name = "products_ids"
    path = "/"
    primary_keys = ["product_id"]

    schema = th.PropertiesList(
        th.Property("product_id", th.StringType),
    ).to_dict()

    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        return SalesforceStream.product_ids
    
    def parse_response(self, response):
        yield from extract_jsonpath(self.records_jsonpath, input=response)

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "product_id": record["product_id"]
        }
    
    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        return None

class ProductsStream(SalesforceStream):
    """Define custom stream."""

    name = "products"
    path = "/products/{product_id}"
    primary_keys = ["id"]
    replication_key = None
    select = "(**)"
    expand = "availability,bundled_products,links,promotions,options,images,prices,variations,set_products,recommendations"
    parent_stream_type = AllProductsIdsStream
    currencies = ["USD", "EUR", "GBP"]
    first_currency = "USD"


    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("brand", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("ean", th.StringType),
        th.Property("id", th.StringType),
        th.Property("image_groups", th.CustomType({"type": ["array", "string"]})),
        th.Property("inventory", th.CustomType({"type": ["object", "string"]})),
        th.Property("long_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("master", th.CustomType({"type": ["object", "string"]})),
        th.Property("min_order_quantity", th.NumberType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("options", th.CustomType({"type": ["array", "string"]})),
        th.Property("page_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_keywords", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_title", th.CustomType({"type": ["object", "string"]})),
        th.Property("price", th.NumberType),
        th.Property("price_per_unit", th.NumberType),
        th.Property("prices", th.CustomType({"type": ["object", "string"]})),
        th.Property("primary_category_id", th.StringType),
        th.Property("product_promotions", th.CustomType({"type": ["array", "string"]})),
        th.Property("short_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("step_quantity", th.NumberType),
        th.Property(
            "type",
           th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("unit_measure", th.StringType),
        th.Property("unit_quantity", th.IntegerType),
        th.Property("upc", th.StringType),
        th.Property("valid_from", th.CustomType({"type": ["object", "string"]})),
        th.Property("valid_to", th.CustomType({"type": ["object", "string"]})),
        th.Property("variants", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "variation_attributes", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("variation_values", th.CustomType({"type": ["object", "string"]})),


        # TODO: Are these going to be dynamic custom fields? Maybe we need to implement dynamic discover here
        th.Property("c_color", th.StringType),
        th.Property("c_refinementColor", th.StringType),
        th.Property("c_size", th.StringType),
        th.Property("c_width", th.StringType),
        th.Property("c_styleNumber", th.StringType),
        th.Property("c_tabDescription", th.StringType),
        th.Property("c_tabDetails", th.StringType)
    ).to_dict()

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        # using to iterate through currencies to get all available prices
        previous_token = previous_token or 0
        if previous_token < len(self.currencies) - 1:
            self.first_currency = None
            next_page_token = previous_token + 1
            return next_page_token
        
        #initialize first curency for next product
        self.first_currency = self.currencies[0]
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        if self.first_currency:
            params["currency"] = self.first_currency
        elif next_page_token:
            params["currency"] = self.currencies[next_page_token]
        return params
    
    def parse_response(self, response: requests.Response):
        if response.status_code not in [400]:
            return super().parse_response(response)
        return []
    
    def validate_response(self, response: requests.Response) -> None:
        res_json = {}
        try:
            res_json = response.json()
        except JSONDecodeError:
            msg = (
                f"Received non-JSON response from {self.path}. "
                f"Content preview: {response.text}"
            )
            raise FatalAPIError(msg)
        
        if response.status_code == 400:
            if res_json.get("fault", {}).get("type") == "UnsupportedCurrencyException":
                # TODO: should we be removing the currency here? I think so, to avoid repeated 400 errors
                self.currencies.remove(res_json.get("fault", {}).get("arguments", {}).get("currency"))
        elif (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:  
            if res_json.get("fault", {}).get("type") != "ProductNotFoundException":
                error_title = res_json.get("title", "")
                error_detail = res_json.get("detail", res_json.get("fault")) or ""
                msg = self.response_error_message(response)
                msg = f"{msg}, Salesforce error: {error_title} - {error_detail}"
                raise FatalAPIError(msg)



class ProductsDataApiStream(SalesforceStream):
    """Define custom stream."""

    name = "products_data_api"
    path = "/product_search"
    replication_key = "last_modified"
    records_jsonpath = "$.hits[*]"
    rest_method = "POST"
    count = 200

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("brand", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("ean", th.StringType),
        th.Property("id", th.StringType),
        th.Property("image_groups", th.CustomType({"type": ["array", "string"]})),
        th.Property("inventory", th.CustomType({"type": ["object", "string"]})),
        th.Property("long_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("master", th.CustomType({"type": ["object", "string"]})),
        th.Property("min_order_quantity", th.NumberType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("options", th.CustomType({"type": ["array", "string"]})),
        th.Property("page_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_keywords", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_title", th.CustomType({"type": ["object", "string"]})),
        th.Property("price", th.NumberType),
        th.Property("price_per_unit", th.NumberType),
        th.Property("prices", th.CustomType({"type": ["object", "string"]})),
        th.Property("primary_category_id", th.StringType),
        th.Property("product_promotions", th.CustomType({"type": ["array", "string"]})),
        th.Property("short_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("step_quantity", th.NumberType),
        th.Property(
            "type",
           th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("unit_measure", th.StringType),
        th.Property("unit_quantity", th.IntegerType),
        th.Property("upc", th.StringType),
        th.Property("valid_from", th.CustomType({"type": ["object", "string"]})),
        th.Property("valid_to", th.CustomType({"type": ["object", "string"]})),
        th.Property("variants", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "variation_attributes", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("last_modified", th.DateTimeType),
        th.Property("variation_values", th.CustomType({"type": ["object", "string"]})),
        th.Property("c_color", th.StringType),
        th.Property("c_refinementColor", th.StringType),
        th.Property("c_size", th.StringType),
        th.Property("c_width", th.StringType),
        th.Property("c_styleNumber", th.StringType),
        th.Property("c_tabDescription", th.StringType),
        th.Property("c_tabDetails", th.StringType)
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        # get all products that are master products, then request the rest of the products as their variations
        start_date = self.get_starting_time(context)
        return {
            "query": {
                "filtered_query": {
                    "filter": {
                        "range_filter": {
                            "field": "last_modified",
                            "from": start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        }
                    },
                    "query": {
                        "term_query": {
                            "fields" : ["type"],
                            "operator":"is",
                            "values":["master"]
                        }
                    }
                }
            },
            "expand": [
                "all"
            ],
            "select": "(**)",
            "count": self.count,
            "start": next_page_token
        }
    
    def get_child_context(self, record, context) -> dict:
        return {"master_product_id": record["id"]}


class ProductVariationsListStream(SalesforceStream):
    name = "product_variants_list"
    path = "/products/{master_product_id}/variations"
    count = 200
    records_jsonpath = "$.data[*]"
    parent_stream_type = ProductsDataApiStream

    schema = th.PropertiesList(
        th.Property("product_id", th.StringType)
    ).to_dict()

    def get_child_context(self, record, context) -> dict:
        return {"variation_id": record["product_id"], "master_product_id": context["master_product_id"]}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # add product_ids to a global env to use them in products/{product_id}
        res_json = response.json()
        product_ids = [{"product_id": prod["product_id"]} for prod in res_json.get("data", [])]
        SalesforceStream.product_ids = SalesforceStream.product_ids + product_ids
        # parse_response as usual
        yield from extract_jsonpath(self.records_jsonpath, input=res_json)

class ProductsVariantsDataApiStream(SalesforceStream):
    """Define product variants data stream."""

    name = "product_variations_data_api"
    path = "/products/{variation_id}"
    primary_keys = ["id"]
    parent_stream_type = ProductVariationsListStream
    replication_key = None
    records_jsonpath = "$.[*]"

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("brand", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("ean", th.StringType),
        th.Property("id", th.StringType),
        th.Property("image_groups", th.CustomType({"type": ["array", "string"]})),
        th.Property("inventory", th.CustomType({"type": ["object", "string"]})),
        th.Property("long_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("master", th.CustomType({"type": ["object", "string"]})),
        th.Property("min_order_quantity", th.NumberType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("options", th.CustomType({"type": ["array", "string"]})),
        th.Property("page_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_keywords", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_title", th.CustomType({"type": ["object", "string"]})),
        th.Property("price", th.NumberType),
        th.Property("price_per_unit", th.NumberType),
        th.Property("prices", th.CustomType({"type": ["object", "string"]})),
        th.Property("primary_category_id", th.StringType),
        th.Property("product_promotions", th.CustomType({"type": ["array", "string"]})),
        th.Property("short_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("step_quantity", th.NumberType),
        th.Property(
            "type",
           th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("unit_measure", th.StringType),
        th.Property("unit_quantity", th.IntegerType),
        th.Property("upc", th.StringType),
        th.Property("valid_from", th.CustomType({"type": ["object", "string"]})),
        th.Property("valid_to", th.CustomType({"type": ["object", "string"]})),
        th.Property("variants", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "variation_attributes", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("last_modified", th.DateTimeType),
        th.Property("variation_values", th.CustomType({"type": ["object", "string"]})),
        th.Property("c_color", th.StringType),
        th.Property("c_refinementColor", th.StringType),
        th.Property("c_size", th.StringType),
        th.Property("c_width", th.StringType),
        th.Property("c_styleNumber", th.StringType),
        th.Property("c_tabDescription", th.StringType),
        th.Property("c_tabDetails", th.StringType),
        th.Property("master_product_id", th.StringType)
    ).to_dict()


class GlobalProductsStream(SalesforceStream):
    """Define custom stream."""

    name = "global_products"
    path = "/products/{product_id}"
    primary_keys = ["id"]
    replication_key = None
    select = "(**)"
    expand = "all"
    parent_stream_type = ProductInventoryRecords

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("assigned_categories", th.ArrayType(
            th.ObjectType(
                th.Property("_type", th.StringType),
                th.Property("catalog_id", th.StringType),
                th.Property("category_id", th.StringType),
            )
        )),
        th.Property("ats", th.NumberType),
        th.Property("brand", th.StringType),
        th.Property(
            "classification_category", th.CustomType({"type": ["object", "array"]})
        ),
        th.Property("creation_date", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("image", th.CustomType({"type": ["object", "string"]})),
        th.Property("image_groups", th.CustomType({"type": ["array", "string"]})),
        th.Property("in_stock", th.BooleanType),
        th.Property("last_modified", th.DateTimeType),
        th.Property("link", th.StringType),
        th.Property("long_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("master", th.CustomType({"type": ["object", "string"]})),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("online", th.BooleanType),
        th.Property(
            "online_flag",
            th.ObjectType(
                th.Property("default", th.BooleanType),
            ),
        ),
        th.Property("owning_catalog_id", th.StringType),
        th.Property(
            "owning_catalog_name", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("page_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_keywords", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_title", th.CustomType({"type": ["object", "string"]})),
        th.Property("primary_categories", th.CustomType({"type": ["object", "array"]})),
        th.Property("primary_category_id", th.StringType),
        th.Property("product_options", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "searchable",
            th.ObjectType(
                th.Property("default", th.BooleanType),
            ),
        ),
        th.Property("short_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("tax_class_id", th.StringType),
        th.Property(
            "type",
           th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("unit_quantity", th.IntegerType),
        th.Property("upc", th.StringType),
        th.Property("valid_from", th.CustomType({"type": ["object", "string"]})),
        th.Property("valid_to", th.CustomType({"type": ["object", "string"]})),
        th.Property(
            "variation_attributes", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("variation_values", th.CustomType({"type": ["object", "string"]})),

        # TODO: Custom fields
        th.Property("c_color", th.StringType),
        th.Property("c_refinementColor", th.StringType),
        th.Property("c_size", th.StringType),
        th.Property("c_width", th.StringType),
        th.Property("c_tabDescription", th.CustomType({"type": ["object", "string"]})),
        th.Property("c_tabDetails", th.CustomType({"type": ["object", "string"]}))
    ).to_dict()


class ProductsVariationAttributesStream(SalesforceStream):
    """Define custom stream."""

    name = "product_variations"
    path = "/products/{product_id}/variations"
    primary_keys = ["id"]
    parent_stream_type = ProductInventoryRecords
    replication_key = None
    records_jsonpath = "$.[*]"

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("id", th.StringType),
        th.Property("long_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("min_order_quantity", th.NumberType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("page_title", th.CustomType({"type": ["object", "string"]})),
        th.Property("short_description", th.StringType),
        th.Property("step_quantity", th.NumberType),
        th.Property("type", th.CustomType({"type": ["object", "string"]})),
        th.Property("unit_measure", th.StringType),
        th.Property("upc", th.StringType),
        th.Property("unit_quantity", th.NumberType),
        th.Property("variants", th.CustomType({"type": ["array", "string"]})),
        th.Property("variation_values", th.CustomType({"type": ["object", "string"]})),
        th.Property("c_color", th.StringType),
        th.Property("c_refinementColor", th.StringType),
        th.Property("c_size", th.StringType),
        th.Property("c_width", th.StringType),
    ).to_dict()

class ProductsPricesStream(SalesforceStream):
    """Define custom stream."""

    name = "prices"
    path = "/products/{product_id}/prices"
    primary_keys = ["id"]
    parent_stream_type = ProductInventoryRecords
    replication_key = None
    records_jsonpath = "$.[*]"

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("brand", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("id", th.StringType),
        th.Property("long_description", th.StringType),
        th.Property("min_order_quantity", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("page_description", th.StringType),
        th.Property("page_keywords", th.StringType),
        th.Property("page_title", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("price_max", th.NumberType),
        th.Property("price_per_unit", th.NumberType),
        th.Property("price_per_unit_max", th.NumberType),
        th.Property("primary_category_id", th.StringType),
        th.Property("short_description", th.StringType),
        th.Property("step_quantity", th.NumberType),
        th.Property("type", th.ObjectType(
            th.Property("master", th.BooleanType)
        )),
        th.Property("unit_measure", th.StringType),
        th.Property("unit_quantity", th.NumberType),
        th.Property("c_displaySize", th.StringType),
        th.Property("c_resolution", th.StringType),
        th.Property("c_tabDescription", th.StringType),
        th.Property("c_tabDetails", th.StringType),
    ).to_dict()

class CategoriesStream(SalesforceStream):
    """Define custom stream."""

    name = "categories"
    path = "/catalogs/{catalog_id}/categories"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"
    parent_stream_type = CatalogsStream
    select = "(**)"
    expand = "vm"

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("categories", th.ArrayType(
            th.ObjectType(
                th.Property("_type", th.StringType),
                th.Property("creation_date", th.StringType),
                th.Property("description", th.CustomType({"type": ["object", "string"]})),
                th.Property("id", th.StringType),
                th.Property("image", th.StringType),
                th.Property("link", th.StringType),
                th.Property("name", th.CustomType({"type": ["object", "string"]})),
                th.Property("online", th.BooleanType),
                th.Property("parent_category_id", th.StringType),
                th.Property("paths", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                th.Property("position", th.NumberType),
                th.Property("thumbnail", th.StringType),
            )
        )),
        th.Property("creation_date", th.DateTimeType),
        th.Property("description", th.CustomType({"type": ["object", "string"]})),
        th.Property("id", th.StringType),
        th.Property("image", th.StringType),
        th.Property("link", th.StringType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("online", th.BooleanType),
        th.Property("paths", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("position", th.NumberType),
        th.Property("sorting_rules", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("thumbnail", th.StringType),
    ).to_dict()


class SitesStream(SalesforceStream):
    """Define custom stream."""

    name = "sites"
    path = "/sites"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"
    select = "(**)"

    #pending to get customers from customer link

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property(
            "customer_list_link", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("description", th.CustomType({"type": ["object", "string"]})),
        th.Property("display_name", th.CustomType({"type": ["object", "string"]})),
        th.Property("id", th.StringType),
        th.Property("in_deletion", th.BooleanType),
        th.Property("in_deletion", th.BooleanType),
        th.Property("storefront_status", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "site_id": record["id"],
        }


class SiteLocalesStream(SalesforceStream):
    """Define custom stream."""

    name = "site_locale_info"
    path = "/sites/{site_id}/locale_info/locales"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.hits[*]"
    select = "(**)"
    include_all = True
    parent_stream_type = SitesStream
    order_ids = []  
    

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("country", th.StringType),
        th.Property("default", th.BooleanType),
        th.Property("display_country", th.StringType),
        th.Property("display_language", th.StringType),
        th.Property("display_name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("iso3_country", th.StringType),
        th.Property("iso3_language", th.StringType),
        th.Property("language", th.StringType),
        th.Property("name", th.StringType),
        th.Property("site_id", th.StringType),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.order_ids = self.config.get("order_ids", [])

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row.update({"site_id": context.get("site_id")})
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        order_ids = self.order_ids
        ids_len = len(order_ids or [])
        previous_token = previous_token or 0
        if ids_len > 0 and previous_token < ids_len - 1:
            next_page_token = previous_token + 1
            return next_page_token
        else:
            return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params=""
        if self.order_ids:
            index = next_page_token or 0
            params =  self.order_ids[index]
        return params
    
    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        
        http_method = self.rest_method
        params: str = self.get_url_params(context, next_page_token)
        url: str = self.get_url(context)
        if params:
            url = f"{url}/{params}"

        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    headers=headers,
                    json=request_data,
                ),
            ),
        )
        return request
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            if self.order_ids:
                resp = decorated_request(prepared_request, context)
            else:
                resp = []
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
            finished = not next_page_token

class CustomerGroupsStream(SalesforceStream):
    """Define custom stream."""

    name = "customer_groups"
    path = "/sites/{site_id}/customer_groups"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"
    parent_stream_type = SitesStream

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("id", th.StringType),
        th.Property("link", th.StringType),
        th.Property("site_id", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "site_id": context["site_id"],
            "customer_group_id": record["id"]
        }
    

class CustomersStream(SalesforceStream):
    """Define custom stream."""

    name = "customers"
    path = "/sites/{site_id}/customer_groups/{customer_group_id}/members"
    primary_keys = ["customer_no"]
    replication_key = None
    records_jsonpath = "$.data[*]"
    parent_stream_type = CustomerGroupsStream
    select = "(**)"

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("customer_no", th.StringType),
        th.Property("login", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("link", th.StringType),
        th.Property("site_id", th.StringType),
        th.Property("customer_group_id", th.StringType),
        th.Property("customer_link", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response.status_code == 200:
            return super().parse_response(response)
        return []

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        # pending to see if customer list is the same as site
        customer_link = record.get("customer_link")
        list_id = None
        if customer_link:
            list_id = customer_link.split(f"customer_lists/")[-1].split("/")[0]
        return {
            "customer_no": record["customer_no"],
            "list_id": list_id
        }
    
class CustomerAddressesStream(SalesforceStream):
    """Define custom stream."""

    name = "customer_addresses"
    path = "/customer_lists/{list_id}/customers/{customer_no}/addresses"
    records_jsonpath = "$.data[*]"
    parent_stream_type = CustomersStream

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("address1", th.StringType),
        th.Property("address_id", th.StringType),
        th.Property("city", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("country_code", th.StringType),
        th.Property("creation_date", th.DateTimeType),
        th.Property("etag", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("last_modified", th.DateTimeType),
        th.Property("last_name", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("postal_code", th.StringType),
        th.Property("salutation", th.StringType),
        th.Property("state_code", th.StringType),
        th.Property("customer_no", th.StringType),
    ).to_dict()


class OrdersStream(SalesforceStream):
    """Define custom stream."""

    name = "orders"
    path = "/order_search"
    replication_key = "last_modified"
    records_jsonpath = "$.hits[*].data"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("adjusted_merchandize_total_tax", th.NumberType),
        th.Property("adjusted_shipping_total_tax", th.NumberType),
        th.Property(
            "billing_address",
            th.ObjectType(
                th.Property("_type", th.StringType),
                th.Property("address1", th.StringType),
                th.Property("city", th.StringType),
                th.Property("country_code", th.StringType),
                th.Property("first_name", th.StringType),
                th.Property("full_name", th.StringType),
                th.Property("id", th.StringType),
                th.Property("last_name", th.StringType),
                th.Property("phone", th.StringType),
                th.Property("postal_code", th.StringType),
                th.Property("state_code", th.StringType),
            ),
        ),
        th.Property("channel_type", th.StringType),
        th.Property("confirmation_status", th.StringType),
        th.Property("created_by", th.StringType),
        th.Property("creation_date", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("customer_info", th.ObjectType(                 
            th.Property("_type", th.StringType),
            th.Property("customer_id", th.StringType),
            th.Property("customer_name", th.StringType),
        )),
        th.Property("customer_name", th.StringType),
        th.Property("export_status", th.StringType),
        th.Property("grouped_tax_items", th.ArrayType(
            th.ObjectType(
                th.Property("_type", th.StringType),
                th.Property("tax_rate", th.NumberType),
                th.Property("tax_value", th.NumberType),
        ))),
        th.Property("guest", th.BooleanType),
        th.Property("last_modified", th.DateTimeType),
        th.Property("merchandize_total_tax", th.NumberType),
        th.Property("notes",th.ObjectType(                  
            th.Property("_type", th.StringType),
            th.Property("link", th.StringType),
        )),
        th.Property("order_no", th.StringType),
        th.Property("order_token", th.StringType),
        th.Property("order_total", th.NumberType),
        th.Property("payment_instruments",
            th.ArrayType(
                th.ObjectType(
                    th.Property("_type", th.StringType),
                    th.Property("amount", th.NumberType),
                    th.Property("payment_instrument_id", th.StringType),
                    th.Property("payment_method_id", th.StringType),
        ))),
        th.Property("payment_status", th.StringType),
        th.Property("product_items",
            th.ArrayType(
                th.ObjectType(
                    th.Property("_type", th.StringType),
                    th.Property("adjusted_tax", th.NumberType),
                    th.Property("base_price", th.NumberType),
                    th.Property("bonus_product_line_item", th.BooleanType),
                    th.Property("gift", th.BooleanType),
                    th.Property("item_id", th.StringType),
                    th.Property("item_text", th.StringType),
                    th.Property("price", th.NumberType),
                    th.Property("price_after_item_discount", th.NumberType),
                    th.Property("price_after_order_discount", th.NumberType),
                    th.Property("product_id", th.StringType),
                    th.Property("product_name", th.StringType),
                    th.Property("quantity", th.NumberType),
                    th.Property("shipment_id", th.StringType),
                    th.Property("tax", th.NumberType),
                    th.Property("tax_basis", th.NumberType),
                    th.Property("tax_class_id", th.StringType),
                    th.Property("tax_rate", th.NumberType),
        ))),
        th.Property("product_sub_total", th.NumberType),
        th.Property("product_total", th.NumberType),
        th.Property("shipments",
            th.ArrayType(
                th.ObjectType(
                    th.Property("_type", th.StringType),
                    th.Property("adjusted_merchandize_total_tax", th.NumberType),
                    th.Property("adjusted_shipping_total_tax", th.NumberType),
                    th.Property("gift", th.BooleanType),
                    th.Property("merchandize_total_tax", th.NumberType),
                    th.Property("product_sub_total", th.NumberType),
                    th.Property("product_total", th.NumberType),
                    th.Property("shipment_id", th.StringType),
                    th.Property("shipment_total", th.NumberType),
                    th.Property("shipping_address",th.ObjectType(                  
                        th.Property("_type", th.StringType),
                        th.Property("address1", th.StringType),
                        th.Property("city", th.StringType),
                        th.Property("country_code", th.StringType),
                        th.Property("first_name", th.StringType),
                        th.Property("full_name", th.StringType),
                        th.Property("id", th.StringType),
                        th.Property("last_name", th.StringType),
                        th.Property("phone", th.StringType),
                        th.Property("postal_code", th.StringType),
                        th.Property("state_code", th.StringType),
                    )),
                    th.Property("shipping_method",th.ObjectType(                  
                        th.Property("_type", th.StringType),
                        th.Property("id", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property("price", th.NumberType),
                    )),
                    th.Property("shipping_status", th.StringType),
                    th.Property("shipping_total", th.NumberType),
                    th.Property("shipping_total_tax", th.NumberType),
                    th.Property("tax_total", th.NumberType),
                    th.Property("shipping_items",
            th.ArrayType(
                th.ObjectType(
                    th.Property("_type", th.StringType),
                    th.Property("adjusted_tax", th.NumberType),
                    th.Property("base_price", th.NumberType),
                    th.Property("item_id", th.StringType),
                    th.Property("item_text", th.StringType),
                    th.Property("price", th.NumberType),
                    th.Property("price_after_item_discount", th.NumberType),
                    th.Property("shipment_id", th.StringType),
                    th.Property("tax", th.NumberType),
                    th.Property("tax_basis", th.NumberType),
                    th.Property("tax_class_id", th.StringType),
                    th.Property("tax_rate", th.NumberType),
        ))),
        th.Property("shipping_status", th.StringType),
        th.Property("shipping_total", th.NumberType),
        th.Property("shipping_total_tax", th.NumberType),
        th.Property("site_id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("taxation", th.StringType),
        th.Property("tax_rounded_at_group", th.BooleanType),
        th.Property("tax_total", th.NumberType),
        ))),
        th.Property("tax_total", th.NumberType),
        th.Property("shipping_status", th.StringType),
        th.Property("shipping_total", th.NumberType),
        th.Property("shipping_total_tax", th.NumberType),
        th.Property("site_id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("taxation", th.StringType),
    ).to_dict()


    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        if self.config.get("order_ids"):
            pagination = 0
            next_page_token = next_page_token or 0
            order_ids = self.config.get("order_ids")
            query = { 
                "text_query": { 
                    "fields": [
                        "order_no"
                    ],
                    "search_phrase":order_ids[next_page_token]
                }
            }
        else:
            pagination = next_page_token
            start_date = (self.start_date or self.get_starting_time(context)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            end_date = datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            query = { 
                "filtered_query": {
                    "filter": {
                        "range_filter": { 	
                            "field": "last_modified",
                            "from": start_date,
                            "to": end_date
                        }
                    },
                    "query" : {
                        "match_all_query": {}
                    }
                }
            }

        order_page_size = self.config.get("order_page_size", 200)
        payload = { 
            "count": order_page_size,
            "query" : query,
            "select" : "(**)",
            "sorts" : [{"field":"last_modified", "sort_order":"asc"}],
            "start": pagination
        }
        return payload
    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "order_no": record["order_no"],
        }
    
    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        order_ids = self.config.get("order_ids")
        if order_ids:
            previous_token = previous_token or 0
            if previous_token < len(order_ids) - 1:
                next_page_token = previous_token + 1
                return next_page_token
            return None
        else:
            return super().get_next_page_token(response, previous_token)


class OrderNotesStream(SalesforceStream):
    """Define custom stream."""

    name = "order_notes"
    path = "/orders/{order_no}/notes"
    records_jsonpath = "$.notes[*]"
    parent_stream_type = OrdersStream

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("created_by", th.StringType),
        th.Property("creation_date", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("subject", th.StringType),
        th.Property("text", th.StringType),
        th.Property("order_no", th.StringType),
    ).to_dict()