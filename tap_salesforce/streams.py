"""Stream type classes for tap-salesforce."""

from singer_sdk import typing as th
from typing import Iterable, Optional, cast, Dict, Any
from tap_salesforce.client import SalesforceStream
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "product_id": record["product_id"],
        }

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
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.hits[*]"
    parent_stream_type = CatalogsByIdStream
    
    @property
    def params(self):
        return {
            "client_id": self.config.get("client_id"),
        }

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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "product_id": record["product_id"],
        }

class ProductsStream(SalesforceStream):
    """Define custom stream."""

    name = "products"
    path = "/products/{product_id}"
    primary_keys = ["id"]
    replication_key = None
    select = "(**)"
    expand = "availability,bundled_products,links,promotions,options,images,prices,variations,set_products,recommendations"
    parent_stream_type = ProductSearchStream
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
        self.first_currency = "USD"
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
        if response.status_code == 400:
            res_json = response.json()
            if res_json.get("fault", {}).get("type") == "UnsupportedCurrencyException":
                pass
        elif (
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row.update({"site_id": context.get("site_id")})
        return row

class OrdersStream(SalesforceStream):
    """Define custom stream."""

    name = "orders"
    path = "/orders/"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$."
    select = "(**)"

    @property
    def order_ids(self):
        return self.config.get("4gift_orders")

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("creation_date", th.DateTimeType),
        th.Property("customer_info", th.CustomType({"type": ["object", "string"]})),
        th.Property("currency", th.StringType),
        th.Property("order_no", th.StringType),
        th.Property("order_price_adjustments", th.ArrayType(
            th.ObjectType(
                th.Property("promotion_id", th.StringType),
                th.Property("promotion_link", th.StringType),
                th.Property("item_text", th.StringType),
                th.Property("price", th.NumberType),
            )
        )),
        th.Property("order_token", th.StringType),
        th.Property("order_total", th.StringType),
        th.Property("payment_instruments", th.ArrayType(
            th.ObjectType(
                th.Property("payment_instrument_id", th.StringType),
                th.Property("payment_method_id", th.StringType),
                th.Property("payment_card", th.CustomType({"type": ["object", "string"]})),
                th.Property("amount", th.StringType),
            )
        )),
        th.Property("product_items", th.ArrayType(
            th.ObjectType(
                th.Property("product_id", th.StringType),
                th.Property("item_text", th.StringType),
                th.Property("quantity", th.NumberType),
                th.Property("product_name", th.StringType),
                th.Property("base_price", th.StringType),
                th.Property("price", th.StringType),
                th.Property("price_adjustments", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
            )
        )),
        th.Property("product_sub_total", th.NumberType),
        th.Property("product_total", th.NumberType),
        th.Property("shipping_total", th.NumberType),
        th.Property("shipments", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("shipping_address", th.ObjectType(
                    th.Property("salutation", th.StringType),
                    th.Property("title", th.StringType),
                    th.Property("company_name", th.StringType),
                    th.Property("first_name", th.StringType),
                    th.Property("second_name", th.StringType),
                    th.Property("last_name", th.StringType),
                    th.Property("postal_code", th.StringType),
                    th.Property("address1", th.StringType),
                    th.Property("address2", th.StringType),
                    th.Property("city", th.StringType),
                    th.Property("post_box", th.StringType),
                    th.Property("country_code", th.StringType),
                    th.Property("state_code", th.StringType),
                    th.Property("phone", th.StringType),
                    th.Property("suffix", th.StringType),
                )),
                th.Property("shipping_method", th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("description", th.StringType),
                )),
            ),
        )),
        th.Property("status", th.DateTimeType),
        th.Property("tax_total", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        order_ids = self.order_ids
        ids_len = len(order_ids)
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
        url: str = self.get_url(context) + params
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

