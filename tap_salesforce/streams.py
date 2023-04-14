"""Stream type classes for tap-salesforce."""

from singer_sdk import typing as th
from typing import Optional
from tap_salesforce.client import SalesforceStream


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


class ProductsStream(SalesforceStream):
    """Define custom stream."""

    name = "products"
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
        th.Property("ats", th.NumberType),
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
        th.Property("price", th.IntegerType),
        th.Property("price_currency", th.StringType),
        th.Property("price_per_unit", th.IntegerType),
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
            th.ObjectType(
                th.Property("_type", th.StringType),
                th.Property("part_of_product_set", th.BooleanType),
                th.Property("variant", th.BooleanType),
            ),
        ),
        th.Property("unit_quantity", th.IntegerType),
        th.Property("variation_attributes", th.CustomType({"type": ["array", "string"]})),
        th.Property("variation_values", th.CustomType({"type": ["object", "string"]})),
        th.Property("upc", th.StringType),
        th.Property("c_color", th.StringType),
        th.Property("c_refinementColor", th.StringType),
        th.Property("c_size", th.StringType),
        th.Property("c_width", th.StringType),
        th.Property("inventory", th.CustomType({"type": ["object", "string"]})),
        
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
        th.Property("_resource_state" , th.StringType),
        th.Property("id" , th.StringType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("description", th.CustomType({"type": ["object", "string"]})),
        th.Property("online" , th.BooleanType),
        th.Property("start_maintenance" , th.DateTimeType),
        th.Property("end_maintenance" , th.DateTimeType),
        th.Property("creation_date" , th.DateTimeType),
        th.Property("is_master_catalog" , th.BooleanType),
        th.Property("is_storefront_catalog" , th.BooleanType),
        th.Property("root_category" , th.StringType),
        th.Property("category_count" , th.NumberType),
        th.Property("owned_product_count" , th.NumberType),
        th.Property("assigned_product_count" , th.NumberType),
        th.Property("recommendation_count" , th.NumberType),
        th.Property("assigned_sites", th.CustomType({"type": ["array", "string"]})),
        th.Property("link" , th.StringType),
    ).to_dict()
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "catalog_id": record["id"],
        }
class CategoriesStream(SalesforceStream):
    """Define custom stream."""

    name = "categories"
    path = "/catalogs/{catalog_id}/categories"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"
    parent_stream_type = CatalogsStream
    select = "(**)"

    schema = th.PropertiesList(
        th.Property("_type", th.StringType),
        th.Property("_resource_state" , th.StringType),
        th.Property("id" , th.StringType),
        th.Property("catalog_id" , th.StringType),
        th.Property("creation_date" , th.DateTimeType),
        th.Property("last_modified" , th.DateTimeType),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
        th.Property("online" , th.BooleanType),
        th.Property("parent_category_id" , th.StringType),
        th.Property("position" , th.NumberType),
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
        th.Property("_resource_state" , th.StringType),
        th.Property("customer_list_link", th.CustomType({"type": ["object", "string"]})),
        th.Property("description", th.CustomType({"type": ["object", "string"]})),
        th.Property("display_name", th.CustomType({"type": ["object", "string"]})),
        th.Property("id" , th.StringType),
        th.Property("in_deletion" , th.BooleanType),
        th.Property("in_deletion" , th.BooleanType),
        th.Property("storefront_status" , th.StringType),
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
        row.update({"site_id":context.get("site_id")})
        return row
