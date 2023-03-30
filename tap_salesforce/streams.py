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
    parent_stream_type = ProductInventoryRecords

    schema = th.PropertiesList(
        th.Property("_v", th.StringType),
        th.Property("_type", th.StringType),
        th.Property("_resource_state", th.StringType),
        th.Property("creation_date", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("last_modified", th.DateTimeType),
        th.Property("link", th.StringType),
        th.Property("long_description", th.CustomType({"type": ["object", "string"]})),
        th.Property("name", th.CustomType({"type": ["object", "string"]})),
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
        th.Property("page_title", th.CustomType({"type": ["object", "string"]})),
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
        th.Property("upc", th.StringType),
        th.Property("c_color", th.StringType),
        th.Property("c_refinementColor", th.StringType),
        th.Property("c_size", th.StringType),
        th.Property("c_width", th.StringType),
        th.Property("inventory", th.CustomType({"type": ["object", "string"]})),
        th.Property("price", th.IntegerType),
        th.Property("price_per_unit", th.IntegerType),
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
