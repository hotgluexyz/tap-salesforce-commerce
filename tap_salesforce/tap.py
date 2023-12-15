"""Salesforce tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_salesforce.streams import (
    ProductsStream,
    GlobalProductsStream,
    InventoryListsStream,
    ProductInventoryRecords,
    ProductsVariationAttributesStream,
    CatalogsStream,
    CategoriesStream,
    SitesStream,
    SiteLocalesStream,
    ProductsPricesStream,
    OrdersStream,
    CatalogsByIdStream,
    ProductSearchStream
)

STREAM_TYPES = [
    ProductsStream,
    GlobalProductsStream,
    InventoryListsStream,
    ProductInventoryRecords,
    ProductsVariationAttributesStream,
    CatalogsStream,
    CategoriesStream,
    SitesStream,
    SiteLocalesStream,
    ProductsPricesStream,
    OrdersStream,
    CatalogsByIdStream,
    ProductSearchStream
]


class TapSalesforce(Tap):
    """Salesforce tap class."""

    name = "tap-salesforce"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
        ),
        th.Property("site_id", th.StringType, required=True),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapSalesforce.cli()
