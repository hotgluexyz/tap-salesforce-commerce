"""Salesforce tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_salesforce.streams import (
    ProductsStream,
    InventoryListsStream,
    ProductInventoryRecords,
    ProductsVariationAttributesStream,
    CatalogsStream,
    CategoriesStream,
)

# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    ProductsStream,
    InventoryListsStream,
    ProductInventoryRecords,
    ProductsVariationAttributesStream,
    CatalogsStream,
    CategoriesStream,
]


class TapSalesforce(Tap):
    """Salesforce tap class."""

    name = "tap-salesforce"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
        ),
        th.Property("domain", th.StringType, required=True),
        th.Property("site_id", th.StringType, required=True),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapSalesforce.cli()
