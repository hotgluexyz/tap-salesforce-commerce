"""Salesforce tap class."""

from typing import List

import requests

from hotglue_singer_sdk import Tap, Stream
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.exceptions import RetriableAPIError
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

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
    CustomerGroupsStream,
    CustomersStream,
    CustomerAddressesStream,
    CatalogsByIdStream,
    ProductSearchStream,
    AllProductsIdsStream,
    OrderNotesStream,
    ProductsDataApiStream,
    ProductVariationsListStream,
    ProductsVariantsDataApiStream,
    ProductAvailabilityStream,
    MasterProductStream,
    VariationGroupStream
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
    CustomerGroupsStream,
    CustomersStream,
    CustomerAddressesStream,
    CatalogsByIdStream,
    ProductSearchStream,
    AllProductsIdsStream,
    OrderNotesStream,
    ProductsDataApiStream,
    ProductVariationsListStream,
    ProductsVariantsDataApiStream,
    ProductAvailabilityStream,
    MasterProductStream,
    VariationGroupStream
]


class TapSalesforce(Tap):
    """Salesforce tap class."""

    name = "tap-salesforce"
    alerting_level = AlertingLevel.ERROR
    exception_alerting_level_map = {
        RetriableAPIError: AlertingLevel.NONE,
        requests.exceptions.RequestException: AlertingLevel.NONE,
    }

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
