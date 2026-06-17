"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from hotglue_singer_sdk.testing import get_standard_tap_tests

from tap_salesforce.tap import TapSalesforce

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapSalesforce, config=SAMPLE_CONFIG)
    for test in tests:
        test()


def test_invalid_credentials_error_does_not_alert():
    """Invalid credentials errors should not trigger connector alerting."""
    assert (
        TapSalesforce.exception_alerting_level_map[InvalidCredentialsError]
        == AlertingLevel.NONE
    )
