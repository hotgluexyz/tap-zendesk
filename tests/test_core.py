"""Tests standard tap features using the built-in SDK tests library."""

import datetime

import pytest
from hotglue_singer_sdk.testing import get_standard_tap_tests

from tap_zendesk.tap import TapZendesk

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "subdomain": "placeholder",
    "client_id": "placeholder",
    "client_secret": "placeholder",
}

# These make live HTTP calls, so they are excluded by default. Replace the
# SAMPLE_CONFIG placeholders with real credentials and call them directly.
#
# `_test_discovery` is here because `--discover` reaches the API on purpose: it
# probes each stream for read access and fetches the account's custom fields,
# both of which the pre-SDK tap also did at discovery time.
_LIVE_TESTS = {"_test_stream_connections", "_test_discovery"}

_STANDARD_TESTS = [
    t
    for t in get_standard_tap_tests(TapZendesk, config=SAMPLE_CONFIG)
    if getattr(t, "__name__", "") not in _LIVE_TESTS
]


@pytest.mark.parametrize("test_func", _STANDARD_TESTS)
def test_standard(test_func):
    """Run built-in SDK tap tests (CLI output and catalog discovery)."""
    test_func()


# TODO: Create additional tests as appropriate for your tap.
