"""Zendesk tap class."""

from __future__ import annotations

from typing import Any

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th  # JSON schema typing helpers
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from typing_extensions import override

from tap_zendesk.auth import (
    TOKEN_ENDPOINT,
    ZendeskAuthenticator,
    token_endpoint_for,
)
from tap_zendesk.client import CustomFieldsMixin
from tap_zendesk.streams import STREAM_TYPES

CONFIG_JSONSCHEMA = th.PropertiesList(
    th.Property(
        "start_date",
        th.DateTimeType,
        description="The earliest record date to sync",
        default="2000-01-01T00:00:00Z",
    ),
    th.Property(
        "subdomain",
        th.StringType,
        required=True,
        description="Zendesk subdomain, i.e. the `acme` in `acme.zendesk.com`",
    ),
    th.Property(
        "client_id",
        th.StringType,
        required=True,
        description="OAuth client ID for the Zendesk OAuth app",
    ),
    th.Property(
        "client_secret",
        th.StringType,
        required=True,
        description="OAuth client secret for the Zendesk OAuth app",
    ),
    th.Property(
        "refresh_token",
        th.StringType,
        description="OAuth refresh token for the Zendesk OAuth app",
    ),
    th.Property(
        "request_timeout",
        th.NumberType,
        description="Request timeout in seconds",
        default=300,
    ),
    th.Property(
        "marketplace_name",
        th.StringType,
        description="Sent as the X-Zendesk-Marketplace-Name header",
    ),
    th.Property(
        "marketplace_organization",
        th.StringType,
        description="Sent as the X-Zendesk-Marketplace-Organization-Id header",
    ),
    th.Property(
        "marketplace_app_id",
        th.StringType,
        description="Sent as the X-Zendesk-Marketplace-App-Id header",
    ),
).to_dict()

# `request_timeout` accepts a string as well as a number: the pre-SDK tap
# coerced with `float()`, and config renderers emit numbers as strings.
# Widened here because `th.Property` appends "null" to the declared type,
# which would nest the list if it were declared as a CustomType above.

# `request_timeout` accepts a string as well as a number: the pre-SDK tap coerced
# with `float()`, and config renderers emit numbers as strings. Widened here
# because `th.Property` appends "null" to the declared type, which would nest the
# list if it were declared as a CustomType above.
CONFIG_JSONSCHEMA["properties"]["request_timeout"]["type"] = ["number", "string", "null"]


class TapZendesk(Tap):
    """Singer tap for Zendesk."""

    name = "tap-zendesk"

    config_jsonschema = CONFIG_JSONSCHEMA

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @override
    def run_discovery(self) -> str:
        """Add the account's custom fields to the catalog before emitting it.

        Returns:
            The discovered catalog as JSON.
        """
        for stream in self.streams.values():
            if isinstance(stream, CustomFieldsMixin):
                stream.merge_custom_fields()
        return super().run_discovery()

    @classmethod
    def access_token_support(
        cls,
        connector: Any = None,
    ) -> tuple[type[OAuthAuthenticator], str]:
        """Return the authenticator class and OAuth token endpoint.

        Returns:
            A tuple with the authenticator class and the OAuth token endpoint URL.
        """
        # The token endpoint is subdomain-scoped, so it is derived from the config.
        # `connector` is None when the SDK probes for the capability in `--about`;
        # that probe must not raise, so fall back to the unformatted template.
        if connector is None:
            return ZendeskAuthenticator, TOKEN_ENDPOINT
        return ZendeskAuthenticator, token_endpoint_for(connector.config)


if __name__ == "__main__":
    TapZendesk.cli()
