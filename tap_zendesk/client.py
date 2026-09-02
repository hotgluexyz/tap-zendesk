"""HTTP API client (REST or GraphQL), including ZendeskStream base class."""

from __future__ import annotations

from copy import deepcopy
from functools import cached_property
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import requests
from hotglue_singer_sdk.authenticators import APIAuthenticatorBase
from hotglue_singer_sdk.streams import RESTStream
from singer import utils
from singer.catalog import Catalog
from singer.transform import Transformer
from typing_extensions import override

from tap_zendesk.schema import process_custom_field

PAGE_SIZE = 100


class ZendeskStream(RESTStream):
    """Zendesk stream class."""

    records_jsonpath = "$[*]"
    page_size = PAGE_SIZE

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, derived from the ``subdomain`` tap setting."""
        return f"https://{self.config['subdomain']}.zendesk.com/api/v2"

    @override
    @cached_property
    def authenticator(self) -> APIAuthenticatorBase:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        authenticator_cls, auth_endpoint = self._tap.access_token_support(self._tap)
        return authenticator_cls(
            self,
            auth_endpoint=auth_endpoint,
            config_file=self._tap.config_file,
        )

    @override
    def apply_catalog(self, catalog: Catalog) -> None:
        """Adopt the catalog's schema when one is supplied.

        The pre-SDK tap emitted records against the catalog's schema, so a
        catalog enriched at discovery time (see `CustomFieldsMixin`) keeps its
        custom-field properties through the sync.

        Args:
            catalog: The catalog passed to the tap.
        """
        super().apply_catalog(catalog)
        entry = catalog.get_stream(self.name)
        if entry is not None and entry.schema is not None:
            self.schema = entry.schema.to_dict()

    @override
    @property
    def timeout(self) -> int:
        """Return the request timeout, honouring the `request_timeout` setting.

        Matches the pre-SDK tap: a falsy or zero value falls back to the
        default rather than being passed through.

        Returns:
            The request timeout in seconds.
        """
        configured = self.config.get("request_timeout")
        if configured and float(configured):
            return float(configured)
        return super().timeout

    @override
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {"Accept": "application/json"}
        # Zendesk asks marketplace apps to identify themselves on every request.
        marketplace = (
            ("X-Zendesk-Marketplace-Name", "marketplace_name"),
            ("X-Zendesk-Marketplace-Organization-Id", "marketplace_organization"),
            ("X-Zendesk-Marketplace-App-Id", "marketplace_app_id"),
        )
        if all(self.config.get(setting) for _, setting in marketplace):
            headers.update({header: str(self.config[setting]) for header, setting in marketplace})
        return headers

    @override
    def response_error_message(self, response: requests.Response) -> str:
        """Build an error message that includes Zendesk's own error text.

        The SDK default reports only the status code and path, which loses the
        vendor's description of what actually went wrong.

        Args:
            response: The failed `requests.Response`_ object.

        Returns:
            The error message.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        message = super().response_error_message(response)
        try:
            payload = response.json()
        except ValueError:
            return message
        detail = payload.get("error") or payload.get("description") or payload.get("message")
        if isinstance(detail, dict):
            detail = detail.get("message") or detail.get("title")
        return f"{message} Zendesk said: {detail}" if detail else message

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Run every record through singer's Transformer, as the pre-SDK tap did.

        `write_page` in the pre-SDK tap always transformed, which coerced values
        to their declared types and dropped fields the schemas do not declare.
        The SDK does not transform, so without this the emitted records drift
        from the pre-SDK tap's and a validating target rejects the streams whose
        schemas have fallen behind the API.

        Subclasses that reshape a record should mutate it and then delegate here,
        so the transform runs last — the same order the pre-SDK tap used.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The transformed record dictionary.
        """
        with Transformer() as transformer:
            return transformer.transform(row, self.schema)

    def start_time_epoch(self, context: dict | None) -> int:
        """Return the incremental start time as a Unix epoch.

        Falls back to ``start_date`` when there is no bookmark yet, because
        ``get_starting_timestamp`` returns None on a config without one.

        Args:
            context: The stream context.

        Returns:
            The start time in seconds since the epoch.
        """
        value = self.get_starting_replication_key_value(context)
        if isinstance(value, (int, float)):
            return int(value)
        if not value:
            value = self.config["start_date"]
        return int(utils.strptime_to_utc(value).timestamp())


class CursorPaginatedStream(ZendeskStream):
    """Stream using Zendesk's cursor pagination (``page[after]`` / ``meta.has_more``)."""

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return the next cursor, or None when the last page has been read.

        Args:
            response: A raw `requests.Response`_ object.
            previous_token: Previous pagination reference.

        Returns:
            The next cursor, or None.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        meta = response.json().get("meta") or {}
        if not meta.get("has_more"):
            return None
        cursor = meta.get("after_cursor")
        # Guard the actual invariant: stop unless the cursor moved.
        if not cursor or cursor == previous_token:
            return None
        return cursor

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, Any] = {"page[size]": self.page_size}
        # The pre-SDK tap mutated one params dict across pages, so filters such
        # as `start_time` were resent on every cursor request. Zendesk needs
        # them, or later pages fall outside the intended window.
        params.update(self.extra_params(context))
        if next_page_token:
            params["page[after]"] = next_page_token
        return params

    def extra_params(self, context: dict | None) -> dict[str, Any]:
        """Return query params to send with every request.

        Args:
            context: The stream context.

        Returns:
            A dictionary of extra URL query parameters.
        """
        return {}


class OffsetPaginatedStream(ZendeskStream):
    """Stream that follows Zendesk's ``next_page`` link."""

    def extra_params(self, context: dict | None) -> dict[str, Any]:
        """Return query params to send with the first request.

        Args:
            context: The stream context.

        Returns:
            A dictionary of extra URL query parameters.
        """
        return {}

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return the query params of the ``next_page`` link, or None when done.

        Args:
            response: A raw `requests.Response`_ object.
            previous_token: Previous pagination reference.

        Returns:
            The next page's query params, or None.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        data = response.json()
        if data.get("end_of_stream"):
            return None
        next_page = data.get("next_page")
        if not next_page:
            return None
        params = dict(parse_qsl(urlsplit(next_page).query))
        # Guard the actual invariant: stop unless the link moved.
        if not params or params == previous_token:
            return None
        return params

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        if next_page_token:
            return dict(next_page_token)
        params: dict[str, Any] = {"per_page": self.page_size}
        params.update(self.extra_params(context))
        return params


class IncrementalExportStream(ZendeskStream):
    """Stream using Zendesk's incremental export endpoints.

    These take a ``start_time`` on the first request and an opaque ``cursor``
    thereafter, and report completion with ``end_of_stream``.
    """

    is_sorted = True

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return the next export cursor, or None at the end of the stream.

        Args:
            response: A raw `requests.Response`_ object.
            previous_token: Previous pagination reference.

        Returns:
            The next cursor, or None.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        data = response.json()
        if data.get("end_of_stream"):
            return None
        cursor = data.get("after_cursor")
        # Guard the actual invariant: stop unless the cursor moved.
        if not cursor or cursor == previous_token:
            return None
        return cursor

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        if next_page_token:
            return {"cursor": next_page_token}
        return {"start_time": self.start_time_epoch(context)}


class CustomFieldsMixin:
    """Adds an account's custom fields to a stream's schema, as the pre-SDK tap did.

    Zendesk lets an account define custom organization and user fields. The
    pre-SDK tap fetched them during discovery and declared a typed property for
    each; without this the catalog carries a single untyped object and a target
    cannot create typed columns for them.

    Enrichment runs during discovery only. For a catalog-driven sync the catalog
    is the source of truth, which is how the pre-SDK tap behaved.
    """

    #: API path listing the custom fields, e.g. ``/organization_fields.json``.
    custom_fields_path: str
    #: Key holding the fields in that response.
    custom_fields_key: str
    #: Record property the fields hang off, e.g. ``organization_fields``.
    custom_fields_property: str

    def fetch_custom_fields(self) -> list[dict]:
        """Return the account's custom fields for this stream.

        Returns:
            The custom field definitions.
        """
        fields: list[dict] = []
        url = self.url_base + self.custom_fields_path
        params: dict[str, Any] | None = {"per_page": self.page_size}
        seen: set[str] = set()
        while url:
            response = self.requests_session.get(
                url,
                params=params,
                headers={**self.http_headers, **self.authenticator.auth_headers},
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
            fields.extend(payload.get(self.custom_fields_key) or [])
            # These endpoints page 100 at a time; an account with more custom
            # fields than that would otherwise lose everything after page one.
            next_page = payload.get("next_page")
            # Guard the actual invariant: stop unless the link moved.
            if not next_page or next_page in seen:
                break
            seen.add(next_page)
            url, params = next_page, None
        return fields

    def merge_custom_fields(self) -> None:
        """Declare a typed property for each of the account's custom fields.

        A credentials or permissions failure leaves the static schema in place
        with a warning rather than failing discovery — the pre-SDK tap did the
        same for accounts whose token lacks the scope.
        """
        try:
            fields = self.fetch_custom_fields()
            # `schema` is a class attribute, so copy before mutating rather than
            # editing the dict that every instance of this stream shares.
            schema = deepcopy(self.schema)
            schema["properties"][self.custom_fields_property]["properties"] = {
                field["key"]: process_custom_field(field) for field in fields
            }
        except Exception:  # noqa: BLE001
            # Inside the guard on purpose: an unmapped field type or a missing
            # schema property should leave the static schema in place, not
            # abort discovery for every stream.
            self.logger.warning(
                "Could not add `%s` custom fields to the schema; the credentials "
                "may lack the scope, or a field type may be unsupported.",
                self.name,
            )
        else:
            self.schema = schema
