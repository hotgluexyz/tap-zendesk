"""Stream type classes for tap-zendesk."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, ClassVar

from hotglue_singer_sdk.streams import RESTStream
from singer import utils
from typing_extensions import override

from tap_zendesk.client import (
    CursorPaginatedStream,
    CustomFieldsMixin,
    IncrementalExportStream,
    OffsetPaginatedStream,
)
from tap_zendesk.schema import load_schema

# Sub-streams fetched per ticket. A missing ticket yields a 404 that should skip
# the sub-record rather than fail the run, matching the pre-SDK tap.
TICKET_CHILD_NOT_FOUND = 404


class TicketsStream(IncrementalExportStream):
    """Stream for ``tickets``."""

    name = "tickets"
    current_generated_timestamp: int | None = None
    path = "/incremental/tickets/cursor.json"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "generated_timestamp"
    records_jsonpath = "$.tickets[*]"
    schema = load_schema("tickets")

    @override
    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return the context passed to the per-ticket sub-streams.

        Args:
            record: The ticket record.
            context: The stream context.

        Returns:
            A context dict carrying the ticket id.
        """
        # The pre-SDK tap advanced the tickets bookmark before syncing a
        # ticket's children; the SDK syncs children first, so `ticket_comments`
        # cannot read the value off the parent's state. Expose it here instead.
        # It is kept out of the context dict because every context key becomes
        # a state partition key.
        self.current_generated_timestamp = record.get("generated_timestamp")
        return {"ticket_id": record["id"]}

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Drop the duplicate ``fields`` key, as the pre-SDK tap did.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary.
        """
        # NB: `fields` is a duplicate of `custom_fields`, removed before emitting.
        row.pop("fields", None)
        return super().post_process(row, context)


class UsersStream(CustomFieldsMixin, IncrementalExportStream):
    """Stream for ``users``."""

    name = "users"
    custom_fields_path = "/user_fields.json"
    custom_fields_key = "user_fields"
    custom_fields_property = "user_fields"
    path = "/incremental/users/cursor.json"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.users[*]"
    schema = load_schema("users")


class OrganizationsStream(CustomFieldsMixin, OffsetPaginatedStream):
    """Stream for ``organizations``."""

    name = "organizations"
    custom_fields_path = "/organization_fields.json"
    custom_fields_key = "organization_fields"
    custom_fields_property = "organization_fields"
    path = "/incremental/organizations.json"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.organizations[*]"
    schema = load_schema("organizations")

    @override
    def extra_params(self, context: dict | None) -> dict[str, Any]:
        """Send the incremental start time on the first request.

        Args:
            context: The stream context.

        Returns:
            A dictionary of extra URL query parameters.
        """
        return {"start_time": self.start_time_epoch(context)}


class GroupsStream(CursorPaginatedStream):
    """Stream for ``groups``."""

    name = "groups"
    path = "/groups"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.groups[*]"
    schema = load_schema("groups")


class MacrosStream(CursorPaginatedStream):
    """Stream for ``macros``."""

    name = "macros"
    path = "/macros"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.macros[*]"
    schema = load_schema("macros")


class TagsStream(CursorPaginatedStream):
    """Stream for ``tags``."""

    name = "tags"
    path = "/tags"
    primary_keys: ClassVar[list[str]] = ["name"]
    replication_key = None
    records_jsonpath = "$.tags[*]"
    schema = load_schema("tags")


class TicketFieldsStream(CursorPaginatedStream):
    """Stream for ``ticket_fields``."""

    name = "ticket_fields"
    path = "/ticket_fields"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.ticket_fields[*]"
    schema = load_schema("ticket_fields")


class GroupMembershipsStream(CursorPaginatedStream):
    """Stream for ``group_memberships``."""

    name = "group_memberships"
    path = "/group_memberships"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.group_memberships[*]"
    schema = load_schema("group_memberships")

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Keep memberships that arrive without an ``updated_at``.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The record, or None to skip it.
        """
        if not row.get("updated_at"):
            if not row.get("id"):
                self.logger.info(
                    "Received group_membership record with no id or updated_at, skipping...",
                )
                return None
            self.logger.info(
                "group_membership record with id: %s does not have an updated_at field "
                "so it will be syncd...",
                row["id"],
            )
        return super().post_process(row, context)


class SatisfactionRatingsStream(CursorPaginatedStream):
    """Stream for ``satisfaction_ratings``."""

    name = "satisfaction_ratings"
    path = "/satisfaction_ratings"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.satisfaction_ratings[*]"
    schema = load_schema("satisfaction_ratings")

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Filter server-side with ``start_time``, as the pre-SDK tap did.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params = super().get_url_params(context, next_page_token)
        if not next_page_token:
            params["start_time"] = self.start_time_epoch(context)
        return params


class TicketFormsStream(OffsetPaginatedStream):
    """Stream for ``ticket_forms``."""

    name = "ticket_forms"
    path = "/ticket_forms.json"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.ticket_forms[*]"
    schema = load_schema("ticket_forms")


class SLAPoliciesStream(OffsetPaginatedStream):
    """Stream for ``sla_policies``."""

    name = "sla_policies"
    path = "/slas/policies.json"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.sla_policies[*]"
    schema = load_schema("sla_policies")


class TicketChildStream(OffsetPaginatedStream):
    """Base for the sub-streams fetched once per ticket.

    These hang off separate per-ticket endpoints, so `parent_stream_type` costs
    exactly the same number of requests the pre-SDK tap made.
    """

    parent_stream_type = TicketsStream
    # NB: leave `ignore_parent_replication_key` at its default of False. Setting
    # it True makes the SDK null the parent's replication key and force
    # FULL_TABLE on `tickets`, so every run would re-sync every ticket.
    state_partitioning_keys: ClassVar[list[str]] = []
    _schema_written = False

    @override
    def _write_schema_message(self) -> None:
        """Emit SCHEMA once, not once per parent ticket."""
        if self._schema_written:
            return
        super()._write_schema_message()
        self._schema_written = True

    @override
    def validate_response(self, response: Any) -> None:
        """Skip sub-records for tickets the API reports as missing.

        Args:
            response: A raw `requests.Response`_ object.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        if response.status_code == TICKET_CHILD_NOT_FOUND:
            self.logger.warning(
                "Unable to retrieve %s for ticket, record not found: %s",
                self.name,
                response.url,
            )
            return
        super().validate_response(response)

    @override
    def parse_response(self, response: Any) -> Any:
        """Yield no records for a skipped 404 response.

        Args:
            response: A raw `requests.Response`_ object.

        Returns:
            An iterable of records.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        if response.status_code == TICKET_CHILD_NOT_FOUND:
            return iter(())
        return super().parse_response(response)


class TicketAuditsStream(TicketChildStream):
    """Stream for ``ticket_audits``."""

    name = "ticket_audits"
    path = "/tickets/{ticket_id}/audits.json"
    primary_keys: ClassVar[list[str]] = ["id"]
    # No replication key: incrementality comes from which tickets the parent
    # yields. The pre-SDK tap labelled this INCREMENTAL with no key, which the
    # SDK rejects at sync time, so it is reported as FULL_TABLE.
    replication_key = None
    records_jsonpath = "$.audits[*]"
    schema = load_schema("ticket_audits")


class TicketMetricsStream(TicketChildStream):
    """Stream for ``ticket_metrics``."""

    name = "ticket_metrics"
    path = "/tickets/{ticket_id}/metrics"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None
    # Only one ticket metric per ticket, returned as an object rather than a list.
    records_jsonpath = "$.ticket_metric"
    schema = load_schema("ticket_metrics")


class TicketCommentsStream(TicketChildStream):
    """Stream for ``ticket_comments``."""

    name = "ticket_comments"
    path = "/tickets/{ticket_id}/comments.json"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "created_at"
    records_jsonpath = "$.comments[*]"
    schema = load_schema("ticket_comments")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._floors: dict[Any, datetime] = {}
        self._parent_snapshot: Any = None

    def _floor(self, context: dict | None) -> datetime:
        """Return the cutoff a comment must be newer than to be emitted.

        Mirrors the pre-SDK tap: the per-ticket comment bookmark when there is
        one, otherwise a single snapshot of the `tickets` bookmark taken when
        the first comment of the run is processed, otherwise `start_date`. The
        snapshot is deliberately taken once and shared across tickets, because
        that is what the pre-SDK tap did.

        Args:
            context: The stream context.

        Returns:
            The cutoff as a timezone-aware datetime.
        """
        ticket_id = (context or {}).get("ticket_id")
        if ticket_id in self._floors:
            return self._floors[ticket_id]

        # NB: not `get_starting_replication_key_value`, which the SDK seeds from
        # `start_date` and so is never None. Only a real bookmark carried in from
        # a previous run should take precedence over the parent snapshot.
        value = self.get_context_state(context).get("replication_key_value")
        if value is None:
            if self._parent_snapshot is None:
                parent = self._tap.streams[TicketsStream.name]
                self._parent_snapshot = parent.current_generated_timestamp
            value = self._parent_snapshot
        if value is None:
            value = self.config["start_date"]

        floor = (
            datetime.fromtimestamp(value, tz=timezone.utc)
            if isinstance(value, (int, float))
            else utils.strptime_to_utc(value)
        )
        self._floors[ticket_id] = floor
        return floor

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Link the comment to its ticket, dropping ones at or below the cutoff.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record, or None to skip it.
        """
        if context:
            row["ticket_id"] = context["ticket_id"]
        created_at = row.get("created_at")
        if created_at and utils.strptime_to_utc(created_at) <= self._floor(context):
            return None
        return super().post_process(row, context)


STREAM_TYPES: list[type[RESTStream]] = [
    TicketsStream,
    TicketAuditsStream,
    TicketMetricsStream,
    TicketCommentsStream,
    UsersStream,
    OrganizationsStream,
    GroupsStream,
    GroupMembershipsStream,
    MacrosStream,
    TagsStream,
    TicketFieldsStream,
    TicketFormsStream,
    SatisfactionRatingsStream,
    SLAPoliciesStream,
]
