"""Unit tests for the logic ported from the pre-SDK tap. No network access."""

from __future__ import annotations

import json
from typing import Any, ClassVar

import pytest

from tap_zendesk.auth import TOKEN_ENDPOINT, token_endpoint_for
from tap_zendesk.client import (
    CursorPaginatedStream,
    IncrementalExportStream,
    OffsetPaginatedStream,
)
from tap_zendesk.schema import load_schema, load_shared_schema_refs, process_custom_field
from tap_zendesk.streams import (
    STREAM_TYPES,
    GroupMembershipsStream,
    TicketCommentsStream,
    TicketsStream,
)
from tap_zendesk.tap import TapZendesk

SAMPLE_CONFIG = {
    "subdomain": "acme",
    "client_id": "placeholder",
    "client_secret": "placeholder",
    "start_date": "2020-01-01T00:00:00Z",
}


class FakeResponse:
    """Minimal stand-in for `requests.Response` carrying a JSON body."""

    request = None
    reason = "Error"

    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.url = "https://acme.zendesk.com/api/v2/tickets"

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            msg = f"{self.status_code} error"
            raise RuntimeError(msg)


def make_tap() -> TapZendesk:
    return TapZendesk(config=SAMPLE_CONFIG, parse_env_config=False)


def get_stream(name: str) -> Any:
    return make_tap().streams[name]


class FakeAuth:
    """Stands in for the OAuth authenticator, which would otherwise refresh."""

    auth_headers: ClassVar[dict] = {}


def stub_http(stream: Any, pages: list[dict]) -> list[str]:
    """Point a stream at canned responses and record the URLs it requests."""
    calls: list[str] = []

    class Session:
        def get(self, url: str, **kwargs: Any) -> FakeResponse:
            calls.append(url)
            return FakeResponse(pages[min(len(calls) - 1, len(pages) - 1)])

    stream.__dict__["authenticator"] = FakeAuth()
    stream._requests_session = Session()
    return calls


# --------------------------------------------------------------------------
# Schema loading
# --------------------------------------------------------------------------


def test_every_stream_schema_loads() -> None:
    """Each ported stream has a schema with properties."""
    tap = make_tap()
    assert len(tap.streams) == len(STREAM_TYPES) == 14
    for stream in tap.streams.values():
        assert stream.schema["properties"], stream.name


def test_shared_refs_are_keyed_by_path() -> None:
    """Shared refs use the `shared/<file>` keys the schemas reference."""
    refs = load_shared_schema_refs()
    assert "shared/attachments.json" in refs
    assert "shared/metadata.json" in refs
    assert "shared/via.json" in refs


def test_cross_file_refs_are_resolved() -> None:
    """`ticket_comments` references shared schemas, which must be inlined."""
    schema = load_schema("ticket_comments")
    assert "$ref" not in json.dumps(schema)
    assert schema["properties"]["attachments"]["items"]["properties"]["id"]


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------


def test_token_endpoint_is_subdomain_scoped() -> None:
    assert token_endpoint_for({"subdomain": "acme"}) == "https://acme.zendesk.com/oauth/tokens"


def test_token_endpoint_requires_subdomain() -> None:
    from hotglue_etl_exceptions import InvalidCredentialsError

    with pytest.raises(InvalidCredentialsError):
        token_endpoint_for({})


def test_capability_probe_does_not_raise() -> None:
    """`access_token_support()` is called with no args by the `--about` probe."""
    authenticator, endpoint = TapZendesk.access_token_support()
    assert endpoint == TOKEN_ENDPOINT
    assert authenticator is not None


# --------------------------------------------------------------------------
# Pagination token math
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "previous", "expected"),
    [
        ({"meta": {"has_more": True, "after_cursor": "c2"}}, "c1", "c2"),
        ({"meta": {"has_more": False, "after_cursor": "c2"}}, "c1", None),
        ({"meta": {}}, None, None),
        ({}, None, None),
        # cursor did not advance: stop rather than request the same page forever
        ({"meta": {"has_more": True, "after_cursor": "c1"}}, "c1", None),
        ({"meta": {"has_more": True, "after_cursor": None}}, "c1", None),
    ],
)
def test_cursor_pagination(payload: dict, previous: Any, expected: Any) -> None:
    stream = get_stream("groups")
    assert isinstance(stream, CursorPaginatedStream)
    assert stream.get_next_page_token(FakeResponse(payload), previous) == expected


def test_cursor_url_params() -> None:
    stream = get_stream("groups")
    assert stream.get_url_params(None, None) == {"page[size]": 100}
    assert stream.get_url_params(None, "abc") == {"page[size]": 100, "page[after]": "abc"}


@pytest.mark.parametrize(
    ("payload", "previous", "expected"),
    [
        (
            {"next_page": "https://acme.zendesk.com/api/v2/x.json?page=2&per_page=100"},
            None,
            {"page": "2", "per_page": "100"},
        ),
        ({"next_page": None}, None, None),
        ({}, None, None),
        ({"next_page": "https://x/y", "end_of_stream": True}, None, None),
        # link did not advance
        (
            {"next_page": "https://acme.zendesk.com/api/v2/x.json?page=2"},
            {"page": "2"},
            None,
        ),
    ],
)
def test_offset_pagination(payload: dict, previous: Any, expected: Any) -> None:
    stream = get_stream("ticket_forms")
    assert isinstance(stream, OffsetPaginatedStream)
    assert stream.get_next_page_token(FakeResponse(payload), previous) == expected


def test_offset_url_params_follow_the_link_verbatim() -> None:
    stream = get_stream("ticket_forms")
    assert stream.get_url_params(None, None) == {"per_page": 100}
    assert stream.get_url_params(None, {"page": "3"}) == {"page": "3"}


@pytest.mark.parametrize(
    ("payload", "previous", "expected"),
    [
        ({"after_cursor": "c2", "end_of_stream": False}, "c1", "c2"),
        ({"after_cursor": "c2", "end_of_stream": True}, "c1", None),
        ({"after_cursor": None, "end_of_stream": False}, "c1", None),
        # cursor did not advance
        ({"after_cursor": "c1", "end_of_stream": False}, "c1", None),
    ],
)
def test_incremental_export_pagination(payload: dict, previous: Any, expected: Any) -> None:
    stream = get_stream("tickets")
    assert isinstance(stream, IncrementalExportStream)
    assert stream.get_next_page_token(FakeResponse(payload), previous) == expected


def test_incremental_export_first_request_uses_start_time() -> None:
    stream = get_stream("tickets")
    params = stream.get_url_params(None, None)
    # 2020-01-01T00:00:00Z
    assert params == {"start_time": 1577836800}
    assert stream.get_url_params(None, "cur") == {"cursor": "cur"}


def test_start_time_falls_back_to_start_date() -> None:
    """`get_starting_timestamp` returns None without a bookmark; don't crash."""
    stream = get_stream("users")
    assert stream.start_time_epoch(None) == 1577836800


def test_start_time_accepts_an_integer_bookmark(monkeypatch: Any) -> None:
    """`tickets` bookmarks an integer epoch, not a timestamp string."""
    stream = get_stream("tickets")
    monkeypatch.setattr(stream, "get_starting_replication_key_value", lambda _ctx: 1688782306)
    assert stream.start_time_epoch(None) == 1688782306


# --------------------------------------------------------------------------
# Record shaping
# --------------------------------------------------------------------------


def test_tickets_strips_the_duplicate_fields_key() -> None:
    """`fields` duplicates `custom_fields`; the pre-SDK tap dropped it."""
    stream = get_stream("tickets")
    custom = [{"id": 7, "value": "x"}]
    row = stream.post_process({"id": 1, "fields": list(custom), "custom_fields": list(custom)})
    assert "fields" not in row
    assert row["custom_fields"] == custom


def test_ticket_comments_are_linked_to_their_ticket() -> None:
    stream = get_stream("ticket_comments")
    row = stream.post_process({"id": 9}, {"ticket_id": 42})
    assert row["ticket_id"] == 42


def test_group_memberships_without_updated_at_are_kept() -> None:
    """The pre-SDK tap synced these anyway, with a log line."""
    stream = get_stream("group_memberships")
    row = stream.post_process({"id": 7, "updated_at": None})
    assert row is not None
    assert row["id"] == 7


def test_group_memberships_without_id_are_skipped() -> None:
    stream = get_stream("group_memberships")
    assert stream.post_process({"id": None, "updated_at": None}) is None


def test_tickets_passes_its_id_to_child_streams() -> None:
    stream = get_stream("tickets")
    assert stream.get_child_context({"id": 5}, None) == {"ticket_id": 5}


# --------------------------------------------------------------------------
# Replication configuration
# --------------------------------------------------------------------------


def test_selecting_children_does_not_force_tickets_to_full_table() -> None:
    """Regression: `ignore_parent_replication_key` nulls the parent's key.

    With it set, the SDK forces FULL_TABLE on `tickets` and every run
    re-syncs every ticket from scratch.
    """
    catalog = make_tap().catalog_dict
    for stream in catalog["streams"]:
        for entry in stream["metadata"]:
            entry["metadata"]["selected"] = True
    tap = TapZendesk(config=SAMPLE_CONFIG, catalog=catalog, parse_env_config=False)
    assert tap.streams["ticket_audits"].selected
    tap._set_compatible_replication_methods()
    tickets = tap.streams["tickets"]
    assert tickets.replication_key == "generated_timestamp"
    assert tickets.replication_method == "INCREMENTAL"


def test_primary_and_replication_keys_match_the_legacy_tap() -> None:
    expected = {
        "tickets": (["id"], "generated_timestamp"),
        "users": (["id"], "updated_at"),
        "organizations": (["id"], "updated_at"),
        "groups": (["id"], "updated_at"),
        "group_memberships": (["id"], "updated_at"),
        "macros": (["id"], "updated_at"),
        "ticket_fields": (["id"], "updated_at"),
        "ticket_forms": (["id"], "updated_at"),
        "satisfaction_ratings": (["id"], "updated_at"),
        "ticket_comments": (["id"], "created_at"),
        "tags": (["name"], None),
        "sla_policies": (["id"], None),
        "ticket_audits": (["id"], None),
        "ticket_metrics": (["id"], None),
    }
    tap = make_tap()
    actual = {
        name: (stream.primary_keys, stream.replication_key) for name, stream in tap.streams.items()
    }
    assert actual == expected


def test_ticket_children_hang_off_tickets() -> None:
    tap = make_tap()
    for name in ("ticket_audits", "ticket_metrics", "ticket_comments"):
        assert tap.streams[name].parent_stream_type is TicketsStream


def test_url_base_is_built_from_the_subdomain() -> None:
    assert get_stream("tickets").url_base == "https://acme.zendesk.com/api/v2"


# --------------------------------------------------------------------------
# Error reporting
# --------------------------------------------------------------------------


def test_error_message_includes_zendesk_text() -> None:
    stream = get_stream("tickets")

    resp = FakeResponse({"error": "RecordInvalid"}, status_code=400)
    assert "RecordInvalid" in stream.response_error_message(resp)


def test_error_message_survives_a_non_json_body() -> None:
    stream = get_stream("tickets")

    class Resp:
        status_code = 500
        url = "https://acme.zendesk.com/api/v2/tickets"
        request = None
        reason = "Server Error"

        def json(self) -> dict:
            raise ValueError

    assert isinstance(stream.response_error_message(Resp()), str)


def test_marketplace_headers_need_every_setting() -> None:
    tap = TapZendesk(config=SAMPLE_CONFIG, parse_env_config=False)
    assert "X-Zendesk-Marketplace-Name" not in tap.streams["tickets"].http_headers

    full = {
        **SAMPLE_CONFIG,
        "marketplace_name": "acme",
        "marketplace_organization": "12",
        "marketplace_app_id": "34",
    }
    headers = TapZendesk(config=full, parse_env_config=False).streams["tickets"].http_headers
    assert headers["X-Zendesk-Marketplace-Name"] == "acme"
    assert headers["X-Zendesk-Marketplace-Organization-Id"] == "12"
    assert headers["X-Zendesk-Marketplace-App-Id"] == "34"


def _unused(_: TicketCommentsStream, __: GroupMembershipsStream) -> None:
    """Keep the direct stream imports referenced for linting."""


def test_ticket_audit_records_are_transformed_like_the_legacy_tap() -> None:
    """Schema drift: chat events return an object, voice a numeric duration.

    Both are coerced to the declared type, and undeclared fields dropped,
    exactly as singer's Transformer did in the pre-SDK tap.
    """
    stream = get_stream("ticket_audits")
    row = stream.post_process(
        {
            "id": 1,
            "ticket_id": 2,
            "events": [
                {"id": 3, "value": {"chat_id": "abc", "is_served": True}},
                {"id": 4, "value": "already a string"},
                {"id": 5, "data": {"call_duration": 41}},
            ],
        },
    )
    assert row["events"][0]["value"] == "{'chat_id': 'abc', 'is_served': True}"
    assert row["events"][1]["value"] == "already a string"
    assert row["events"][2]["data"]["call_duration"] == "41"


# --------------------------------------------------------------------------
# Parity with the pre-SDK tap
# --------------------------------------------------------------------------


def test_every_stream_runs_the_transformer() -> None:
    """Tap-wide, so output stays byte-identical to the pre-SDK tap.

    The Transformer normalises datetimes to singer's spelling and drops fields
    the schemas do not declare.
    """
    stream = get_stream("groups")
    row = stream.post_process(
        {"id": 1, "updated_at": "2023-01-24T05:44:25Z", "not_in_the_schema": "x"},
    )
    assert row["updated_at"] == "2023-01-24T05:44:25.000000Z"
    assert "not_in_the_schema" not in row


def test_ticket_comments_below_the_cutoff_are_dropped() -> None:
    """Reproduces the pre-SDK tap's comment filter.

    The pre-SDK tap snapshotted the tickets bookmark once, when the first
    comment of the run was processed, and used it as the cutoff for every
    ticket. Comments at or below it were never emitted.
    """
    tap = make_tap()
    tickets = tap.streams["tickets"]
    comments = tap.streams["ticket_comments"]
    # 2023-07-08T02:11:46Z, the first ticket's generated_timestamp
    tickets.get_child_context({"id": 3, "generated_timestamp": 1688782306}, None)

    ctx = {"ticket_id": 3}
    assert comments.post_process({"id": 1, "created_at": "2023-07-08T02:10:18Z"}, ctx) is None
    assert comments.post_process({"id": 2, "created_at": "2023-07-08T02:11:46Z"}, ctx) is None
    kept = comments.post_process({"id": 3, "created_at": "2023-07-08T02:11:47Z"}, ctx)
    assert kept is not None
    assert kept["ticket_id"] == 3


def test_ticket_comment_cutoff_is_shared_across_tickets() -> None:
    """One snapshot, reused for every ticket - as the pre-SDK tap did."""
    tap = make_tap()
    tickets = tap.streams["tickets"]
    comments = tap.streams["ticket_comments"]
    tickets.get_child_context({"id": 3, "generated_timestamp": 1688782306}, None)
    comments.post_process({"id": 1, "created_at": "2023-07-08T02:11:47Z"}, {"ticket_id": 3})

    # a later ticket is still measured against the first ticket's snapshot
    tickets.get_child_context({"id": 9, "generated_timestamp": 1900000000}, None)
    assert (
        comments.post_process({"id": 2, "created_at": "2023-01-24T05:44:28Z"}, {"ticket_id": 1})
        is None
    )


# --------------------------------------------------------------------------
# request_timeout (ported from the pre-SDK tap)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        (None, 300),
        (600, 600),
        ("600", 600),
        # falsy values fall back to the default, as the pre-SDK tap did
        (0, 300),
        ("0", 300),
        ("", 300),
    ],
)
def test_request_timeout(configured: Any, expected: int) -> None:
    config = dict(SAMPLE_CONFIG)
    if configured is not None:
        config["request_timeout"] = configured
    tap = TapZendesk(config=config, parse_env_config=False)
    assert tap.streams["tickets"].timeout == expected


# --------------------------------------------------------------------------
# Custom fields (ported from the pre-SDK tap's `_add_custom_fields`)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ({"type": "text"}, {"type": ["string", "null"]}),
        ({"type": "textarea"}, {"type": ["string", "null"]}),
        ({"type": "regexp"}, {"type": ["string", "null"]}),
        ({"type": "lookup"}, {"type": ["string", "null"]}),
        ({"type": "integer"}, {"type": ["integer", "null"]}),
        ({"type": "decimal"}, {"type": ["number", "null"]}),
        ({"type": "checkbox"}, {"type": ["boolean", "null"]}),
        ({"type": "date"}, {"type": ["string", "null"], "format": "datetime"}),
        (
            {"type": "dropdown", "custom_field_options": [{"value": "a"}, {"value": "b"}]},
            {"type": ["string", "null"], "enum": ["a", "b"]},
        ),
    ],
)
def test_process_custom_field(field: dict, expected: dict) -> None:
    assert process_custom_field({"key": "k", "title": "T", **field}) == expected


def test_process_custom_field_rejects_an_unknown_type() -> None:
    with pytest.raises(ValueError, match="unsupported type"):
        process_custom_field({"key": "k", "title": "T", "type": "quantum"})


@pytest.mark.parametrize(
    ("stream_name", "prop"),
    [("organizations", "organization_fields"), ("users", "user_fields")],
)
def test_custom_fields_are_declared_on_the_schema(stream_name: str, prop: str) -> None:
    tap = make_tap()
    stream = tap.streams[stream_name]
    stream.fetch_custom_fields = lambda: [  # type: ignore[method-assign]
        {"key": "seats", "type": "integer", "title": "Seats"},
    ]
    stream.merge_custom_fields()
    assert stream.schema["properties"][prop]["properties"] == {
        "seats": {"type": ["integer", "null"]},
    }


def test_custom_fields_do_not_mutate_the_shared_class_schema() -> None:
    """`schema` is a class attribute; enrichment must not leak between taps."""
    first = make_tap().streams["organizations"]
    first.fetch_custom_fields = lambda: [  # type: ignore[method-assign]
        {"key": "seats", "type": "integer", "title": "Seats"},
    ]
    first.merge_custom_fields()

    second = make_tap().streams["organizations"]
    assert "properties" not in second.schema["properties"]["organization_fields"]


def test_missing_custom_field_scope_leaves_the_schema_alone() -> None:
    """The pre-SDK tap warned and carried on when the token lacked the scope."""
    stream = make_tap().streams["organizations"]

    def boom() -> list[dict]:
        msg = "403 Forbidden"
        raise RuntimeError(msg)

    stream.fetch_custom_fields = boom  # type: ignore[method-assign]
    stream.merge_custom_fields()
    assert "properties" not in stream.schema["properties"]["organization_fields"]


def test_custom_field_values_survive_the_transformer() -> None:
    """`additionalProperties: true` keeps values even without the typed schema."""
    stream = get_stream("organizations")
    row = stream.post_process(
        {"id": 1, "updated_at": "2023-01-01T00:00:00Z", "organization_fields": {"seats": 5}},
    )
    assert row["organization_fields"] == {"seats": 5}


# --------------------------------------------------------------------------
# Child-stream 404 handling
# --------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["ticket_audits", "ticket_metrics", "ticket_comments"])
def test_child_stream_skips_a_missing_ticket(name: str) -> None:
    """A 404 for one ticket must skip it, not fail the run."""
    stream = get_stream(name)
    resp = FakeResponse({}, status_code=404)
    stream.validate_response(resp)  # must not raise
    assert list(stream.parse_response(resp)) == []


@pytest.mark.parametrize("name", ["ticket_audits", "ticket_metrics", "ticket_comments"])
def test_child_stream_still_raises_on_other_errors(name: str) -> None:
    stream = get_stream(name)
    with pytest.raises(Exception, match="boom"):
        stream.validate_response(FakeResponse({"error": "boom"}, status_code=500))


# --------------------------------------------------------------------------
# Review findings
# --------------------------------------------------------------------------


def test_multiselect_custom_field() -> None:
    """Zendesk user/org fields can be multiselect; the pre-SDK map had no entry."""
    assert process_custom_field(
        {
            "key": "k",
            "title": "T",
            "type": "multiselect",
            "custom_field_options": [{"value": "a"}, {"value": "b"}],
        },
    ) == {"type": ["array", "null"], "items": {"type": "string", "enum": ["a", "b"]}}


def test_an_unmappable_field_type_does_not_abort_discovery() -> None:
    """A future Zendesk type must not take the whole catalog down with it."""
    stream = make_tap().streams["organizations"]
    stream.fetch_custom_fields = lambda: [  # type: ignore[method-assign]
        {"key": "k", "title": "T", "type": "some_future_type"},
    ]
    stream.merge_custom_fields()
    assert "properties" not in stream.schema["properties"]["organization_fields"]


def test_custom_fields_are_paginated() -> None:
    """These endpoints page 100 at a time; page two must not be dropped."""
    stream = make_tap().streams["users"]
    calls = stub_http(
        stream,
        [
            {"user_fields": [{"key": "a", "type": "text", "title": "A"}], "next_page": "p2"},
            {"user_fields": [{"key": "b", "type": "text", "title": "B"}], "next_page": None},
        ],
    )
    stream.merge_custom_fields()
    assert len(calls) == 2
    assert sorted(stream.schema["properties"]["user_fields"]["properties"]) == ["a", "b"]


def test_custom_field_pagination_stops_on_a_repeated_link() -> None:
    """Guard the invariant: stop unless the link moved."""
    stream = make_tap().streams["users"]
    calls = stub_http(stream, [{"user_fields": [], "next_page": "same"}])
    stream.merge_custom_fields()
    assert len(calls) == 2


def test_group_membership_without_updated_at_does_not_break_state() -> None:
    """These records are emitted; the SDK would raise KeyError bookmarking them."""
    stream = make_tap().streams["group_memberships"]
    stream._increment_stream_state({"id": 9, "updated_at": "2024-01-01T00:00:00Z"})
    before = json.dumps(stream.stream_state)
    stream._increment_stream_state({"id": 7})  # must not raise
    assert json.dumps(stream.stream_state) == before


def test_satisfaction_ratings_sends_start_time_on_every_page() -> None:
    """Zendesk needs the original filters alongside the cursor."""
    stream = get_stream("satisfaction_ratings")
    first = stream.get_url_params(None, None)
    later = stream.get_url_params(None, "CURSOR")
    assert first["start_time"] == later["start_time"]
    assert later["page[after]"] == "CURSOR"


def test_streams_without_extra_params_are_unaffected() -> None:
    stream = get_stream("groups")
    assert stream.get_url_params(None, "CURSOR") == {
        "page[size]": 100,
        "page[after]": "CURSOR",
    }
