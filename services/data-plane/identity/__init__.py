"""identity — Phase-03 Step I: the one identity field this system's real
design treats as always-present and never optional.

dataplane.proto's Identity message has four plain (non-optional) string
fields: org_id, user_id, project_id, conversation_id. Proto3 gives no way
to distinguish "not sent" from "" for a non-optional scalar, so presence
here means non-empty. Only org_id is validated: edge-gateway's own
Identity struct (normalize.rs) never wraps org_id in Option — it's always
a real UUID — while user_id/project_id ARE legitimately Option<Uuid>
there (e.g. an API-key-only auth flow that resolves no specific
user/project) and arrive here as "" via unwrap_or_default() on entirely
normal, legitimate traffic. Requiring those to be non-empty too would
reject real requests edge-gateway is designed to send. org_id is also
the one value every downstream write depends on: usage_event.org_id and
request_log.org_id are both `NOT NULL REFERENCES tenants (org_id)` — a
request with no org_id has no tenant to durably attribute anything to,
which is exactly why the reject path (main.py) never writes a
request_log/usage_event row for this case at all, not even under a
default/placeholder tenant.
"""


class MissingIdentity(Exception):
    def __init__(self) -> None:
        super().__init__("request missing required org_id")


def validate(org_id: str) -> None:
    if not org_id:
        raise MissingIdentity()
