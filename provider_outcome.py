"""
Provider Outcome — lossless external send semantics
====================================================

The execution layer must never collapse provider uncertainty into a
retry-safe state.  A boolean cannot express the difference between

    "Mailchimp rejected the request"        (safe to retry)
    "we never heard back"                   (MUST NOT retry)

so this module replaces the boolean with a three-valued outcome plus
the evidence that justified it.

    CONFIRMED_SENT    provider proved the send was accepted
    DEFINITE_FAILURE  provider proved the send was rejected
    AMBIGUOUS         anything else — the send may have happened

Only DEFINITE_FAILURE may release a claim and unlock a retry.

-----------------------------------------------------------------------
DOCUMENTED PROVIDER FACTS (Mailchimp Marketing API v3.0.91)
-----------------------------------------------------------------------
Source: https://mailchimp.com/developer/marketing/api/campaigns/
        get-campaign-info/  (retrieved 2026-09-14)

Campaign `status` field — verbatim from the API reference:

    "The current status of the campaign. Possible values:
     'save', 'paused', 'schedule', 'sending', 'sent',
     'canceled', 'canceling', or 'archived'."

Send action — verbatim:

    POST /campaigns/{campaign_id}/actions/send
    "Send a Mailchimp campaign. For RSS Campaigns, the campaign will
     send according to its schedule. All other campaigns will send
     immediately."

Cancel action — verbatim, and decisive for safety:

    POST /campaigns/{campaign_id}/actions/cancel-send
    "Cancel a Regular or Plain-Text Campaign AFTER YOU SEND, before all
     of your recipients receive it."

That last sentence is why `canceled` and `canceling` are NOT safe to
retry: a campaign only reaches those states once delivery has already
begun, so some recipients have already received the email.  Retrying
would send them a second copy.

Any naive `status != "sent" => safe to retry` rule silently treats
'sending', 'canceling', 'canceled', 'paused', 'schedule', 'archived'
and every unrecognised/missing value as retry-safe.  Four of those
mean mail is in flight or already delivered.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


# ─────────────────────────────────────────────────────────
# Typed provider exceptions
# ─────────────────────────────────────────────────────────

class ProviderError(Exception):
    """Base class for provider interaction failures."""


class ProviderTransportError(ProviderError):
    """The request was dispatched but no usable response came back.

    Timeout, connection reset, socket close, DNS failure mid-flight,
    or any unrecognised HTTP-client exception.

    CRITICAL: this proves NOTHING about whether the provider acted on
    the request.  It must never be interpreted as a rejection.
    """

    def __init__(self, message: str, error_type: str = "transport_error"):
        super().__init__(message)
        self.error_type = error_type


class ProviderHTTPError(ProviderError):
    """The provider returned an HTTP response with a non-2xx status.

    Carries the status code so callers can decide — per operation —
    whether that specific code proves rejection.
    """

    def __init__(self, message: str, http_status: int,
                 body: Optional[Dict[str, Any]] = None,
                 detail: str = ""):
        super().__init__(message)
        self.http_status = http_status
        self.body = body or {}
        self.detail = detail


class ProviderMalformedResponseError(ProviderError):
    """A 2xx response arrived but its body could not be parsed.

    The provider accepted the request (2xx), so this leans toward
    "probably acted on it" — and is therefore AMBIGUOUS, never a
    definite failure.
    """

    def __init__(self, message: str, http_status: int):
        super().__init__(message)
        self.http_status = http_status


@dataclass
class ProviderResponse:
    """A successful (2xx) provider response.

    `body` is {} for 204 No Content — that is a success, not an error.
    """
    http_status: int
    body: Dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────
# Send outcome
# ─────────────────────────────────────────────────────────

class ProviderSendStatus(str, Enum):
    CONFIRMED_SENT = "confirmed_sent"
    DEFINITE_FAILURE = "definite_failure"
    AMBIGUOUS = "ambiguous"


@dataclass
class ProviderSendOutcome:
    """Outcome of a send request, with the evidence behind it."""
    status: ProviderSendStatus
    http_status: Optional[int] = None
    provider_campaign_id: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    detail: Optional[str] = None

    @property
    def is_confirmed_sent(self) -> bool:
        return self.status == ProviderSendStatus.CONFIRMED_SENT

    @property
    def is_definite_failure(self) -> bool:
        return self.status == ProviderSendStatus.DEFINITE_FAILURE

    @property
    def is_ambiguous(self) -> bool:
        return self.status == ProviderSendStatus.AMBIGUOUS

    def to_dict(self) -> Dict[str, Any]:
        """Log/API-safe view. Never contains credentials or recipients."""
        return {
            "status": self.status.value,
            "http_status": self.http_status,
            "provider_campaign_id": self.provider_campaign_id,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "detail": self.detail,
        }


# HTTP status codes that PROVE the send request was rejected before the
# provider acted on it.  Deliberately narrow: a code only belongs here
# if a rejection is the sole possible interpretation.
#
#   400 malformed request      — never processed
#   401 unauthenticated        — rejected at the edge
#   403 forbidden              — rejected at the edge
#   404 campaign not found     — nothing existed to send
#   405 method not allowed     — never routed to the send handler
#   422 validation failure     — Mailchimp's documented validation reject
#
# Everything else — every 5xx, plus 408 / 409 / 429 and any code not
# listed — is AMBIGUOUS.  A 500 can be returned after the send was
# already queued, so it cannot clear a retry.
DEFINITE_SEND_REJECTION_CODES = frozenset({400, 401, 403, 404, 405, 422})


def classify_send_http_error(
    http_status: int,
    provider_campaign_id: Optional[str] = None,
    detail: str = "",
) -> ProviderSendOutcome:
    """Classify a non-2xx response to the send action.

    Definite failure only for codes that prove pre-processing rejection.
    """
    if http_status in DEFINITE_SEND_REJECTION_CODES:
        return ProviderSendOutcome(
            status=ProviderSendStatus.DEFINITE_FAILURE,
            http_status=http_status,
            provider_campaign_id=provider_campaign_id,
            error_type="provider_rejected",
            error_message=(
                f"Mailchimp rejected the send with HTTP {http_status}; "
                f"the request was not processed."
            ),
            detail=detail,
        )

    return ProviderSendOutcome(
        status=ProviderSendStatus.AMBIGUOUS,
        http_status=http_status,
        provider_campaign_id=provider_campaign_id,
        error_type="provider_http_uncertain",
        error_message=(
            f"Mailchimp returned HTTP {http_status}, which does not prove "
            f"the send was rejected. Treating as ambiguous."
        ),
        detail=detail,
    )


# ─────────────────────────────────────────────────────────
# Reconciliation classification
# ─────────────────────────────────────────────────────────

class ReconciliationVerdict(str, Enum):
    SENT = "sent"
    DEFINITELY_NOT_SENT = "definitely_not_sent"
    UNKNOWN = "unknown"


# Statuses that PROVE the campaign was sent.
RECONCILE_SENT_STATUSES = frozenset({"sent"})

# Statuses that PROVE the campaign was never dispatched.
#
# Only 'save' qualifies.  It is the draft state: the campaign exists but
# no send action has been accepted for it.  Every other documented value
# either means mail is moving ('sending'), already moved ('sent',
# 'canceling', 'canceled' — see the cancel-endpoint wording above), or
# has send-state semantics we cannot prove ('paused', 'schedule',
# 'archived').
#
# 'schedule' deserves a specific note: a scheduled campaign will send
# later on its own.  Treating it as "not sent, safe to retry" risks BOTH
# the scheduled delivery and our retry landing in the same inbox.
RECONCILE_NOT_SENT_STATUSES = frozenset({"save"})

# Documented but deliberately unsafe to act on. Listed explicitly so the
# intent is auditable rather than implied by an `else`.
RECONCILE_KNOWN_UNSAFE_STATUSES = frozenset({
    "sending",    # delivery in progress right now
    "canceling",  # cancel requested AFTER send began
    "canceled",   # send began, then cancelled — partial delivery
    "paused",     # pause semantics mid-send are not proven
    "schedule",   # will send later on its own
    "archived",   # orthogonal to send state; a sent campaign can be archived
})


def classify_reconciliation_status(
    provider_status: Optional[str],
    emails_sent: Optional[int] = None,
    send_time: Optional[str] = None,
) -> ReconciliationVerdict:
    """Classify a campaign status into a send verdict.

    Allowlist-based: a status must appear in an explicit safe set to
    produce a decisive verdict.  Unknown, missing, empty, null and any
    value outside the documented enum all return UNKNOWN, which keeps
    the attempt ambiguous and blocks retry.

    `emails_sent` and `send_time` are corroborating evidence: a 'save'
    campaign that nonetheless reports delivered mail is contradictory,
    so we refuse to call it not-sent.
    """
    if provider_status is None:
        return ReconciliationVerdict.UNKNOWN

    normalized = str(provider_status).strip().lower()
    if not normalized:
        return ReconciliationVerdict.UNKNOWN

    if normalized in RECONCILE_SENT_STATUSES:
        return ReconciliationVerdict.SENT

    if normalized in RECONCILE_NOT_SENT_STATUSES:
        # Defence in depth: contradictory evidence downgrades to UNKNOWN
        # rather than clearing the retry.
        if emails_sent is not None and emails_sent > 0:
            return ReconciliationVerdict.UNKNOWN
        if send_time:
            return ReconciliationVerdict.UNKNOWN
        return ReconciliationVerdict.DEFINITELY_NOT_SENT

    return ReconciliationVerdict.UNKNOWN


# ─────────────────────────────────────────────────────────
# Campaign creation outcome
# ─────────────────────────────────────────────────────────

class ProviderCreateStatus(str, Enum):
    CREATED = "created"
    DEFINITE_FAILURE = "definite_failure"
    AMBIGUOUS = "ambiguous"


@dataclass
class ProviderCreateOutcome:
    """Outcome of campaign creation.

    AMBIGUOUS here does not risk a duplicate customer email on its own —
    an un-sent campaign harms nobody — but it can orphan a campaign at
    the provider, and more importantly it means we may hold no
    provider_campaign_id for a campaign that exists.  Without that ID we
    cannot reconcile later, so the attempt must not silently become
    retryable.
    """
    status: ProviderCreateStatus
    provider_campaign_id: Optional[str] = None
    http_status: Optional[int] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    content_set: bool = False

    @property
    def is_created(self) -> bool:
        return self.status == ProviderCreateStatus.CREATED

    @property
    def is_ambiguous(self) -> bool:
        return self.status == ProviderCreateStatus.AMBIGUOUS

    @property
    def is_definite_failure(self) -> bool:
        return self.status == ProviderCreateStatus.DEFINITE_FAILURE

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "provider_campaign_id": self.provider_campaign_id,
            "http_status": self.http_status,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "content_set": self.content_set,
        }
