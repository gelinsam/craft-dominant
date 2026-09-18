"""Regression tests for campaign-name tokenisation and the SEACF alias.

Two defects found by running the read-only assignment diagnostic against the
real production ad accounts (1,244 campaigns), before any spend was ingested:

1. Punctuation was DELETED during normalisation instead of being replaced with
   a space, so "Instagram post:DCCF ..." became the single token "postdccf".
   The 'dccf' alias stopped being a whole word and both the abbreviation and
   reverse-alias strategies missed it. The same campaign written with a space
   after the colon matched normally, so attribution depended on typing.

2. 'SEACF' is used in the live account but was absent from EVENT_ALIASES,
   alongside the 'sea' and 'scf' variants that were already present.

Both failed in the safe direction (no match, spend skipped) rather than
misattributing, but both silently understate spend, which deflates CAC and
makes paid media look better than it is.
"""

import pytest

from craft_unified import MetaAdsSync


@pytest.fixture
def sync():
    return MetaAdsSync('token', 'act_test', db=None)


# --------------------------------------------------------------------------
# Defect 1: punctuation must separate tokens, not fuse them
# --------------------------------------------------------------------------

PUNCT_ADJACENT = [
    ("Instagram post:DCCF  Less Labor ~ More Fun! Secure your spot",
     "DC Coffee Festival 2026"),
    ("Instagram post:ACF  Hot day, cold brew", "Austin Coffee Festival 2026"),
    ("Instagram post:SEA  Brewing up an incredible coffee weekend",
     "Seattle Coffee Festival 2026"),
    ("Instagram post:PCF Tickets now live", "Philly Coffee Festival 2026"),
]


@pytest.mark.parametrize("campaign,event", PUNCT_ADJACENT)
def test_alias_against_punctuation_still_matches(sync, campaign, event):
    """An alias glued to a delimiter must still be found."""
    assert sync._campaign_matches_event(campaign, event) is not None


@pytest.mark.parametrize("campaign,event", PUNCT_ADJACENT)
def test_spaced_and_unspaced_forms_agree(sync, campaign, event):
    """Whether a human typed a space after the colon must not change the outcome."""
    spaced = campaign.replace("post:", "post: ")
    assert (sync._campaign_matches_event(campaign, event) is not None) == \
           (sync._campaign_matches_event(spaced, event) is not None)


def test_tokenize_splits_on_punctuation(sync):
    assert sync._tokenize_name("Instagram post:DCCF Less") == "instagram post dccf less"
    assert "dccf" in sync._tokenize_name("post:DCCF").split()


def test_tokenize_collapses_runs(sync):
    """Deleting ',' used to leave a double space that broke the substring test."""
    assert sync._tokenize_name("Bacon, Beer & Bourbon") == "bacon beer bourbon"
    assert sync._tokenize_name("A -- B") == "a b"


def test_tokenize_handles_empty_and_none(sync):
    assert sync._tokenize_name("") == ""
    assert sync._tokenize_name(None) == ""


def test_full_name_strategy_survives_punctuation(sync):
    """Event and campaign now normalise identically, so commas cannot desync them."""
    reason = sync._campaign_matches_event(
        "Bacon, Beer & Bourbon 2026 — early bird", "Bacon Beer Bourbon 2026")
    assert reason is not None


def test_emoji_do_not_fuse_tokens(sync):
    assert sync._campaign_matches_event(
        "Instagram post: ACF☕ So much coffee!", "Austin Coffee Festival 2026") is not None


# --------------------------------------------------------------------------
# Defect 2: SEACF alias
# --------------------------------------------------------------------------

def test_seacf_matches_seattle(sync):
    assert sync._campaign_matches_event(
        "Instagram post: SEACF Coffee is always better together",
        "Seattle Coffee Festival 2026") is not None


def test_seacf_registered_alongside_existing_variants(sync):
    for token in ('sea', 'scf', 'seacf'):
        assert token in MetaAdsSync.EVENT_ALIASES
        assert MetaAdsSync.EVENT_ALIASES[token] == ['seattle coffee']


@pytest.mark.parametrize("other", [
    "San Diego Coffee Festival 2026",
    "San Francisco Coffee Festival 2026",
    "DC Coffee Festival 2026",
    "Dallas Coffee Festival 2027",
])
def test_seacf_does_not_reach_other_cities(sync, other):
    assert sync._campaign_matches_event("Instagram post: SEACF Coffee", other) is None


# --------------------------------------------------------------------------
# The fix must not loosen city isolation
# --------------------------------------------------------------------------

ISOLATION = [
    # (campaign, event that must NOT match)
    ("Instagram post:SDCF San Diego coffee", "DC Coffee Festival 2026"),
    ("Instagram post: SDCF coffee", "DC Coffee Festival 2026"),
    ("Instagram post:DCF Dallas", "DC Coffee Festival 2026"),
    ("Instagram post:DCCF DC", "Dallas Coffee Festival 2027"),
    ("Instagram post:SCF Seattle", "San Francisco Coffee Festival 2026"),
]


@pytest.mark.parametrize("campaign,event", ISOLATION)
def test_punctuation_fix_does_not_create_cross_city_matches(sync, campaign, event):
    assert sync._campaign_matches_event(campaign, event) is None


def test_sdcf_still_reaches_san_diego(sync):
    assert sync._campaign_matches_event(
        "Instagram post:SDCF coffee", "San Diego Coffee Festival 2026") is not None


def test_explicit_year_gate_still_rejects(sync):
    """Tokenisation must not weaken the cross-year hard reject."""
    assert sync._campaign_matches_event(
        "Instagram post:ACF 2025 tickets", "Austin Coffee Festival 2026",
        event_year=2026) is None


def test_clean_event_name_unchanged_for_plain_names(sync):
    assert sync._clean_event_name("Austin Coffee Festival 2026") == "austin coffee festival"
    assert sync._clean_event_name("DC Wine Festival Fall Edition 2026") == "dc wine festival"


@pytest.mark.parametrize('campaign', ['SFCF26 engagement v1 - Copy 2', 'post:SFCF2026 coffee'])
def test_compact_alias_year_recovers_correct_edition(sync, campaign):
    assert sync._campaign_matches_event(campaign, 'San Francisco Coffee Festival', 2026)
    assert sync._extract_year(campaign) == 2026
    assert sync._campaign_matches_event(campaign, 'San Francisco Coffee Festival', 2025) is None
    assert sync._campaign_matches_event(campaign, 'Seattle Coffee Festival', 2026) is None


@pytest.mark.parametrize('campaign', ['XSFCF26', 'SFCF260', 'SFCF261234', 'SFCF2', 'NYC26', 'Seattle26'])
def test_compact_year_does_not_invent_aliases(sync, campaign):
    assert sync._campaign_tokens(campaign) == campaign.lower()


@pytest.mark.parametrize('campaign', ['SFCF25 2026', 'SFCF26 SFCF27', 'SFCF 2025 2026'])
def test_conflicting_years_do_not_choose_an_edition(sync, campaign):
    assert sync._extract_year(campaign) is None
    for year in (2025, 2026, 2027):
        assert sync._campaign_matches_event(campaign, 'San Francisco Coffee Festival', year) is None


def test_compact_alias_retains_dc_dallas_san_diego_isolation(sync):
    assert sync._campaign_matches_event('DCF coffee', 'DC Coffee Festival', 2026) is None
    assert sync._campaign_matches_event('SDCF26 coffee', 'San Diego Coffee Festival', 2026)
    assert sync._campaign_matches_event('SDCF26 coffee', 'DC Coffee Festival', 2026) is None
    assert sync._campaign_matches_event('DCF27 coffee', 'Dallas Coffee Festival', 2027)
    assert sync._campaign_matches_event('DCF27 coffee', 'DC Coffee Festival', 2027) is None


def test_plain_variant_number_is_not_a_year(sync):
    assert sync._extract_year('SFCF engagement v1 - Copy 2') is None
    assert sync._campaign_matches_event('SFCF engagement v1 - Copy 2', 'SF Coffee Festival', 2026)
