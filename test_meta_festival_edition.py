"""Meta campaign -> festival-edition attribution.

Meta buys against a festival edition ("Austin Coffee Festival 2026"). Eventbrite
stores that edition as one or more timed-entry session rows, which are ticket
inventory a consumer picks from, not advertising targets.

Attribution used to tie-break between those raw rows, which failed two ways
against real production campaigns:

  * sibling rows share a date, `_pick_closest_event` returns None on a same-date
    tie, and the campaign was skipped — 0 of 10 real ACTIVE coffee campaigns
    were assigned; and
  * where one date happened to be uniquely nearest (DC Wine Fest's single 10-16
    row against four on 10-17), the whole edition's spend landed silently on
    that one session and the rest showed $0.

Fixtures here are modelled on campaign names and event shapes taken from the
read-only production audit of both configured ad accounts.
"""

import datetime
import os
import sys
import tempfile
import unittest

os.environ.setdefault("CRAFT_AUTO_SYNC", "0")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from craft_unified import (  # noqa: E402
    Database,
    DecisionEngine,
    MetaAdsSync,
    canonical_edition_event,
    festival_edition_key,
)

TODAY = datetime.date(2026, 9, 15)


def ev(eid, name, d):
    return {"event_id": eid, "name": name, "event_date": d + "T09:00:00"}


def sessions(prefix, name, dates_and_counts):
    """Build timed-entry rows the way production stores them."""
    out, n = [], 0
    for d, count in dates_and_counts:
        for _ in range(count):
            out.append(ev(f"{prefix}{n}", name, d))
            n += 1
    return out


class _Base(unittest.TestCase):
    def setUp(self):
        self.db = Database(os.path.join(tempfile.mkdtemp(), "meta.db"))
        self.m = MetaAdsSync("token", "act_1", self.db)

    def assign(self, campaign_name, events):
        return self.m._assign_campaign_to_edition(
            {"id": "c1", "name": campaign_name}, events, TODAY)

    def assertEdition(self, campaign_name, events, expected_edition):
        a = self.assign(campaign_name, events)
        self.assertIsNotNone(a, f"{campaign_name!r} matched nothing")
        self.assertFalse(a.get("ambiguous"),
                         f"{campaign_name!r} was ambiguous: {a.get('tied_editions')}")
        self.assertEqual(a["edition"], expected_edition)
        return a


# ---------------------------------------------------------------------------
# Edition identity reuses the existing normalizer
# ---------------------------------------------------------------------------
class TestFestivalEditionKey(_Base):
    def test_reuses_decision_engine_normalization(self):
        """Meta attribution and pacing must agree on what one edition is."""
        engine = DecisionEngine(self.db)
        for name in ("Austin Coffee Festival", "DC Wine Fest! Fall Edition",
                     "DC Wine Fest", "Philly Coffee Festival"):
            key = festival_edition_key(name, "2026-10-17T09:00:00")
            self.assertEqual(key[0], engine._get_pattern(name))

    def test_timed_entry_rows_collapse_to_one_edition(self):
        rows = sessions("A", "Austin Coffee Festival",
                        [("2026-10-17", 3), ("2026-10-18", 3)])
        keys = {festival_edition_key(e["name"], e["event_date"]) for e in rows}
        self.assertEqual(len(keys), 1)

    def test_editions_separate_by_year(self):
        a = festival_edition_key("Austin Coffee Festival", "2025-10-25T09:00:00")
        b = festival_edition_key("Austin Coffee Festival", "2026-10-17T09:00:00")
        self.assertNotEqual(a, b)
        self.assertEqual(a[0], b[0])

    def test_dc_wine_rename_keeps_one_edition_lineage(self):
        """2026 dropped '! Fall Edition' from the name; pattern must not split."""
        old = festival_edition_key("DC Wine Fest! Fall Edition", "2025-10-18T09:00:00")
        new = festival_edition_key("DC Wine Fest", "2026-10-16T09:00:00")
        self.assertEqual(old[0], new[0])

    def test_seasons_stay_distinct_within_a_year(self):
        spring = festival_edition_key("DC Wine Fest! Spring Edition", "2026-04-25T09:00:00")
        fall = festival_edition_key("DC Wine Fest", "2026-10-16T09:00:00")
        self.assertNotEqual(spring[0], fall[0])

    def test_canonical_event_is_deterministic(self):
        rows = sessions("A", "Austin Coffee Festival",
                        [("2026-10-18", 3), ("2026-10-17", 3)])
        first = canonical_edition_event(rows)
        self.assertEqual(first, canonical_edition_event(list(reversed(rows))))
        self.assertTrue(first["event_date"].startswith("2026-10-17"))


# ---------------------------------------------------------------------------
# Austin — the skip failure
# ---------------------------------------------------------------------------
class TestAustinRegression(_Base):
    def events(self):
        return (sessions("A25", "Austin Coffee Festival",
                         [("2025-10-25", 3), ("2025-10-26", 3)])
                + sessions("A26", "Austin Coffee Festival",
                           [("2026-10-17", 3), ("2026-10-18", 3)]))

    def test_yearless_acf_campaign_is_not_skipped(self):
        a = self.assertEdition("Instagram post: ACF Coffee is always better together...",
                               self.events(), "austin_coffee_fest:2026")
        self.assertEqual(len(a["edition_event_ids"]), 6,
                         "all six session rows belong to the chosen edition")

    def test_picks_upcoming_edition_not_a_past_one(self):
        a = self.assign("Instagram post: ACF So much coffee!", self.events())
        self.assertTrue(a["canonical_event"]["event_date"].startswith("2026-"))

    def test_spend_lands_on_exactly_one_canonical_row(self):
        a = self.assign("Instagram post: ACF ...", self.events())
        self.assertIn(a["canonical_event"]["event_id"], a["edition_event_ids"])
        self.assertEqual(a["canonical_event"]["event_date"][:10], "2026-10-17")

    def test_window_covers_the_whole_edition_not_just_day_one(self):
        a = self.assign("Instagram post: ACF ...", self.events())
        self.assertEqual(a["edition_last_date"][:10], "2026-10-18")

    def test_literal_full_name_campaign_also_resolves(self):
        self.assertEdition("Austin Coffee Festival 2026 - Retargeting",
                           self.events(), "austin_coffee_fest:2026")


# ---------------------------------------------------------------------------
# DC Wine — the silent-partial failure (mandatory)
# ---------------------------------------------------------------------------
class TestDCWineSilentPartial(_Base):
    def events(self):
        # Exactly production's shape: one row 10/16, four timed rows 10/17.
        return [ev("WA", "DC Wine Fest", "2026-10-16")] + sessions(
            "WB", "DC Wine Fest", [("2026-10-17", 4)])

    def test_all_rows_resolve_to_one_edition(self):
        a = self.assertEdition("Instagram post: DCWF Who do you want to see there?",
                               self.events(), "dc_wine_fest_fall_fall:2026")
        self.assertEqual(len(a["edition_event_ids"]), 5)

    def test_campaign_is_not_pinned_to_the_lone_1016_row_by_accident(self):
        """The old code chose 10-16 only because that date was uniquely nearest.

        Choosing it as the canonical row is fine; silently dropping the other
        four is not. The edition must own all five rows.
        """
        a = self.assign("Instagram post: DCWF ...", self.events())
        ids = set(a["edition_event_ids"])
        self.assertEqual(len(ids), 5)
        self.assertIn(a["canonical_event"]["event_id"], ids)
        self.assertEqual(a["edition_last_date"][:10], "2026-10-17",
                         "insight window must reach the 10-17 sessions")


# ---------------------------------------------------------------------------
# Flagships — one behaviour, no city-specific handling
# ---------------------------------------------------------------------------
class TestFlagshipEditions(_Base):
    CASES = [
        ("Instagram post: PCF The citys greatest coffee...", "Philly Coffee Festival",
         "philly_coffee_fest:2026"),
        ("Instagram post: SDCF Lots of smiles...", "San Diego Coffee Festival",
         "sd_coffee_fest:2026"),
        ("Instagram post: DCCF The Official DC Coffee Festival...", "DC Coffee Festival",
         "dc_coffee_fest:2026"),
        ("Instagram post: SEA Brewing up an incredible coffee...", "Seattle Coffee Festival",
         "seattle_coffee_fest:2026"),
        ("Instagram post: The Official SF Coffee Festival...", "San Francisco Coffee Festival",
         "sf_coffee_fest:2026"),
        ("Instagram post: Cant wait for the Dallas Coffee...", "Dallas Coffee Festival",
         "dallas_coffee_fest:2026"),
    ]

    def test_each_flagship_resolves_to_its_own_edition(self):
        for campaign, event_name, expected in self.CASES:
            with self.subTest(campaign=campaign):
                events = sessions("F", event_name,
                                  [("2026-10-17", 3), ("2026-10-18", 3)])
                self.assertEdition(campaign, events, expected)

    def test_each_flagship_uses_one_canonical_row(self):
        for campaign, event_name, _ in self.CASES:
            with self.subTest(campaign=campaign):
                events = sessions("F", event_name,
                                  [("2026-10-17", 3), ("2026-10-18", 3)])
                a = self.assign(campaign, events)
                self.assertEqual(len(a["edition_event_ids"]), 6)
                self.assertIsNotNone(a["canonical_event"])


# ---------------------------------------------------------------------------
# Safety invariants that must not regress
# ---------------------------------------------------------------------------
class TestCrossYearSafety(_Base):
    def events(self):
        return (sessions("X25", "Austin Coffee Festival",
                         [("2025-10-25", 3), ("2025-10-26", 3)])
                + sessions("X26", "Austin Coffee Festival",
                           [("2026-10-17", 3), ("2026-10-18", 3)]))

    def test_explicit_year_hard_gates_to_that_year(self):
        a = self.assertEdition("ACF 2025 retargeting", self.events(),
                               "austin_coffee_fest:2025")
        for eid in a["edition_event_ids"]:
            self.assertTrue(eid.startswith("X25"))

    def test_2026_campaign_never_touches_2025(self):
        a = self.assertEdition("ACF 2026 prospecting", self.events(),
                               "austin_coffee_fest:2026")
        self.assertTrue(all(e.startswith("X26") for e in a["edition_event_ids"]))


class TestCrossProductSafety(_Base):
    def test_coffee_and_wine_in_the_same_city_never_merge(self):
        events = (sessions("C", "Philly Coffee Festival", [("2026-10-17", 3)])
                  + sessions("W", "Philly Wine Fest! Fall Edition", [("2026-11-14", 1)]))
        coffee = self.assertEdition("Instagram post: PCF ...", events,
                                    "philly_coffee_fest:2026")
        wine = self.assertEdition("Instagram post: PWF ...", events,
                                  "philly_wine_fest_fall_fall:2026")
        self.assertNotEqual(coffee["edition"], wine["edition"])
        self.assertFalse(set(coffee["edition_event_ids"]) & set(wine["edition_event_ids"]))

    def test_dc_coffee_wine_and_cocktail_stay_separate(self):
        events = (sessions("C", "DC Coffee Festival", [("2026-09-26", 3)])
                  + sessions("W", "DC Wine Fest", [("2026-10-16", 1)])
                  + sessions("K", "The Official DC Cocktail Festival", [("2026-03-28", 1)]))
        a = self.assertEdition("Instagram post: DCCF ...", events, "dc_coffee_fest:2026")
        b = self.assertEdition("Instagram post: DCWF ...", events, "dc_wine_fest_fall_fall:2026")
        self.assertNotEqual(a["edition"], b["edition"])


class TestNoSpendDuplication(_Base):
    def test_one_campaign_yields_exactly_one_canonical_event_id(self):
        events = sessions("A", "Austin Coffee Festival",
                          [("2026-10-17", 3), ("2026-10-18", 3)])
        a = self.assign("Instagram post: ACF ...", events)
        self.assertEqual(len([a["canonical_event"]["event_id"]]), 1)

    def test_many_campaigns_same_edition_share_one_canonical_row(self):
        events = sessions("A", "Austin Coffee Festival",
                          [("2026-10-17", 3), ("2026-10-18", 3)])
        canon = {self.assign(c, events)["canonical_event"]["event_id"]
                 for c in ("Instagram post: ACF one", "Instagram post: ACF two",
                           "Austin Coffee Festival 2026 promo")}
        self.assertEqual(len(canon), 1,
                         "all campaigns for an edition must target one storage row")


class TestSimpleEventNotRegressed(_Base):
    def test_single_row_event_behaves_exactly_as_before(self):
        events = [ev("S1", "Nashville Coffee Festival", "2026-11-04")]
        a = self.assertEdition("Nashville Coffee Festival 2026", events,
                               "nashville_coffee_fest:2026")
        self.assertEqual(a["edition_event_ids"], ["S1"])
        self.assertEqual(a["canonical_event"]["event_id"], "S1")

    def test_unrelated_campaign_still_matches_nothing(self):
        events = [ev("S1", "Nashville Coffee Festival", "2026-11-04")]
        self.assertIsNone(self.assign("Completely Unrelated Brand Campaign", events))


class TestGenuineAmbiguityStillFailsSafe(_Base):
    def test_two_distinct_editions_on_the_same_date_are_skipped(self):
        """Fail-safe must survive for real ambiguity between DIFFERENT editions."""
        # 'sfcf' reaches both the main SF festival and the separately-named
        # latte-art event; distinct patterns, identical date, equal strength.
        events = [ev("P1", "San Francisco Coffee Festival", "2026-11-07"),
                  ev("P2", "SF Coffee Festival Latte Art Competition", "2026-11-07")]
        a = self.assign("SFCF engagement", events)
        self.assertTrue(a.get("ambiguous"))
        self.assertGreaterEqual(len(a["tied_editions"]), 2)


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------
class TestAliases(_Base):
    def test_added_and_corrected_aliases(self):
        cases = [
            ("sea", "Instagram post: SEA Brewing up...", "Seattle Coffee Festival"),
            ("sd", "Instagram post: SD Assemble your coffee crew", "San Diego Coffee Festival"),
            ("sf", "Instagram post: SF The energy. The people.", "San Francisco Coffee Festival"),
            ("nyccf", "NYCCF engagement", "NYC Craft Coffee Festival"),
            ("nycf", "NYCF engagement", "NYC Craft Coffee Festival"),
            ("dcf", "DCF engagement", "Dallas Coffee Festival"),
            ("scf", "SCF engagement", "Seattle Coffee Festival"),
        ]
        for alias, campaign, event_name in cases:
            with self.subTest(alias=alias):
                self.assertIsNotNone(
                    self.m._campaign_matches_event(campaign, event_name, event_year=2026),
                    f"alias {alias!r} did not match {event_name!r}")

    def test_dcf_means_dallas_not_dc(self):
        self.assertIsNotNone(
            self.m._campaign_matches_event("DCF push", "Dallas Coffee Festival", 2026))
        self.assertIsNone(
            self.m._campaign_matches_event("DCF push", "DC Coffee Festival", 2026))

    def test_scf_is_seattle_only_not_san_francisco(self):
        self.assertIsNotNone(
            self.m._campaign_matches_event("SCF push", "Seattle Coffee Festival", 2026))
        self.assertIsNone(
            self.m._campaign_matches_event("SCF push", "San Francisco Coffee Festival", 2026))

    def test_preserved_aliases_still_work(self):
        for campaign, event_name in [("ACF x", "Austin Coffee Festival"),
                                     ("PCF x", "Philly Coffee Festival"),
                                     ("DCCF x", "DC Coffee Festival"),
                                     ("SDCF x", "San Diego Coffee Festival"),
                                     ("SFCF x", "San Francisco Coffee Festival"),
                                     ("DCWF x", "DC Wine Fest"),
                                     ("PWF x", "Philly Wine Fest! Fall Edition"),
                                     ("DAL CF x", "Dallas Coffee Festival")]:
            with self.subTest(campaign=campaign):
                self.assertIsNotNone(
                    self.m._campaign_matches_event(campaign, event_name, 2026))

    def test_nyc_coffee_aliases_do_not_reach_nyc_non_coffee_events(self):
        """NYC also runs whiskey/margarita/cocktail/beer; no bare 'nyc' alias."""
        for other in ("NYC Whiskey Walk 2026", "NYC Margarita Rumble!",
                      "NYC Cocktail Fest", "NYC Beerathon 2026"):
            for campaign in ("NYCCF engagement", "NYCF engagement"):
                with self.subTest(event=other, campaign=campaign):
                    self.assertIsNone(
                        self.m._campaign_matches_event(campaign, other, 2026),
                        f"{campaign!r} wrongly matched {other!r}")

    def test_no_bare_nyc_alias_exists(self):
        self.assertNotIn("nyc", MetaAdsSync.EVENT_ALIASES)


# ---------------------------------------------------------------------------
# Match strategy / rank cleanup
# ---------------------------------------------------------------------------
class TestMatchStrategy(_Base):
    def test_strategy_keys_line_up_with_match_rank(self):
        pairs = [("Austin Coffee Festival promo", "Austin Coffee Festival", "full_name"),
                 ("ACF promo", "Austin Coffee Festival", "abbreviation")]
        for campaign, event_name, expected in pairs:
            with self.subTest(campaign=campaign):
                strategy, reason = self.m._match_strategy(campaign, event_name, 2026)
                self.assertEqual(strategy, expected)
                self.assertIn(strategy, MetaAdsSync._MATCH_RANK)
                self.assertGreater(MetaAdsSync._MATCH_RANK[strategy], 0)

    def test_reason_string_is_unchanged_for_logs(self):
        self.assertEqual(
            self.m._campaign_matches_event("ACF promo", "Austin Coffee Festival", 2026),
            "abbreviation 'acf'")

    def test_stronger_strategy_wins_between_editions(self):
        events = [ev("E1", "Austin Coffee Festival", "2026-10-17"),
                  ev("E2", "Seattle Coffee Festival", "2026-10-17")]
        # Full name beats the 'sea' abbreviation that also appears in the text.
        a = self.assign("Austin Coffee Festival - SEA lookalike test", events)
        self.assertEqual(a["edition"], "austin_coffee_fest:2026")


# ---------------------------------------------------------------------------
# Read-only guarantees
# ---------------------------------------------------------------------------
class TestReadOnly(_Base):
    def test_meta_client_issues_only_get(self):
        import ast
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "craft_unified.py")).read()
        cls = next(n for n in ast.walk(ast.parse(src))
                   if isinstance(n, ast.ClassDef) and n.name == "MetaAdsSync")
        verbs = set()
        for node in ast.walk(cls):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in ("get", "post", "put", "patch", "delete", "request"):
                    base = node.func.value
                    if isinstance(base, ast.Attribute) and base.attr == "session":
                        verbs.add(node.func.attr)
        self.assertEqual(verbs, {"get"}, f"MetaAdsSync HTTP verbs: {verbs}")

    def test_dry_run_writes_nothing(self):
        events = sessions("A", "Austin Coffee Festival",
                          [("2026-10-17", 3), ("2026-10-18", 3)])
        before = self.db.conn.execute("SELECT COUNT(*) FROM ad_spend").fetchone()[0]
        report = self.m.dry_run_assignment(
            events, campaigns=[{"id": "1", "name": "Instagram post: ACF ...",
                                "status": "ACTIVE"}])
        after = self.db.conn.execute("SELECT COUNT(*) FROM ad_spend").fetchone()[0]
        self.assertEqual(before, after)
        self.assertEqual(report["writes_performed"], 0)
        self.assertEqual(report["meta_mutations_performed"], 0)
        self.assertEqual(report["counts"]["assigned"], 1)

    def test_dry_run_requests_no_insights(self):
        called = []
        self.m._fetch_daily_insights = lambda *a, **k: called.append(a) or []
        self.m.dry_run_assignment(
            [ev("E1", "Austin Coffee Festival", "2026-10-17")],
            campaigns=[{"id": "1", "name": "ACF x", "status": "ACTIVE"}])
        self.assertEqual(called, [], "dry run must not fetch campaign spend")

    def test_dry_run_reports_required_fields(self):
        report = self.m.dry_run_assignment(
            sessions("A", "Austin Coffee Festival", [("2026-10-17", 3)]),
            campaigns=[{"id": "9", "name": "Instagram post: ACF ...", "status": "PAUSED"}])
        row = report["campaigns"][0]
        for field in ("account_id", "campaign_id", "campaign_name", "status",
                      "parsed_year", "outcome", "match_reason", "edition",
                      "raw_candidate_event_ids", "canonical_event_id"):
            self.assertIn(field, row)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestEditionYearSource(_Base):
    """Edition year: explicit name year wins, date is the fallback."""

    def test_explicit_name_year_separates_editions_with_same_date_year(self):
        a = festival_edition_key("Austin Coffee Festival 2025", "2026-02-27T09:00:00")
        b = festival_edition_key("Austin Coffee Festival 2026", "2026-11-14T09:00:00")
        self.assertNotEqual(a, b)
        self.assertEqual(a[1], 2025)
        self.assertEqual(b[1], 2026)

    def test_date_year_used_when_the_name_has_none(self):
        self.assertEqual(
            festival_edition_key("Austin Coffee Festival", "2026-10-17T09:00:00")[1], 2026)

    def test_named_and_unnamed_rows_of_one_edition_still_agree(self):
        """2023 rows are name-tagged; 2026 rows are not. Both must be coherent."""
        tagged = festival_edition_key("Austin Coffee Festival 2023", "2023-09-30T09:00:00")
        untagged = festival_edition_key("Austin Coffee Festival", "2023-10-01T09:00:00")
        self.assertEqual(tagged, untagged)

    def test_untagged_campaign_prefers_the_upcoming_edition(self):
        events = [ev("P", "Austin Coffee Festival 2025", "2026-02-27"),
                  ev("U", "Austin Coffee Festival 2026", "2026-11-14")]
        a = self.assign("Austin Coffee Fest - Retargeting", events)
        self.assertEqual(a["canonical_event"]["event_id"], "U")


class TestContentWordPrefixMatching(_Base):
    """Content words match as a prefix, never inside an acronym.

    'DC Coffee Festival' reduces to content words ['dc', 'coffee']. Under the
    old arbitrary-substring rule, 'dc' matched inside 'sdcf', so real San Diego
    campaigns ("Instagram post: SDCF ... coffee") matched DC Coffee — and with
    edition grouping that turned a safe skip into San Diego spend recorded
    against DC.
    """

    def test_sdcf_campaign_does_not_match_dc_coffee(self):
        for campaign in ("Instagram post: SDCF Assemble your coffee crew ...",
                         "Instagram post: SDCF The citys greatest coffee..."):
            with self.subTest(campaign=campaign):
                self.assertIsNone(
                    self.m._campaign_matches_event(campaign, "DC Coffee Festival", 2026))
                self.assertIsNotNone(
                    self.m._campaign_matches_event(campaign, "San Diego Coffee Festival", 2026))

    def test_sdcf_resolves_to_san_diego_edition(self):
        events = (sessions("SD", "San Diego Coffee Festival",
                           [("2026-10-10", 3), ("2026-10-11", 3)])
                  + sessions("DC", "DC Coffee Festival",
                             [("2026-10-03", 3), ("2026-10-04", 3)]))
        a = self.assertEdition("Instagram post: SDCF The citys greatest coffee...",
                               events, "sd_coffee_fest:2026")
        self.assertTrue(all(e.startswith("SD") for e in a["edition_event_ids"]))

    def test_dccf_still_matches_dc_coffee(self):
        self.assertIsNotNone(
            self.m._campaign_matches_event("Instagram post: DCCF The citys greatest coffee...",
                                           "DC Coffee Festival", 2026))

    def test_plurals_and_possessives_still_match(self):
        """Prefix must keep the cases the loose rule existed for."""
        for campaign, event_name in [
            ("Instagram post: Austins much loved coffee festival", "Austin Coffee Festival"),
            ("Instagram post: Phillys much loved coffee festival", "Philly Coffee Festival"),
        ]:
            with self.subTest(campaign=campaign):
                self.assertIsNotNone(
                    self.m._campaign_matches_event(campaign, event_name, 2026))

    def test_city_tokens_do_not_leak_across_cities(self):
        pairs = [("Instagram post: SDCF coffee", "DC Coffee Festival"),
                 ("Instagram post: SEA coffee", "DC Coffee Festival"),
                 ("Instagram post: SDCF coffee", "Philly Coffee Festival")]
        for campaign, event_name in pairs:
            with self.subTest(campaign=campaign, event=event_name):
                self.assertIsNone(
                    self.m._campaign_matches_event(campaign, event_name, 2026))
