import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import (
    backfill_archive_quality,
    filter_raw_records,
    finalize_review,
    ingest_sources,
    run_summarizer,
    run_verifier,
)
from scripts import verification_store


class IngestSourceClassificationTests(unittest.TestCase):
    def test_classifies_root_url_as_homepage(self):
        text = "\n".join(
            [
                "The New York Stock Exchange | NYSE",
                "Listings",
                "Trading",
                "Market Data",
                "About",
                "Today’s Stock Market",
                "March 27, 2026 at 5:00 p.m. EST",
            ]
        )

        page_type = ingest_sources.classify_page_type(
            url="https://www.nyse.com/",
            title="The New York Stock Exchange | NYSE",
            text=text,
            published_at="2026-03-27T17:00:00-05:00",
        )

        self.assertEqual(page_type, "homepage")

    def test_classifies_language_selector_page_as_navigation(self):
        text = "\n".join(
            [
                "Monetary policy decisions",
                "Skip to navigation",
                "Skip to content",
                "BG",
                "English",
                "Deutsch",
                "Menu",
                "Contacts",
                "Frequently asked questions",
            ]
        )

        page_type = ingest_sources.classify_page_type(
            url="https://www.ecb.europa.eu/press/govcdec/mopo/html/index.bg.html",
            title="Monetary policy decisions",
            text=text,
            published_at="",
        )

        self.assertEqual(page_type, "navigation_page")

    def test_extract_links_prefers_article_over_root_and_index(self):
        html = """
        <html>
          <body>
            <a href="https://www.nyse.com/">Today's Stock Market</a>
            <a href="https://www.nyse.com/index">Weekly market recap</a>
            <a href="https://www.nyse.com/trade/2026-03-27-weekly-market-recap">Weekly market recap March 27 2026</a>
          </body>
        </html>
        """

        links = ingest_sources.extract_links(
            index_url="https://www.nyse.com/trade",
            html=html,
            allowed_prefixes=["https://www.nyse.com/"],
            blocklist_fragments=[],
            max_links=3,
        )

        self.assertEqual(links, ["https://www.nyse.com/trade/2026-03-27-weekly-market-recap"])

    def test_classifies_press_release_page(self):
        page_type = ingest_sources.classify_page_type(
            url="https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm",
            title="Federal Reserve issues FOMC statement",
            text="Federal Reserve issues FOMC statement on monetary policy.",
            published_at="2026-01-28",
        )

        self.assertEqual(page_type, "press_release")

    def test_classifies_speech_page(self):
        page_type = ingest_sources.classify_page_type(
            url="https://www.bankofcanada.ca/2026/03/speech-on-inflation-outlook/",
            title="Speech: Inflation outlook and monetary policy",
            text="Speech delivered by the Governor on inflation and monetary policy.",
            published_at="2026-03-18",
        )

        self.assertEqual(page_type, "speech")

    def test_classifies_market_notice_page(self):
        page_type = ingest_sources.classify_page_type(
            url="https://www.bankofcanada.ca/markets/market-notices/2026/03/operation-details/",
            title="Market Notice: Term repo operation details",
            text="Market Notice covering the upcoming term repo operation.",
            published_at="2026-03-20",
        )

        self.assertEqual(page_type, "market_notice")

    def test_classifies_data_release_page(self):
        page_type = ingest_sources.classify_page_type(
            url="https://www.bea.gov/news/2026/gross-domestic-product-fourth-quarter-2025-third-estimate",
            title="Gross Domestic Product, Fourth Quarter 2025 and Year 2025 (Third Estimate)",
            text="Gross domestic product increased 2.4 percent in the fourth quarter.",
            published_at="2026-03-26",
        )

        self.assertEqual(page_type, "data_release")


class RawRecordPromptTests(unittest.TestCase):
    def test_record_template_uses_source_metadata_defaults(self):
        schema = {
            "id": "",
            "created_at": "",
            "status": "",
            "topic": "",
            "source": {
                "name": "",
                "url": "",
                "published_at": "",
                "source_type": "",
            },
            "llm_review": {"verdict": "", "verification_confidence": 0},
        }
        metadata = {
            "TARGET": "ECB Monetary Policy Decisions",
            "TOPIC": "macro catalysts",
            "URL": "https://www.ecb.europa.eu/press/govcdec/mopo/2026/html/ecb.mp260319~example.en.html",
            "PUBLISHED_AT": "2026-03-19",
            "PAGE_TYPE": "press_release",
        }

        record = run_summarizer.build_record_template(schema, metadata, raw_file_stem="ecb_record")

        self.assertEqual(record["topic"], "macro catalysts")
        self.assertEqual(record["source"]["name"], "ECB Monetary Policy Decisions")
        self.assertEqual(
            record["source"]["url"],
            "https://www.ecb.europa.eu/press/govcdec/mopo/2026/html/ecb.mp260319~example.en.html",
        )
        self.assertEqual(record["source"]["published_at"], "2026-03-19")
        self.assertEqual(record["source"]["source_type"], "press_release")

    def test_record_template_prefers_canonical_url(self):
        schema = {
            "id": "",
            "created_at": "",
            "status": "",
            "topic": "",
            "source": {
                "name": "",
                "url": "",
                "published_at": "",
                "source_type": "",
            },
            "llm_review": {"verdict": "", "verification_confidence": 0},
        }
        metadata = {
            "TARGET": "Federal Reserve Press Releases",
            "TOPIC": "macro catalysts",
            "URL": "https://www.federalreserve.gov/newsevents/pressreleases/2026-press.htm",
            "ARTICLE_URL": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm?utm_source=test",
            "CANONICAL_URL": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm",
            "PUBLISHED_AT": "2026-01-28",
            "PAGE_TYPE": "press_release",
        }

        record = run_summarizer.build_record_template(schema, metadata, raw_file_stem="fed_record")

        self.assertEqual(
            record["source"]["url"],
            "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm",
        )

    def test_prompt_input_separates_metadata_and_body(self):
        raw_text = "\n".join(
            [
                "TARGET: NYSE Trade",
                "TOPIC: market structure",
                "TITLE: Weekly Market Recap",
                "URL: https://www.nyse.com/trade/weekly-market-recap",
                "PAGE_TYPE: article",
                "PUBLISHED_AT: 2026-03-27T17:00:00-05:00",
                "",
                "The S&P 500 fell 2% for the week.",
            ]
        )

        parsed = filter_raw_records.parse_raw_record(raw_text)
        prompt = run_summarizer.build_prompt_input("PROMPT", parsed, {"id": "demo"})

        self.assertIn("=== SOURCE METADATA START ===", prompt)
        self.assertIn('"PAGE_TYPE": "article"', prompt)
        self.assertIn("=== SOURCE BODY START ===", prompt)
        self.assertIn("The S&P 500 fell 2% for the week.", prompt)

    def test_filter_rejects_language_mismatch(self):
        raw_text = "\n".join(
            [
                "TARGET: ECB Monetary Policy Decisions",
                "TOPIC: macro catalysts",
                "TITLE: Monetary policy decisions",
                "URL: https://www.ecb.europa.eu/press/govcdec/mopo/html/index.bg.html",
                "PAGE_TYPE: article",
                "EXPECTED_LANGUAGE: en",
                "DETECTED_LANGUAGE: non_en",
                "",
                "Лихвени проценти и парична политика.",
            ]
        )

        parsed = filter_raw_records.parse_raw_record(raw_text)
        keep, reasons = filter_raw_records.evaluate_article_record(
            parsed["body"],
            {"min_word_count": 1, "expected_language": "en"},
            parsed["metadata"],
        )

        self.assertFalse(keep)
        self.assertIn("language_mismatch", reasons)

    def test_filter_rejects_disallowed_page_type(self):
        raw_text = "\n".join(
            [
                "TARGET: ECB Monetary Policy Decisions",
                "TOPIC: macro catalysts",
                "TITLE: Monetary policy decisions",
                "URL: https://www.ecb.europa.eu/press/govcdec/mopo/2026/html/ecb.mp260319~example.en.html",
                "PAGE_TYPE: press_release",
                "EXPECTED_LANGUAGE: en",
                "DETECTED_LANGUAGE: en",
                "",
                "Interest rates and monetary policy decisions were announced by the Governing Council.",
            ]
        )

        parsed = filter_raw_records.parse_raw_record(raw_text)
        keep, reasons = filter_raw_records.evaluate_article_record(
            parsed["body"],
            {"min_word_count": 1, "expected_language": "en", "allowed_page_types": ["speech"]},
            parsed["metadata"],
        )

        self.assertFalse(keep)
        self.assertIn("page_type_not_allowed", reasons)


class VerifierGateTests(unittest.TestCase):
    def test_container_page_is_forced_to_rejected(self):
        record = {
            "status": "accepted",
            "source": {
                "name": "European Central Bank",
                "url": "https://www.ecb.europa.eu/press/govcdec/mopo/html/index.bg.html",
                "published_at": "",
                "source_type": "website_navigation",
            },
            "llm_review": {
                "verification_confidence": 9,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {"required": False, "decision": "", "notes": ""},
        }
        verification = {
            "verification_confidence": 9,
            "verdict": "accept",
            "issues_found": [],
            "human_review_required": False,
            "human_review_reason": "",
            "suggested_status": "accepted",
            "corrected_fields": {},
        }
        metadata = {
            "PAGE_TYPE": "navigation_page",
            "CONTENT_WORD_COUNT": "900",
            "EXTRACTION_WARNINGS": "container_page",
        }

        updated = run_verifier.apply_archive_quality_gate(record, verification, metadata, {})

        self.assertEqual(updated["status"], "rejected")
        self.assertIn("container_page", updated["human_review"]["notes"])

    def test_missing_source_attribution_is_hard_rejected(self):
        record = {
            "status": "review_queue",
            "source": {
                "name": "Federal Reserve Press Releases",
                "url": "",
                "published_at": "2026-01-28",
                "source_type": "press_release",
            },
            "llm_review": {
                "verification_confidence": 9,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {"required": False, "decision": "", "notes": ""},
        }
        verification = {
            "verification_confidence": 9,
            "verdict": "accept",
            "issues_found": [],
            "human_review_required": False,
            "human_review_reason": "",
            "suggested_status": "accepted",
            "corrected_fields": {},
        }
        metadata = {"PAGE_TYPE": "press_release", "EXPECTED_LANGUAGE": "en", "DETECTED_LANGUAGE": "en"}

        updated = run_verifier.apply_archive_quality_gate(
            record,
            verification,
            metadata,
            {"allowed_page_types": ["press_release"]},
        )

        self.assertEqual(updated["status"], "rejected")
        self.assertIn("missing_source_attribution", updated["human_review"]["notes"])

    def test_low_confidence_article_stays_in_review_queue(self):
        record = {
            "status": "accepted",
            "source": {
                "name": "Federal Reserve Press Releases",
                "url": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm",
                "published_at": "2026-01-28",
                "source_type": "press_release",
            },
            "llm_review": {
                "verification_confidence": 7,
                "verdict": "review",
                "issues_found": [],
            },
            "human_review": {"required": False, "decision": "", "notes": ""},
        }
        verification = {
            "verification_confidence": 7,
            "verdict": "review",
            "issues_found": [],
            "human_review_required": True,
            "human_review_reason": "needs review",
            "suggested_status": "review_queue",
            "corrected_fields": {},
        }
        metadata = {"PAGE_TYPE": "press_release", "EXPECTED_LANGUAGE": "en", "DETECTED_LANGUAGE": "en"}

        updated = run_verifier.apply_archive_quality_gate(
            record,
            verification,
            metadata,
            {"allowed_page_types": ["press_release"]},
        )

        self.assertEqual(updated["status"], "review_queue")

    def test_clean_allowed_article_is_accepted(self):
        record = {
            "status": "review_queue",
            "source": {
                "name": "Federal Reserve Press Releases",
                "url": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm",
                "published_at": "2026-01-28",
                "source_type": "press_release",
            },
            "llm_review": {
                "verification_confidence": 9,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {"required": False, "decision": "", "notes": ""},
        }
        verification = {
            "verification_confidence": 9,
            "verdict": "accept",
            "issues_found": [],
            "human_review_required": False,
            "human_review_reason": "",
            "suggested_status": "accepted",
            "corrected_fields": {},
        }
        metadata = {"PAGE_TYPE": "press_release", "EXPECTED_LANGUAGE": "en", "DETECTED_LANGUAGE": "en"}

        updated = run_verifier.apply_archive_quality_gate(
            record,
            verification,
            metadata,
            {"allowed_page_types": ["press_release"]},
        )

        self.assertEqual(updated["status"], "accepted")


class FinalizeReviewTests(unittest.TestCase):
    def test_hard_blocked_human_approval_is_rejected(self):
        record = {
            "status": "review_queue",
            "source": {
                "name": "European Central Bank",
                "url": "https://www.ecb.europa.eu/press/govcdec/mopo/html/index.bg.html",
                "published_at": "",
                "source_type": "website_navigation",
            },
            "llm_review": {
                "verification_confidence": 9,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {"required": True, "decision": "", "notes": ""},
        }

        updated = finalize_review.apply_review_decision(
            record,
            decision="approve",
            hard_blockers=["container_page"],
        )

        self.assertEqual(updated["status"], "rejected")
        self.assertEqual(updated["human_review"]["decision"], "rejected_by_quality_gate")
        self.assertIn("container_page", updated["human_review"]["notes"])


class BackfillQualityTests(unittest.TestCase):
    def test_missing_raw_navigation_record_is_rejected(self):
        record = {
            "id": "ecb_monetary_policy_decisions_nav_page_2026_03_19",
            "source": {
                "name": "European Central Bank",
                "url": "https://www.ecb.europa.eu/press/govcdec/mopo/html/index.bg.html",
                "published_at": "",
                "source_type": "website_navigation",
            },
            "summary": "This source is an ECB website navigation/menu page.",
            "notes": "LOW QUALITY SOURCE - navigation page",
        }

        status, reasons = backfill_archive_quality.classify_missing_raw_record(record)

        self.assertEqual(status, "rejected")
        self.assertIn("missing_raw_navigation_record", reasons)

    def test_placeholder_quant_record_is_rejected_during_backfill(self):
        record = {
            "status": "accepted",
            "source": {
                "name": "treasury_fiscal_data",
                "url": "",
                "published_at": "",
                "source_type": "dataset_snapshot",
            },
            "summary": "Placeholder dataset snapshot with no real Treasury auction data.",
            "notes": "placeholder dataset snapshot",
            "llm_review": {
                "verification_confidence": 9,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {"required": False, "decision": "", "notes": ""},
        }
        verification = {
            "verification_confidence": 9,
            "verdict": "accept",
            "issues_found": [],
            "human_review_required": False,
            "human_review_reason": "",
            "suggested_status": "accepted",
            "corrected_fields": {},
        }
        raw_text = "\n".join(
            [
                "TARGET: Upcoming Treasury Auctions",
                "TOPIC: market structure",
                "SOURCE: treasury_fiscal_data",
                "SNAPSHOT_DATE: 2026_03_28",
                "",
                "This is still a placeholder dataset snapshot for Upcoming Treasury Auctions from treasury_fiscal_data.",
                "Next we can replace this with real dataset/API fetches.",
            ]
        )

        status, reasons = backfill_archive_quality.evaluate_record_for_backfill(
            record,
            verification,
            raw_text,
            {},
        )

        self.assertEqual(status, "rejected")
        self.assertIn("missing_quant_values", reasons)

    def test_legacy_nyse_index_record_is_reclassified_and_rejected(self):
        record = {
            "status": "accepted",
            "source": {
                "name": "NYSE Trade",
                "url": "https://www.nyse.com/index",
                "published_at": "2026-03-27T17:00:00-05:00",
                "source_type": "exchange_report",
            },
            "summary": "Weekly market recap from the NYSE index page.",
            "notes": "",
            "llm_review": {
                "verification_confidence": 8,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {"required": False, "decision": "", "notes": ""},
        }
        verification = {
            "verification_confidence": 8,
            "verdict": "accept",
            "issues_found": [],
            "human_review_required": False,
            "human_review_reason": "",
            "suggested_status": "accepted",
            "corrected_fields": {},
        }
        raw_text = "\n".join(
            [
                "TARGET: NYSE Trade",
                "TOPIC: market structure",
                "TITLE: The New York Stock Exchange | NYSE",
                "URL: https://www.nyse.com/index",
                "",
                "The New York Stock Exchange | NYSE",
                "Listings",
                "Trading",
                "Market Data",
                "About",
                "Today's Stock Market",
                "March 27, 2026 at 5:00 p.m. EST",
                "The S&P 500 declined 2% for the week ending March 27, 2026.",
                "What’s next?",
                "Connect with NYSE",
                "Terms of Use",
            ]
        )

        status, reasons = backfill_archive_quality.evaluate_record_for_backfill(
            record,
            verification,
            raw_text,
            {"allowed_page_types": ["article", "market_notice"], "expected_language": "en"},
        )

        self.assertEqual(status, "rejected")
        self.assertIn("non_article_page_type", reasons)


class VerificationStoreTests(unittest.TestCase):
    def test_verification_artifact_path_uses_verify_directory(self):
        path = verification_store.verification_artifact_path("sample_record")

        self.assertEqual(path.parent.name, "verify")
        self.assertEqual(path.name, "sample_record_verification.json")

    def test_canonicalize_verification_artifact_moves_legacy_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            rejected_dir = base_dir / "data" / "rejected"
            review_queue_dir = base_dir / "data" / "review_queue"
            verify_dir = base_dir / "data" / "verify"
            accepted_dir.mkdir(parents=True)
            rejected_dir.mkdir(parents=True)
            review_queue_dir.mkdir(parents=True)

            legacy_path = accepted_dir / "sample_record_verification.json"
            legacy_path.write_text("{}", encoding="utf-8")

            with (
                patch.object(verification_store, "BASE_DIR", base_dir),
                patch.object(verification_store, "VERIFY_DIR", verify_dir),
                patch.object(
                    verification_store,
                    "LEGACY_VERIFICATION_DIRS",
                    [accepted_dir, rejected_dir, review_queue_dir],
                ),
            ):
                canonical_path = verification_store.canonicalize_verification_artifact("sample_record")

            self.assertEqual(canonical_path, verify_dir / "sample_record_verification.json")
            self.assertTrue(canonical_path.exists())
            self.assertFalse(legacy_path.exists())


if __name__ == "__main__":
    unittest.main()
