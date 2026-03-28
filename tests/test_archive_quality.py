import unittest

from scripts import filter_raw_records, ingest_sources, run_summarizer, run_verifier


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

        updated = run_verifier.apply_archive_quality_gate(record, verification, metadata)

        self.assertEqual(updated["status"], "rejected")
        self.assertIn("container_page", updated["human_review"]["notes"])


if __name__ == "__main__":
    unittest.main()
