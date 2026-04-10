"""Tests for the local file drop inbox ingestion script (scripts/ingest_inbox.py)."""

import importlib.util
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
MODULE_PATH = BASE_DIR / "scripts" / "ingest_inbox.py"
MODULE_SPEC = importlib.util.spec_from_file_location("ingest_inbox", MODULE_PATH)
assert MODULE_SPEC and MODULE_SPEC.loader
ingest_inbox = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(ingest_inbox)


INBOX_DIR = BASE_DIR / "data" / "inbox"
PROCESSED_DIR = INBOX_DIR / "processed"
RAW_DIR = BASE_DIR / "data" / "raw"


class TestInboxExtensions:
    def test_supported_extensions(self):
        assert ".pdf" in ingest_inbox.SUPPORTED_EXTENSIONS
        assert ".txt" in ingest_inbox.SUPPORTED_EXTENSIONS
        assert ".html" in ingest_inbox.SUPPORTED_EXTENSIONS
        assert ".md" in ingest_inbox.SUPPORTED_EXTENSIONS


class TestBuildRawRecord:
    def test_build_raw_record_text_format(self):
        record = {
            "title": "Test Document",
            "source": "inbox",
            "url": "file://inbox/test_document.pdf",
            "content": "This is the document content.",
            "ingested_at": "2026-04-09T12:00:00Z",
        }
        output = ingest_inbox.build_raw_record_text(record)
        assert "TARGET: inbox" in output
        assert "TOPIC: manual_upload" in output
        assert "TITLE: Test Document" in output
        assert "URL: file://inbox/test_document.pdf" in output
        assert "SOURCE_TYPE: document" in output
        assert "INGEST_SOURCE: inbox" in output
        assert "PAGE_TYPE: document" in output
        assert "This is the document content." in output

    def test_build_record_id_from_filename(self):
        record_id = ingest_inbox.build_record_id("my_test_file.pdf")
        assert "my_test_file" in record_id
        assert record_id.startswith("inbox_")


class TestExtractText:
    def test_extract_text_from_txt(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, this is plain text content.", encoding="utf-8")
        text = ingest_inbox.extract_text(str(test_file))
        assert "Hello, this is plain text content." in text

    def test_extract_text_from_md(self, tmp_path):
        test_file = tmp_path / "test.md"
        test_file.write_text(
            "# Markdown Document\n\nSome **bold** content.", encoding="utf-8"
        )
        text = ingest_inbox.extract_text(str(test_file))
        assert "Markdown Document" in text
        assert "bold" in text

    def test_extract_text_from_html(self, tmp_path):
        test_file = tmp_path / "test.html"
        test_file.write_text(
            "<html><head><title>Test</title></head><body><article><p>Hello from HTML</p></article></body></html>",
            encoding="utf-8",
        )
        text = ingest_inbox.extract_text(str(test_file))
        assert "Hello from HTML" in text

    def test_extract_text_from_pdf(self, tmp_path):
        try:
            from pypdf import PdfWriter

            test_file = tmp_path / "test.pdf"
            writer = PdfWriter()
            writer.add_blank_page(width=200, height=200)
            with open(test_file, "wb") as f:
                writer.write(f)
            text = ingest_inbox.extract_text(str(test_file))
            assert len(text) >= 0
        except ImportError:
            pytest.skip("pypdf not available")


class TestScanInbox:
    def test_scan_inbox_finds_supported_files(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        (inbox / "file1.txt").write_text("content")
        (inbox / "file2.pdf").write_text("pdf content")
        (inbox / "file3.html").write_text("<html><body>content</body></html>")
        (inbox / "file4.md").write_text("# markdown")
        (inbox / "file5.jpg").write_text("not supported")
        (inbox / "file6.TXT").write_text("uppercase extension")

        with patch.object(ingest_inbox, "INBOX_DIR", inbox):
            files = ingest_inbox.scan_inbox()
            filenames = [Path(f).name for f in files]
            assert "file1.txt" in filenames
            assert "file6.TXT" in filenames

    def test_scan_inbox_ignores_processed_dir(self, tmp_path):
        inbox = tmp_path / "inbox"
        processed = inbox / "processed"
        processed.mkdir(parents=True)
        (inbox / "file1.txt").write_text("content")
        (processed / "file2.txt").write_text("already processed")

        with patch.object(ingest_inbox, "INBOX_DIR", inbox):
            files = ingest_inbox.scan_inbox()
            filenames = [Path(f).name for f in files]
            assert "file1.txt" in filenames
            assert "file2.txt" not in filenames


class TestProcessInboxFile:
    def test_process_file_creates_raw_record(self, tmp_path):
        inbox = tmp_path / "inbox"
        processed = inbox / "processed"
        raw = tmp_path / "data" / "raw"
        inbox.mkdir(parents=True)
        processed.mkdir(parents=True)
        raw.mkdir(parents=True)

        test_file = inbox / "test_file.txt"
        test_file.write_text("Document content here.", encoding="utf-8")

        with patch.object(ingest_inbox, "INBOX_DIR", inbox):
            with patch.object(ingest_inbox, "PROCESSED_DIR", processed):
                with patch.object(ingest_inbox, "RAW_DIR", raw):
                    with patch.object(ingest_inbox, "ensure_schema"):
                        with patch.object(ingest_inbox, "upsert_seen_url"):
                            with patch.object(ingest_inbox, "set_record_map"):
                                with patch.object(ingest_inbox, "set_record_rules"):
                                    with patch.object(ingest_inbox, "add_fingerprint"):
                                        result = ingest_inbox.process_file(
                                            str(test_file)
                                        )

        assert result is not None
        assert result["title"] == "test_file"
        assert result["source"] == "inbox"
        assert "file://inbox/test_file.txt" in result["url"]
        assert "Document content here." in result["content"]


class TestMoveToProcessed:
    def test_moves_file_to_processed_dir(self, tmp_path):
        inbox = tmp_path / "inbox"
        processed = inbox / "processed"
        inbox.mkdir(parents=True)
        processed.mkdir(parents=True)

        test_file = inbox / "to_process.txt"
        test_file.write_text("content", encoding="utf-8")

        with patch.object(ingest_inbox, "INBOX_DIR", inbox):
            with patch.object(ingest_inbox, "PROCESSED_DIR", processed):
                ingest_inbox.move_to_processed(str(test_file))

        assert not test_file.exists()
        assert (processed / "to_process.txt").exists()


class TestMainIngestInbox:
    def test_main_creates_records_for_inbox_files(self, tmp_path):
        inbox = tmp_path / "data" / "inbox"
        processed = inbox / "processed"
        raw = tmp_path / "data" / "raw"
        inbox.mkdir(parents=True)
        processed.mkdir(parents=True)
        raw.mkdir(parents=True)

        (inbox / "doc1.txt").write_text("First document content.", encoding="utf-8")
        (inbox / "doc2.md").write_text(
            "# Second Document\n\nMarkdown content.", encoding="utf-8"
        )

        with patch.object(ingest_inbox, "INBOX_DIR", inbox):
            with patch.object(ingest_inbox, "PROCESSED_DIR", processed):
                with patch.object(ingest_inbox, "RAW_DIR", raw):
                    with patch.object(ingest_inbox, "ensure_schema"):
                        with patch.object(ingest_inbox, "upsert_seen_url"):
                            with patch.object(ingest_inbox, "set_record_map"):
                                with patch.object(ingest_inbox, "set_record_rules"):
                                    with patch.object(ingest_inbox, "add_fingerprint"):
                                        created = ingest_inbox.main()

        assert len(created) == 2
        for record_id in created:
            assert (raw / f"{record_id}.txt").exists()

        assert not (inbox / "doc1.txt").exists()
        assert not (inbox / "doc2.md").exists()
        assert (processed / "doc1.txt").exists()
        assert (processed / "doc2.md").exists()

    def test_main_returns_empty_when_inbox_empty(self, tmp_path):
        inbox = tmp_path / "data" / "inbox"
        processed = inbox / "processed"
        raw = tmp_path / "data" / "raw"
        inbox.mkdir(parents=True)
        processed.mkdir(parents=True)
        raw.mkdir(parents=True)

        with patch.object(ingest_inbox, "INBOX_DIR", inbox):
            with patch.object(ingest_inbox, "PROCESSED_DIR", processed):
                with patch.object(ingest_inbox, "RAW_DIR", raw):
                    with patch.object(ingest_inbox, "ensure_schema"):
                        created = ingest_inbox.main()

        assert created == []
