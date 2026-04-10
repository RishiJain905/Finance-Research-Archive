import importlib.util
from pathlib import Path
from unittest.mock import patch, MagicMock
import json

BASE_DIR = Path(__file__).resolve().parent.parent
MODULE_PATH = BASE_DIR / "scripts" / "run_edgar_pipeline.py"
MODULE_SPEC = importlib.util.spec_from_file_location("run_edgar_pipeline", MODULE_PATH)
run_edgar_pipeline = importlib.util.module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(run_edgar_pipeline)


def test_extract_created_ids_parses_json_block():
    output = "some output\nJSON_OUTPUT_START[ \"record-a\", \"record-b\" ]JSON_OUTPUT_END\nmore"
    ids = run_edgar_pipeline.extract_created_ids(output)
    assert ids == ["record-a", "record-b"]


def test_extract_created_ids_empty():
    assert run_edgar_pipeline.extract_created_ids("no markers here") == []
    assert run_edgar_pipeline.extract_created_ids("JSON_OUTPUT_STARTJSON_OUTPUT_END") == []


def test_extract_created_ids_non_list():
    output = "JSON_OUTPUT_START{\"key\":\"value\"}JSON_OUTPUT_END"
    assert run_edgar_pipeline.extract_created_ids(output) == []


def test_extract_created_ids_filters_invalid():
    output = "JSON_OUTPUT_START[ \"\", \"record-a\", null, 123 ]JSON_OUTPUT_END"
    ids = run_edgar_pipeline.extract_created_ids(output)
    assert ids == ["record-a"]


def test_process_record_id_success():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
        rid, ok, err = run_edgar_pipeline.process_record_id("test-001", "/usr/bin/python")
        assert ok is True
        assert err == ""
        mock_run.assert_called_once()


def test_process_record_id_failure():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="processing error")
        rid, ok, err = run_edgar_pipeline.process_record_id("test-001", "/usr/bin/python")
        assert ok is False
        assert "processing error" in err


def test_run_command_captures_output():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="ingested 5 records", stderr="")
        out = run_edgar_pipeline.run_command(["echo", "test"], "test step")
        assert "ingested 5 records" in out


def test_run_command_raises_on_nonzero():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="died")
        try:
            run_edgar_pipeline.run_command(["false"], "failing step")
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "failing step failed with exit code 1" in str(e)
