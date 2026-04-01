import importlib.util
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
MODULE_PATH = BASE_DIR / "scripts" / "run_ingest_and_process.py"
MODULE_SPEC = importlib.util.spec_from_file_location("run_ingest_and_process", MODULE_PATH)
run_ingest_and_process = importlib.util.module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(run_ingest_and_process)


def test_select_records_to_process_new_only_and_capped():
    before = {"a", "b"}
    after = {"a", "b", "c", "d", "e"}

    selected = run_ingest_and_process.select_records_to_process(
        raw_ids_before_run=before,
        raw_ids_after_filter=after,
        include_backlog=False,
        max_records=2,
    )

    assert selected == ["c", "d"]


def test_select_records_to_process_include_backlog():
    before = {"a", "b"}
    after = {"a", "b", "c"}

    selected = run_ingest_and_process.select_records_to_process(
        raw_ids_before_run=before,
        raw_ids_after_filter=after,
        include_backlog=True,
        max_records=0,
    )

    assert selected == ["a", "b", "c"]
