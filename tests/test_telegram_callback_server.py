import importlib.util
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
MODULE_PATH = BASE_DIR / "scripts" / "telegram_callback_server.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "telegram_callback_server", MODULE_PATH
)
telegram_callback_server = importlib.util.module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(telegram_callback_server)


def test_normalize_decision_for_workflow_maps_promote():
    assert (
        telegram_callback_server.normalize_decision_for_workflow("promote")
        == "approve_and_promote"
    )


def test_normalize_decision_for_workflow_maps_weak():
    assert (
        telegram_callback_server.normalize_decision_for_workflow("weak")
        == "approve_but_weak"
    )


def test_normalize_decision_for_workflow_keeps_approve_reject():
    assert telegram_callback_server.normalize_decision_for_workflow("approve") == "approve"
    assert telegram_callback_server.normalize_decision_for_workflow("reject") == "reject"
