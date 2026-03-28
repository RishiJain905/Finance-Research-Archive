import shutil
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
VERIFY_DIR = BASE_DIR / "data" / "verify"
LEGACY_VERIFICATION_DIRS = [
    BASE_DIR / "data" / "accepted",
    BASE_DIR / "data" / "rejected",
    BASE_DIR / "data" / "review_queue",
]


def verification_artifact_path(record_id: str) -> Path:
    return VERIFY_DIR / f"{record_id}_verification.json"


def legacy_verification_paths(record_id: str) -> list[Path]:
    return [directory / f"{record_id}_verification.json" for directory in LEGACY_VERIFICATION_DIRS]


def resolve_verification_artifact_path(record_id: str) -> Path:
    target_path = verification_artifact_path(record_id)
    if target_path.exists():
        return target_path

    for legacy_path in legacy_verification_paths(record_id):
        if legacy_path.exists():
            return legacy_path

    return target_path


def canonicalize_verification_artifact(record_id: str) -> Path:
    target_path = verification_artifact_path(record_id)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if target_path.exists():
        for legacy_path in legacy_verification_paths(record_id):
            if legacy_path.exists():
                legacy_path.unlink()
        return target_path

    for legacy_path in legacy_verification_paths(record_id):
        if legacy_path.exists():
            shutil.move(str(legacy_path), str(target_path))
            return target_path

    return target_path
