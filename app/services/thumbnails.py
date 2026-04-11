import shutil
import subprocess
import tempfile
from pathlib import Path


SUPPORTED_DOC_THUMB_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
}

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}


def _find_quicklook_output(tmp_dir: Path, source_file: Path) -> Path | None:
    stem = source_file.stem
    direct = sorted(
        [path for path in tmp_dir.glob(f"{stem}*") if path.suffix.lower() in IMAGE_EXTENSIONS],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if direct:
        return direct[0]

    any_image = sorted(
        [path for path in tmp_dir.iterdir() if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return any_image[0] if any_image else None


def doc_thumbnail_candidates(resource_id: str, thumbnails_root: Path) -> list[Path]:
    return sorted(thumbnails_root.glob(f"{resource_id}.*"))


def ensure_doc_thumbnail(source_path: str, resource_id: str, thumbnails_root: Path) -> Path | None:
    source_file = Path(source_path)
    if not source_file.exists():
        return None
    if source_file.suffix.lower() not in SUPPORTED_DOC_THUMB_EXTENSIONS:
        return None

    existing = doc_thumbnail_candidates(resource_id, thumbnails_root)
    if existing:
        return existing[0]

    qlmanage = shutil.which("qlmanage")
    if not qlmanage:
        return None

    thumbnails_root.mkdir(parents=True, exist_ok=True)
    target_stem = thumbnails_root / resource_id

    with tempfile.TemporaryDirectory(prefix="docume_thumb_") as tmp_name:
        tmp_dir = Path(tmp_name)
        try:
            subprocess.run(
                [qlmanage, "-t", "-s", "640", "-o", str(tmp_dir), str(source_file)],
                capture_output=True,
                check=False,
                timeout=25,
                text=True,
            )
        except Exception:  # noqa: BLE001
            return None

        generated = _find_quicklook_output(tmp_dir, source_file)
        if not generated:
            return None

        final_path = target_stem.with_suffix(generated.suffix.lower())
        try:
            shutil.copy2(generated, final_path)
        except Exception:  # noqa: BLE001
            return None

        for candidate in doc_thumbnail_candidates(resource_id, thumbnails_root):
            if candidate != final_path:
                try:
                    candidate.unlink(missing_ok=True)
                except Exception:  # noqa: BLE001
                    continue
        return final_path
