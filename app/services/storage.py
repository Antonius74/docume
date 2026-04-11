import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.models import Resource

_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
_THEME_PATTERN = re.compile(r"[^a-z0-9]+")


def sanitize_filename(filename: str | None) -> str:
    base = Path(filename or "uploaded").name
    cleaned = _FILENAME_PATTERN.sub("_", base).strip("._")
    return (cleaned or "uploaded.bin")[:180]


def slugify_theme(theme: str | None) -> str:
    candidate = (theme or "Uncategorized").strip().lower()
    slug = _THEME_PATTERN.sub("-", candidate).strip("-")
    return slug or "uncategorized"


def source_macro_bucket(source_type: str | None) -> str:
    value = (source_type or "").strip().lower()
    if value == "link":
        return "link"
    if value == "file":
        return "doc"
    return slugify_theme(value or "doc")


def detail_macro_bucket(resource: Resource) -> str:
    labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
    detail_value = (
        resource.inferred_subtheme
        or labels.get("dettaglio_contenuto")
        or labels.get("detail")
        or "generale"
    )
    return slugify_theme(str(detail_value))


def author_macro_bucket(resource: Resource) -> str:
    labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
    author_value = (
        resource.author_name
        or labels.get("author")
        or labels.get("autore")
        or labels.get("youtube_channel")
        or "sconosciuto"
    )
    return slugify_theme(str(author_value))


def save_file_bytes(data: bytes, filename: str, files_root: Path) -> dict[str, str | int]:
    now = datetime.now(timezone.utc)
    bucket = files_root / str(now.year) / f"{now.month:02d}"
    bucket.mkdir(parents=True, exist_ok=True)

    safe_name = sanitize_filename(filename)
    final_name = f"{uuid4().hex}_{safe_name}"
    final_path = bucket / final_name

    final_path.write_bytes(data)

    return {
        "stored_path": str(final_path.resolve()),
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def _write_link_note(resource: Resource, theme_dir: Path) -> Path:
    note_path = theme_dir / f"{resource.id}.md"
    labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
    content = {
        "id": resource.id,
        "title": resource.title,
        "document_type": labels.get("tipologia_documento"),
        "canonical_theme": resource.canonical_theme,
        "theme": resource.inferred_theme,
        "subtheme": resource.inferred_subtheme,
        "author": resource.author_name,
        "keywords": resource.keywords,
        "summary": resource.summary,
        "source_url": resource.source_url,
        "uploaded_at": resource.uploaded_at.isoformat() if resource.uploaded_at else None,
        "relevance_score": resource.relevance_score,
        "conceptual_score": resource.conceptual_score,
        "combined_score": resource.combined_score,
    }
    markdown = "\n".join(
        [
            f"# {resource.title}",
            "",
            f"- Document Type: **{labels.get('tipologia_documento') or 'N/A'}**",
            f"- Canonical Theme: **{resource.canonical_theme or resource.inferred_theme}**",
            f"- Theme: **{resource.inferred_theme}**",
            f"- Subtheme: {resource.inferred_subtheme or 'N/A'}",
            f"- Author: {resource.author_name or labels.get('author') or 'N/A'}",
            f"- Source URL: {resource.source_url or 'N/A'}",
            f"- Uploaded: {resource.uploaded_at.isoformat() if resource.uploaded_at else 'N/A'}",
            "",
            "## Summary",
            resource.summary or "No summary available.",
            "",
            "## Metadata",
            "```json",
            json.dumps(content, ensure_ascii=False, indent=2),
            "```",
            "",
        ]
    )
    note_path.write_text(markdown, encoding="utf-8")
    return note_path


def save_in_thematic_folder(resource: Resource, themes_root: Path) -> str:
    source_bucket = source_macro_bucket(resource.source_type)
    content_bucket = slugify_theme(resource.canonical_theme or resource.inferred_theme)
    author_bucket = author_macro_bucket(resource)
    detail_bucket = detail_macro_bucket(resource)

    theme_dir = themes_root / source_bucket / content_bucket / author_bucket / detail_bucket
    theme_dir.mkdir(parents=True, exist_ok=True)

    if resource.stored_path:
        source_path = Path(resource.stored_path)
        if source_path.exists():
            target = theme_dir / f"{resource.id}_{source_path.name}"
            if not target.exists():
                try:
                    target.symlink_to(source_path.resolve())
                except OSError:
                    shutil.copy2(source_path, target)
            _cleanup_previous_thematic_path(resource, target, themes_root)
            # Keep thematic path anchored to the theme folder for UI navigation.
            return str(target.absolute())

    note = _write_link_note(resource, theme_dir)
    _cleanup_previous_thematic_path(resource, note, themes_root)
    return str(note.absolute())


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:  # noqa: BLE001
        return False


def _safe_unlink(path: Path) -> bool:
    try:
        if path.is_file() or path.is_symlink():
            path.unlink(missing_ok=True)
            return True
    except Exception:  # noqa: BLE001
        return False
    return False


def _cleanup_previous_thematic_path(resource: Resource, current_target: Path, themes_root: Path) -> None:
    previous = Path(resource.thematic_path) if resource.thematic_path else None
    if not previous:
        return
    if not _is_within(previous, themes_root):
        return
    try:
        if previous.resolve() == current_target.resolve():
            return
    except Exception:  # noqa: BLE001
        if str(previous.absolute()) == str(current_target.absolute()):
            return
    if _safe_unlink(previous):
        _prune_empty_parents(previous.parent, themes_root)


def _prune_empty_parents(start: Path, stop: Path) -> None:
    current = start
    stop_resolved = stop.resolve()
    while True:
        try:
            current_resolved = current.resolve()
        except Exception:  # noqa: BLE001
            return
        if current_resolved == stop_resolved:
            return
        if not _is_within(current_resolved, stop_resolved):
            return
        try:
            current.rmdir()
        except OSError:
            return
        parent = current.parent
        if parent == current:
            return
        current = parent


def remove_resource_artifacts(
    resource: Resource,
    files_root: Path,
    themes_root: Path,
    thumbnails_root: Path | None = None,
) -> dict[str, list[str]]:
    removed_paths: list[str] = []

    # Remove current thematic reference path if present.
    thematic_path = Path(resource.thematic_path) if resource.thematic_path else None
    if thematic_path and _is_within(thematic_path, themes_root):
        if _safe_unlink(thematic_path):
            removed_paths.append(str(thematic_path))
            _prune_empty_parents(thematic_path.parent, themes_root)

    # Remove any stale thematic references for the same resource id.
    for candidate in themes_root.rglob(f"{resource.id}*"):
        if not _is_within(candidate, themes_root):
            continue
        if _safe_unlink(candidate):
            path_str = str(candidate)
            if path_str not in removed_paths:
                removed_paths.append(path_str)
            _prune_empty_parents(candidate.parent, themes_root)

    # Remove original stored file.
    stored_path = Path(resource.stored_path) if resource.stored_path else None
    if stored_path and _is_within(stored_path, files_root):
        if _safe_unlink(stored_path):
            removed_paths.append(str(stored_path))
            _prune_empty_parents(stored_path.parent, files_root)

    if thumbnails_root:
        try:
            thumbnails_root_resolved = thumbnails_root.resolve()
        except Exception:  # noqa: BLE001
            thumbnails_root_resolved = thumbnails_root
        for thumb in thumbnails_root_resolved.glob(f"{resource.id}.*"):
            if _safe_unlink(thumb):
                removed_paths.append(str(thumb))

    return {"removed_paths": removed_paths}
