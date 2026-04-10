import hashlib
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse, urlunparse
from uuid import uuid4

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Resource
from app.services.extractors import extract_from_file, extract_from_link
from app.services.ollama_client import OllamaClassifier
from app.services.storage import save_file_bytes, save_in_thematic_folder


class IngestionService:
    def __init__(self, settings: Settings, classifier: OllamaClassifier):
        self.settings = settings
        self.classifier = classifier

    def _touch_duplicate(self, db: Session, resource: Resource) -> Resource:
        resource.uploaded_at = datetime.now(timezone.utc)
        db.add(resource)
        db.commit()
        db.refresh(resource)
        return resource

    def _youtube_id(self, url: str) -> str | None:
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        if "youtu.be" in host:
            return parsed.path.strip("/") or None

        if "youtube.com" in host:
            if parsed.path == "/watch":
                return parse_qs(parsed.query).get("v", [None])[0]
            if parsed.path.startswith("/shorts/"):
                return parsed.path.split("/shorts/")[-1].split("/")[0]
            if parsed.path.startswith("/embed/"):
                return parsed.path.split("/embed/")[-1].split("/")[0]

        return None

    def _normalize_link_url(self, url: str) -> str:
        parsed = urlparse(url)
        youtube_id = self._youtube_id(url)
        if youtube_id:
            return f"https://www.youtube.com/watch?v={youtube_id}"

        clean_path = parsed.path.rstrip("/") or parsed.path
        normalized = parsed._replace(path=clean_path, fragment="")
        return urlunparse(normalized)

    async def ingest_link(
        self,
        db: Session,
        *,
        url: str,
        title: str | None,
        description: str | None,
    ) -> Resource:
        normalized_url = self._normalize_link_url(url)
        youtube_id = self._youtube_id(normalized_url)

        duplicate_query = select(Resource).where(Resource.source_type == "link")
        duplicate_predicates = [Resource.source_url == url, Resource.source_url == normalized_url]
        if youtube_id:
            duplicate_predicates.append(Resource.youtube_video_id == youtube_id)
        duplicate_query = duplicate_query.where(or_(*duplicate_predicates))

        duplicate = db.scalars(duplicate_query).first()
        if duplicate:
            return self._touch_duplicate(db, duplicate)

        extracted = await extract_from_link(
            url,
            timeout_seconds=self.settings.request_timeout_seconds,
            max_chars=self.settings.max_extract_chars,
        )

        inferred_title = title or extracted.get("title") or url
        extracted_text = extracted.get("text") or ""
        extracted_description = description or extracted.get("description")
        source_name = extracted.get("site_name") or urlparse(normalized_url).netloc or inferred_title
        now_utc = datetime.now(timezone.utc)

        classification = await self.classifier.classify(
            source_type="link",
            title=inferred_title,
            description=extracted_description,
            extracted_text=extracted_text,
            mime_type="text/html",
            source_url=url,
            source_name=source_name,
        )

        resource = Resource(
            id=str(uuid4()),
            source_type="link",
            title=classification.title,
            description=extracted_description,
            source_url=normalized_url,
            youtube_video_id=extracted.get("youtube_video_id") or youtube_id,
            mime_type="text/html",
            size_bytes=None,
            sha256=None,
            language=classification.language,
            inferred_theme=classification.theme,
            inferred_subtheme=classification.subtheme,
            canonical_theme=classification.canonical_theme,
            keywords=classification.keywords,
            summary=classification.summary,
            content_text=extracted_text,
            relevance_score=classification.relevance_score,
            conceptual_score=classification.conceptual_score,
            combined_score=classification.combined_score,
            llm_labels={
                "fallback_used": classification.fallback_used,
                "model": classification.model_used,
                "classification_source": "llm-content-v2",
            },
            llm_raw=classification.raw,
            status="processed",
            uploaded_at=now_utc,
            processed_at=now_utc,
        )

        db.add(resource)
        db.flush()

        resource.thematic_path = save_in_thematic_folder(resource, self.settings.themes_root)

        db.commit()
        db.refresh(resource)
        return resource

    async def ingest_file(
        self,
        db: Session,
        *,
        filename: str,
        mime_type: str | None,
        file_data: bytes,
        title: str | None,
        description: str | None,
    ) -> Resource:
        file_hash = hashlib.sha256(file_data).hexdigest()
        duplicate = db.scalars(
            select(Resource).where(Resource.source_type == "file", Resource.sha256 == file_hash)
        ).first()
        if duplicate:
            return self._touch_duplicate(db, duplicate)

        saved = save_file_bytes(file_data, filename, self.settings.files_root)

        extracted = extract_from_file(
            saved["stored_path"],
            mime_type=mime_type,
            max_chars=self.settings.max_extract_chars,
            max_document_pages=self.settings.max_document_pages,
        )

        inferred_title = title or filename
        extracted_text = extracted.get("text") or ""
        now_utc = datetime.now(timezone.utc)

        classification = await self.classifier.classify(
            source_type="file",
            title=inferred_title,
            description=description,
            extracted_text=extracted_text,
            mime_type=mime_type,
            source_url=None,
            source_name=filename,
            image_b64=extracted.get("image_b64"),
        )

        resource = Resource(
            id=str(uuid4()),
            source_type="file",
            title=classification.title,
            description=description,
            source_url=None,
            youtube_video_id=None,
            stored_path=saved["stored_path"],
            mime_type=mime_type,
            size_bytes=saved["size_bytes"],
            sha256=saved["sha256"],
            language=classification.language,
            inferred_theme=classification.theme,
            inferred_subtheme=classification.subtheme,
            canonical_theme=classification.canonical_theme,
            keywords=classification.keywords,
            summary=classification.summary,
            content_text=extracted_text,
            relevance_score=classification.relevance_score,
            conceptual_score=classification.conceptual_score,
            combined_score=classification.combined_score,
            llm_labels={
                "fallback_used": classification.fallback_used,
                "model": classification.model_used,
                "classification_source": "llm-content-v2",
            },
            llm_raw=classification.raw,
            status="processed",
            uploaded_at=now_utc,
            processed_at=now_utc,
        )

        db.add(resource)
        db.flush()

        resource.thematic_path = save_in_thematic_folder(resource, self.settings.themes_root)

        db.commit()
        db.refresh(resource)
        return resource
