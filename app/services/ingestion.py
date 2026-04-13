import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse, urlunparse
from uuid import uuid4

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Resource
from app.services.extractors import extract_from_file, extract_from_link
from app.services.ollama_client import OllamaClassifier
from app.services.search_index import build_search_text
from app.services.storage import save_file_bytes, save_in_thematic_folder


class IngestionService:
    def __init__(self, settings: Settings, classifier: OllamaClassifier):
        self.settings = settings
        self.classifier = classifier

    def _touch_duplicate(self, db: Session, resource: Resource) -> Resource:
        resource.uploaded_at = datetime.now(timezone.utc)
        if not (resource.search_text or "").strip():
            labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
            resource.search_text = build_search_text(
                title=resource.title,
                description=resource.description,
                summary=resource.summary,
                content_text=resource.content_text,
                source_url=resource.source_url,
                author_name=resource.author_name,
                inferred_theme=resource.inferred_theme,
                inferred_subtheme=resource.inferred_subtheme,
                canonical_theme=resource.canonical_theme,
                keywords=resource.keywords or [],
                llm_labels=labels,
            )
        db.add(resource)
        db.commit()
        db.refresh(resource)
        return resource

    def _sanitize_text(self, value: str | None, *, max_len: int | None = None) -> str | None:
        if value is None:
            return None
        cleaned = str(value).replace("\x00", " ")
        cleaned = re.sub(r"[\x01-\x08\x0B\x0C\x0E-\x1F]+", " ", cleaned)
        if max_len and max_len > 0:
            cleaned = cleaned[:max_len]
        return cleaned

    def _sanitize_json_like(self, value):
        if isinstance(value, dict):
            return {str(self._sanitize_text(str(k))): self._sanitize_json_like(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize_json_like(item) for item in value]
        if isinstance(value, str):
            return self._sanitize_text(value)
        return value

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

        inferred_title = self._sanitize_text(title or extracted.get("title") or url, max_len=500) or url
        extracted_text = self._sanitize_text(extracted.get("text") or "", max_len=self.settings.max_extract_chars) or ""
        extracted_description = self._sanitize_text(description or extracted.get("description"))
        source_name = self._sanitize_text(
            extracted.get("site_name") or urlparse(normalized_url).netloc or inferred_title,
            max_len=500,
        )
        lowered_url = normalized_url.lower()
        is_youtube_source = bool(youtube_id) or ("youtube.com" in lowered_url) or ("youtu.be" in lowered_url)
        searchable_text = self._sanitize_text(
            extracted_text
            or " ".join(part for part in [inferred_title, extracted_description or "", normalized_url] if part),
            max_len=self.settings.max_extract_chars,
        )
        author_name = self.classifier._sanitize_author_name(extracted.get("author"))
        if not author_name:
            author_name = await self.classifier.infer_author_name(
                source_type="link",
                title=inferred_title,
                description=extracted_description,
                extracted_text=searchable_text or "",
                source_url=normalized_url,
                source_name=source_name,
                metadata_hints={
                    "author": extracted.get("author"),
                    "youtube_channel": extracted.get("youtube_channel"),
                    "site_name": extracted.get("site_name"),
                    "domain": extracted.get("domain"),
                    "section": extracted.get("section"),
                },
            )
            author_name = self.classifier._sanitize_author_name(author_name)
        if not author_name and not is_youtube_source:
            author_name = self.classifier._sanitize_author_name(source_name)
        now_utc = datetime.now(timezone.utc)

        classification = await self.classifier.classify(
            source_type="link",
            title=inferred_title,
            description=extracted_description,
            extracted_text=searchable_text or "",
            mime_type="text/html",
            source_url=url,
            source_name=source_name,
        )
        taxonomy_author = self.classifier._sanitize_author_name(classification.taxonomy_author)
        final_author = taxonomy_author or author_name

        safe_keywords = self._sanitize_json_like(classification.keywords) or []
        safe_llm_labels = self._sanitize_json_like(
            {
                "fallback_used": classification.fallback_used,
                "model": classification.model_used,
                "classification_source": "llm-content-v5-type-genre-author-title",
                "tipologia_documento": classification.document_type,
                "contenuto": classification.semantic_theme or classification.theme,
                "dettaglio_contenuto": classification.semantic_subtheme or classification.subtheme,
                "taxonomy_type": classification.taxonomy_domain or classification.canonical_theme,
                "taxonomy_genre": classification.taxonomy_subdomain,
                "taxonomy_title": classification.taxonomy_work,
                "taxonomy_domain": classification.taxonomy_domain or classification.canonical_theme,
                "taxonomy_subdomain": classification.taxonomy_subdomain,
                "taxonomy_author": classification.taxonomy_author,
                "taxonomy_work": classification.taxonomy_work,
                "taxonomy_path": classification.taxonomy_path,
                "author": final_author,
                "tags": safe_keywords,
                "preview_image_url": self._sanitize_text(extracted.get("preview_image_url"), max_len=1000),
            }
        )
        safe_llm_raw = self._sanitize_json_like(classification.raw)

        resource = Resource(
            id=str(uuid4()),
            source_type="link",
            title=self._sanitize_text(classification.title, max_len=500) or inferred_title,
            description=extracted_description,
            source_url=normalized_url,
            youtube_video_id=extracted.get("youtube_video_id") or youtube_id,
            mime_type="text/html",
            size_bytes=None,
            sha256=None,
            language=classification.language,
            author_name=final_author,
            inferred_theme=self._sanitize_text(classification.theme, max_len=120) or "General",
            inferred_subtheme=self._sanitize_text(classification.subtheme, max_len=120),
            canonical_theme=self._sanitize_text(classification.canonical_theme, max_len=120),
            keywords=safe_keywords,
            summary=self._sanitize_text(classification.summary, max_len=1200),
            content_text=searchable_text,
            search_text=build_search_text(
                title=self._sanitize_text(classification.title, max_len=500) or inferred_title,
                description=extracted_description,
                summary=self._sanitize_text(classification.summary, max_len=1200),
                content_text=searchable_text,
                source_url=normalized_url,
                author_name=final_author,
                inferred_theme=self._sanitize_text(classification.theme, max_len=120) or "General",
                inferred_subtheme=self._sanitize_text(classification.subtheme, max_len=120),
                canonical_theme=self._sanitize_text(classification.canonical_theme, max_len=120),
                keywords=safe_keywords,
                llm_labels=safe_llm_labels,
            ),
            relevance_score=classification.relevance_score,
            conceptual_score=classification.conceptual_score,
            combined_score=classification.combined_score,
            llm_labels=safe_llm_labels,
            llm_raw=safe_llm_raw,
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

        inferred_title = self._sanitize_text(title or filename, max_len=500) or filename
        extracted_text = self._sanitize_text(extracted.get("text") or "", max_len=self.settings.max_extract_chars) or ""
        sanitized_description = self._sanitize_text(description)
        searchable_text = self._sanitize_text(
            extracted_text or " ".join(part for part in [inferred_title, sanitized_description or ""] if part),
            max_len=self.settings.max_extract_chars,
        )
        author_name = self.classifier._sanitize_author_name(extracted.get("author"))
        if not author_name:
            author_name = await self.classifier.infer_author_name(
                source_type="file",
                title=inferred_title,
                description=sanitized_description,
                extracted_text=searchable_text or "",
                source_url=None,
                source_name=filename,
                metadata_hints={
                    "author": extracted.get("author"),
                    "filename": filename,
                    "mime_type": mime_type,
                },
            )
            author_name = self.classifier._sanitize_author_name(author_name)
        now_utc = datetime.now(timezone.utc)

        classification = await self.classifier.classify(
            source_type="file",
            title=inferred_title,
            description=sanitized_description,
            extracted_text=searchable_text or "",
            mime_type=mime_type,
            source_url=None,
            source_name=filename,
            image_b64=extracted.get("image_b64"),
        )
        taxonomy_author = self.classifier._sanitize_author_name(classification.taxonomy_author)
        final_author = taxonomy_author or author_name

        safe_keywords = self._sanitize_json_like(classification.keywords) or []
        safe_llm_labels = self._sanitize_json_like(
            {
                "fallback_used": classification.fallback_used,
                "model": classification.model_used,
                "classification_source": "llm-content-v5-type-genre-author-title",
                "tipologia_documento": classification.document_type,
                "contenuto": classification.semantic_theme or classification.theme,
                "dettaglio_contenuto": classification.semantic_subtheme or classification.subtheme,
                "taxonomy_type": classification.taxonomy_domain or classification.canonical_theme,
                "taxonomy_genre": classification.taxonomy_subdomain,
                "taxonomy_title": classification.taxonomy_work,
                "taxonomy_domain": classification.taxonomy_domain or classification.canonical_theme,
                "taxonomy_subdomain": classification.taxonomy_subdomain,
                "taxonomy_author": classification.taxonomy_author,
                "taxonomy_work": classification.taxonomy_work,
                "taxonomy_path": classification.taxonomy_path,
                "author": final_author,
                "tags": safe_keywords,
                "preview_image_url": None,
            }
        )
        safe_llm_raw = self._sanitize_json_like(classification.raw)

        resource = Resource(
            id=str(uuid4()),
            source_type="file",
            title=self._sanitize_text(classification.title, max_len=500) or inferred_title,
            description=sanitized_description,
            source_url=None,
            youtube_video_id=None,
            stored_path=saved["stored_path"],
            mime_type=mime_type,
            size_bytes=saved["size_bytes"],
            sha256=saved["sha256"],
            language=classification.language,
            author_name=final_author,
            inferred_theme=self._sanitize_text(classification.theme, max_len=120) or "General",
            inferred_subtheme=self._sanitize_text(classification.subtheme, max_len=120),
            canonical_theme=self._sanitize_text(classification.canonical_theme, max_len=120),
            keywords=safe_keywords,
            summary=self._sanitize_text(classification.summary, max_len=1200),
            content_text=searchable_text,
            search_text=build_search_text(
                title=self._sanitize_text(classification.title, max_len=500) or inferred_title,
                description=sanitized_description,
                summary=self._sanitize_text(classification.summary, max_len=1200),
                content_text=searchable_text,
                source_url=None,
                author_name=final_author,
                inferred_theme=self._sanitize_text(classification.theme, max_len=120) or "General",
                inferred_subtheme=self._sanitize_text(classification.subtheme, max_len=120),
                canonical_theme=self._sanitize_text(classification.canonical_theme, max_len=120),
                keywords=safe_keywords,
                llm_labels=safe_llm_labels,
            ),
            relevance_score=classification.relevance_score,
            conceptual_score=classification.conceptual_score,
            combined_score=classification.combined_score,
            llm_labels=safe_llm_labels,
            llm_raw=safe_llm_raw,
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
