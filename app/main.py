import re
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import asc, case, desc, func, or_, select, text
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import SessionLocal, get_db, init_db
from app.models import Resource
from app.schemas import (
    AuthorStatOut,
    AuthorTreeNodeOut,
    DetailNodeOut,
    IngestLinkRequest,
    ResourceListOut,
    ResourceOut,
    ThemeStatOut,
    ThemeTreeNodeOut,
)
from app.services.ingestion import IngestionService
from app.services.ollama_client import OllamaClassifier
from app.services.search_index import build_search_text
from app.services.semantic import (
    SemanticSearchService,
    score_resource_for_query,
)
from app.services.storage import remove_resource_artifacts, save_in_thematic_folder
from app.services.text_similarity import similarity_profile
from app.services.thumbnails import ensure_doc_thumbnail

settings = get_settings()
classifier = OllamaClassifier(
    base_url=settings.ollama_url,
    text_model=settings.resolved_ollama_model_text,
    image_model=settings.resolved_ollama_model_image,
    category_catalog_path=settings.categories_catalog_path,
    timeout_seconds=settings.request_timeout_seconds,
)
ingestion_service = IngestionService(settings=settings, classifier=classifier)
semantic_search_service = SemanticSearchService(
    base_url=settings.ollama_url,
    model=settings.resolved_ollama_model_text,
    timeout_seconds=settings.request_timeout_seconds,
)

app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.parsed_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

base_dir = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=base_dir / "static"), name="static")
templates = Jinja2Templates(directory=str(base_dir / "templates"))
static_dir = base_dir / "static"
_PG_TRGM_AVAILABLE: bool | None = None


def _asset_version() -> int:
    mtimes: list[int] = []
    for asset_name in ("style.css", "app.js"):
        asset_path = static_dir / asset_name
        try:
            mtimes.append(int(asset_path.stat().st_mtime))
        except OSError:
            continue
    return max(mtimes) if mtimes else int(datetime.now(timezone.utc).timestamp())


def _is_postgres_backend() -> bool:
    return settings.database_url.lower().startswith("postgresql")


def _normalized_search_terms(raw_terms: list[str], *, max_items: int = 10) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in raw_terms:
        cleaned = re.sub(r"[^0-9A-Za-zÀ-ÿ\s\-_/]+", " ", str(raw or "")).strip().lower()
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if len(cleaned) < 2:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        output.append(cleaned)
        if len(output) >= max_items:
            break
    return output


def _pg_trgm_enabled(db: Session) -> bool:
    global _PG_TRGM_AVAILABLE  # noqa: PLW0603

    if not _is_postgres_backend():
        return False
    if _PG_TRGM_AVAILABLE is not None:
        return _PG_TRGM_AVAILABLE

    try:
        result = db.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm')")
        ).scalar()
        _PG_TRGM_AVAILABLE = bool(result)
    except Exception:  # noqa: BLE001
        _PG_TRGM_AVAILABLE = False

    return _PG_TRGM_AVAILABLE


def _link_thumbnail_from_labels(resource: Resource) -> str | None:
    labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
    candidates = [
        labels.get("preview_image_url"),
        labels.get("thumbnail_url"),
        labels.get("image_url"),
    ]
    for candidate in candidates:
        value = str(candidate or "").strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value[:1200]
    return None


def _resource_thumbnail_url(resource: Resource) -> str | None:
    if resource.source_type == "file":
        mime = (resource.mime_type or "").lower()
        if mime.startswith("image/"):
            return f"/api/files/{resource.id}"
        if resource.stored_path:
            return f"/api/resources/{resource.id}/thumbnail"
        return None

    if resource.source_type == "link":
        if resource.youtube_video_id:
            return f"https://i.ytimg.com/vi/{resource.youtube_video_id}/hqdefault.jpg"

        link_thumb = _link_thumbnail_from_labels(resource)
        if link_thumb:
            return link_thumb

        if resource.source_url:
            try:
                host = (urlparse(resource.source_url).hostname or "").strip()
            except Exception:  # noqa: BLE001
                host = ""
            if host:
                return f"https://www.google.com/s2/favicons?domain={host}&sz=128"

    return None


def _attach_thumbnail(resource: Resource) -> Resource:
    setattr(resource, "thumbnail_url", _resource_thumbnail_url(resource))
    return resource


def _attach_thumbnails(resources: list[Resource]) -> list[Resource]:
    for resource in resources:
        _attach_thumbnail(resource)
    return resources


@app.on_event("startup")
async def on_startup() -> None:
    settings.ensure_storage_paths()
    init_db()
    await _reclassify_existing_resources()


async def _reclassify_existing_resources() -> None:
    db = SessionLocal()
    try:
        all_resources = db.scalars(select(Resource)).all()
        resources = []
        prefill_changed = False

        for resource in all_resources:
            base_content = (
                resource.content_text
                or " ".join(
                    part
                    for part in [resource.summary, resource.description, resource.title, resource.source_url]
                    if part
                )
            )[: settings.max_extract_chars]
            if resource.content_text != base_content:
                resource.content_text = base_content
                prefill_changed = True

            labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
            author_changed = False
            normalized_author = classifier._sanitize_author_name(resource.author_name) if resource.author_name else None
            if not normalized_author:
                fallback_author = (
                    labels.get("author")
                    or labels.get("youtube_channel")
                    or None
                )
                is_youtube_link = False
                if resource.source_type == "link" and resource.source_url:
                    lowered_url = resource.source_url.lower()
                    is_youtube_link = "youtube.com" in lowered_url or "youtu.be" in lowered_url
                if (
                    not fallback_author
                    and resource.source_type == "link"
                    and resource.source_url
                    and not is_youtube_link
                ):
                    fallback_author = (urlparse(resource.source_url).netloc or "").replace("www.", "")
                normalized_author = classifier._sanitize_author_name(fallback_author) if fallback_author else None
            if resource.author_name != normalized_author:
                resource.author_name = normalized_author
                prefill_changed = True
                author_changed = True

            current_search_text = build_search_text(
                title=resource.title,
                description=resource.description,
                summary=resource.summary,
                content_text=base_content,
                source_url=resource.source_url,
                author_name=resource.author_name,
                inferred_theme=resource.inferred_theme,
                inferred_subtheme=resource.inferred_subtheme,
                canonical_theme=resource.canonical_theme,
                keywords=resource.keywords or [],
                llm_labels=labels if isinstance(labels, dict) else {},
            )
            if resource.search_text != current_search_text:
                resource.search_text = current_search_text
                prefill_changed = True

            # If the LLM inferred a specific theme but canonical theme is still
            # "General", align canonical taxonomy without waiting for full re-run.
            inferred_canonical = classifier._normalize_canonical_theme(
                resource.inferred_theme,
                allow_create=True,
            )
            canonical_current = (resource.canonical_theme or "General").strip()
            if canonical_current.lower() == "general" and inferred_canonical != "General":
                resource.canonical_theme = inferred_canonical
                prefill_changed = True
                remapped_thematic_path = save_in_thematic_folder(resource, settings.themes_root)
                if resource.thematic_path != remapped_thematic_path:
                    resource.thematic_path = remapped_thematic_path
                    prefill_changed = True

            # Keep thematic path aligned for existing rows too, including migration
            # from legacy layouts to macro layout:
            # /themes/<tipo>/<genere>/<autore>/<titolo>/<file>
            thematic_path = resource.thematic_path or ""
            has_macro_layout = False
            if "/themes/" in thematic_path:
                relative = thematic_path.split("/themes/", 1)[1]
                segments = [segment for segment in relative.split("/") if segment]
                if len(segments) >= 5:
                    has_macro_layout = True
            if not has_macro_layout:
                refreshed_thematic_path = save_in_thematic_folder(resource, settings.themes_root)
                if resource.thematic_path != refreshed_thematic_path:
                    resource.thematic_path = refreshed_thematic_path
                    prefill_changed = True
            elif author_changed:
                refreshed_thematic_path = save_in_thematic_folder(resource, settings.themes_root)
                if resource.thematic_path != refreshed_thematic_path:
                    resource.thematic_path = refreshed_thematic_path
                    prefill_changed = True

            if labels.get("classification_source") != "llm-content-v5-type-genre-author-title" or labels.get("fallback_used") is True:
                resources.append(resource)

        if not resources:
            if prefill_changed:
                db.commit()
            return

        changed = prefill_changed

        for resource in resources:
            base_content = resource.content_text or ""
            classification = await classifier.classify(
                source_type=resource.source_type,
                title=resource.title,
                description=resource.description,
                extracted_text=base_content,
                mime_type=resource.mime_type,
                source_url=resource.source_url,
                source_name=resource.title,
            )

            if resource.title != classification.title:
                resource.title = classification.title
                changed = True
            if resource.inferred_theme != classification.theme:
                resource.inferred_theme = classification.theme
                changed = True
            if resource.inferred_subtheme != classification.subtheme:
                resource.inferred_subtheme = classification.subtheme
                changed = True
            if resource.canonical_theme != classification.canonical_theme:
                resource.canonical_theme = classification.canonical_theme
                changed = True
            if resource.summary != classification.summary:
                resource.summary = classification.summary
                changed = True
            if resource.keywords != classification.keywords:
                resource.keywords = classification.keywords
                changed = True
            if resource.language != classification.language:
                resource.language = classification.language
                changed = True
            taxonomy_author = classifier._sanitize_author_name(classification.taxonomy_author)
            if taxonomy_author and resource.author_name != taxonomy_author:
                resource.author_name = taxonomy_author
                changed = True
            llm_labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
            if not resource.author_name:
                inferred_author = await classifier.infer_author_name(
                    source_type=resource.source_type,
                    title=resource.title,
                    description=resource.description,
                    extracted_text=base_content,
                    source_url=resource.source_url,
                    source_name=resource.title,
                    metadata_hints=llm_labels if isinstance(llm_labels, dict) else {},
                )
                inferred_author = classifier._sanitize_author_name(inferred_author)
                if inferred_author and resource.author_name != inferred_author:
                    resource.author_name = inferred_author
                    changed = True
            if resource.relevance_score != classification.relevance_score:
                resource.relevance_score = classification.relevance_score
                changed = True
            if resource.conceptual_score != classification.conceptual_score:
                resource.conceptual_score = classification.conceptual_score
                changed = True
            if resource.combined_score != classification.combined_score:
                resource.combined_score = classification.combined_score
                changed = True

            updated_labels = {
                **llm_labels,
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
                "author": resource.author_name,
                "tags": classification.keywords,
            }
            if resource.llm_labels != updated_labels:
                resource.llm_labels = updated_labels
                changed = True
            if resource.llm_raw != classification.raw:
                resource.llm_raw = classification.raw
                changed = True
            now_utc = datetime.now(timezone.utc)
            resource.processed_at = now_utc
            changed = True

            new_thematic_path = save_in_thematic_folder(resource, settings.themes_root)
            if resource.thematic_path != new_thematic_path:
                resource.thematic_path = new_thematic_path
                changed = True

            refreshed_search_text = build_search_text(
                title=resource.title,
                description=resource.description,
                summary=resource.summary,
                content_text=base_content,
                source_url=resource.source_url,
                author_name=resource.author_name,
                inferred_theme=resource.inferred_theme,
                inferred_subtheme=resource.inferred_subtheme,
                canonical_theme=resource.canonical_theme,
                keywords=resource.keywords or [],
                llm_labels=resource.llm_labels if isinstance(resource.llm_labels, dict) else {},
            )
            if resource.search_text != refreshed_search_text:
                resource.search_text = refreshed_search_text
                changed = True

        if changed:
            db.commit()
    finally:
        db.close()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "ollama_model": (
                f"testo: {settings.resolved_ollama_model_text} | "
                f"immagini: {settings.resolved_ollama_model_image}"
            ),
            "asset_version": _asset_version(),
        },
    )


@app.get("/health")
def health():
    return {"status": "ok", "service": settings.app_name}


@app.post("/api/ingest/file", response_model=ResourceOut)
async def ingest_file(
    file: UploadFile = File(...),
    title: str | None = Form(default=None),
    description: str | None = Form(default=None),
    db: Session = Depends(get_db),
):
    file_data = await file.read()
    if not file_data:
        raise HTTPException(status_code=400, detail="File vuoto")

    try:
        resource = await ingestion_service.ingest_file(
            db,
            filename=file.filename or "uploaded.bin",
            mime_type=file.content_type,
            file_data=file_data,
            title=title,
            description=description,
        )
        return _attach_thumbnail(resource)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Errore ingest file: {exc}") from exc


@app.post("/api/ingest/link", response_model=ResourceOut)
async def ingest_link(payload: IngestLinkRequest, db: Session = Depends(get_db)):
    try:
        resource = await ingestion_service.ingest_link(
            db,
            url=str(payload.url),
            title=payload.title,
            description=payload.description,
        )
        return _attach_thumbnail(resource)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Errore ingest link: {exc}") from exc


@app.get("/api/resources", response_model=ResourceListOut)
def list_resources(
    q: str | None = Query(default=None),
    theme: str | None = Query(default=None),
    author: str | None = Query(default=None),
    detail: str | None = Query(default=None),
    source_type: str | None = Query(default=None),
    semantic: bool = Query(default=True),
    live: bool = Query(default=False),
    sort_by: str = Query(default="pertinence", pattern="^(pertinence|date)$"),
    order: str = Query(default="desc", pattern="^(asc|desc)$"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    def safe_uploaded_at(item: Resource) -> datetime:
        stamp = item.uploaded_at
        if stamp is None:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if stamp.tzinfo is None:
            return stamp.replace(tzinfo=timezone.utc)
        return stamp

    query = select(Resource)
    theme_expr = func.coalesce(Resource.canonical_theme, Resource.inferred_theme, "Uncategorized")
    genre_expr = func.coalesce(func.nullif(func.trim(Resource.inferred_subtheme), ""), "Generale")
    author_name_expr = func.coalesce(func.nullif(func.trim(Resource.author_name), ""), "Sconosciuto")

    normalized_q = (q or "").strip()
    expansion = None
    search_terms: list[str] = []
    rank_expr = None
    trigram_rank_expr = None
    contains_expr = None

    if normalized_q:
        use_semantic_expansion = semantic and not live
        expansion = semantic_search_service.expand_query(normalized_q, use_llm=use_semantic_expansion)
        if live:
            search_terms = _normalized_search_terms([normalized_q], max_items=4)
        else:
            search_terms = _normalized_search_terms(expansion.merged_terms(max_items=16), max_items=10)
        if not search_terms:
            search_terms = _normalized_search_terms([normalized_q], max_items=6)

        if live:
            theme_filters = []
        else:
            theme_filters = [
                func.lower(theme_expr) == target_theme.lower()
                for target_theme in (expansion.target_themes if expansion else [])
            ]

        if _is_postgres_backend():
            query_text = " ".join(search_terms)
            if not query_text:
                fallback_terms = _normalized_search_terms([normalized_q], max_items=1)
                query_text = fallback_terms[0] if fallback_terms else normalized_q
            ts_query = func.websearch_to_tsquery("simple", query_text)
            search_vector = func.to_tsvector("simple", func.coalesce(Resource.search_text, ""))
            rank_expr = func.coalesce(func.ts_rank_cd(search_vector, ts_query), 0.0)
            text_filter = search_vector.op("@@")(ts_query)
            search_expr = func.lower(func.coalesce(Resource.search_text, ""))
            contains_expr = search_expr.ilike(f"%{query_text.lower()}%")
            combined_filters = [text_filter]
            combined_filters.append(contains_expr)

            raw_tokens = [token for token in query_text.lower().split(" ") if len(token) >= 2]
            token_filters = [search_expr.ilike(f"%{token}%") for token in raw_tokens]
            prefix_filters = [search_expr.ilike(f"%{token[:3]}%") for token in raw_tokens if len(token) >= 4]
            combined_filters.extend(token_filters)
            combined_filters.extend(prefix_filters)

            if _pg_trgm_enabled(db):
                normalized_query = query_text.lower()
                trigram_rank_expr = func.similarity(search_expr, normalized_query)
                trigram_match = search_expr.op("%")(normalized_query)
                combined_filters.append(trigram_match)

            combined_filters.extend(theme_filters)
            if combined_filters:
                query = query.where(or_(*combined_filters))
        else:
            term_filters = []
            for term in search_terms:
                like = f"%{term}%"
                term_filters.append(Resource.search_text.ilike(like))
            combined_filters = [*term_filters, *theme_filters]
            if combined_filters:
                query = query.where(or_(*combined_filters))

    if theme:
        query = query.where(func.lower(theme_expr) == theme.lower())

    if author:
        query = query.where(
            or_(
                func.lower(author_name_expr) == author.lower(),
                func.lower(genre_expr) == author.lower(),
            )
        )

    if detail:
        query = query.where(func.lower(author_name_expr) == detail.lower())

    if source_type in {"file", "link"}:
        query = query.where(Resource.source_type == source_type)

    offset = (page - 1) * page_size
    reverse_order = order == "desc"

    if normalized_q and rank_expr is not None:
        if contains_expr is not None:
            contains_rank = case((contains_expr, 1.0), else_=0.0)
            rank_expr = (rank_expr * 0.60) + (contains_rank * 0.40)
        if trigram_rank_expr is not None:
            rank_expr = (func.coalesce(rank_expr, 0.0) * 0.72) + (func.coalesce(trigram_rank_expr, 0.0) * 0.28)

        if live:
            candidate_limit = max(60, page_size * max(page, 1) * 5)
            candidate_limit = min(candidate_limit, 400)
        else:
            candidate_limit = max(120, page_size * max(page, 1) * 12)
            candidate_limit = min(candidate_limit, 1500)
        candidate_query = query
        if reverse_order:
            candidate_query = candidate_query.order_by(
                desc(rank_expr),
                desc(Resource.combined_score),
                desc(Resource.uploaded_at),
            )
        else:
            candidate_query = candidate_query.order_by(
                asc(rank_expr),
                asc(Resource.combined_score),
                asc(Resource.uploaded_at),
            )
        candidates = db.scalars(candidate_query.limit(candidate_limit)).all()

        rescored: list[tuple[Resource, float]] = []
        rescored_raw: list[tuple[Resource, float, float, float, float, float, bool, bool, bool]] = []
        query_lower = normalized_q.lower()
        target_theme_set = {
            str(theme_name or "").strip().lower()
            for theme_name in (expansion.target_themes if expansion else [])
            if str(theme_name or "").strip()
        }
        if len(query_lower) <= 3:
            min_sim_threshold = 0.03
        elif len(query_lower) <= 7:
            min_sim_threshold = 0.06
        else:
            min_sim_threshold = 0.14

        for item in candidates:
            search_blob = getattr(item, "search_text", "") or ""
            full_profile = similarity_profile(query_lower, search_blob)
            title_profile = similarity_profile(query_lower, item.title or "")
            similarity = max(full_profile.score, min(1.0, title_profile.score * 1.10))
            token_coverage = max(full_profile.token_coverage, title_profile.token_coverage)
            prefix_coverage = max(full_profile.prefix_coverage, title_profile.prefix_coverage)
            exact_match = full_profile.exact_substring or title_profile.exact_substring
            title_token_coverage = title_profile.token_coverage
            title_blob = (item.title or "").lower()
            source_blob = (item.source_url or "").lower()
            keep_by_contains = query_lower in title_blob or query_lower in source_blob
            item_theme = str(item.canonical_theme or item.inferred_theme or "").strip().lower()
            theme_match = (not target_theme_set) or (item_theme in target_theme_set)

            score = score_resource_for_query(
                item,
                terms=search_terms,
                target_themes=expansion.target_themes if expansion else [],
                raw_query=normalized_q,
            )
            rescored_raw.append(
                (
                    item,
                    score,
                    similarity,
                    token_coverage,
                    prefix_coverage,
                    title_token_coverage,
                    exact_match,
                    keep_by_contains,
                    theme_match,
                )
            )

        if live and rescored_raw:
            best_similarity = max(entry[2] for entry in rescored_raw)
            dynamic_threshold = max(min_sim_threshold, best_similarity * 0.68)
            min_coverage = 0.15 if len(query_lower) <= 3 else 0.34
            absolute_floor = 0.18 if len(query_lower) <= 4 else 0.24
            rescored = [
                (item, score)
                for (
                    item,
                    score,
                    similarity,
                    token_coverage,
                    prefix_coverage,
                    title_token_coverage,
                    exact_match,
                    keep_by_contains,
                    theme_match,
                ) in rescored_raw
                if (
                    (
                        exact_match
                        or keep_by_contains
                        or (
                            similarity >= max(dynamic_threshold, absolute_floor)
                            and (token_coverage >= min_coverage or prefix_coverage >= 0.50)
                        )
                    )
                    and (
                        theme_match
                        or keep_by_contains
                        or title_token_coverage >= 0.95
                        or exact_match
                    )
                )
            ]
        else:
            rescored = [
                (item, score)
                for item, score, similarity, token_coverage, _, _, exact_match, keep_by_contains, _ in rescored_raw
                if exact_match or keep_by_contains or token_coverage >= 0.18 or similarity >= 0.20
            ]

        if sort_by == "date":
            rescored.sort(
                key=lambda pair: (
                    safe_uploaded_at(pair[0]),
                    pair[1],
                ),
                reverse=reverse_order,
            )
        else:
            rescored.sort(
                key=lambda pair: (
                    pair[1],
                    safe_uploaded_at(pair[0]),
                ),
                reverse=reverse_order,
            )

        if live:
            total = len(rescored)
        else:
            total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
        items = [pair[0] for pair in rescored[offset : offset + page_size]]
    elif normalized_q:
        candidates = db.scalars(query.limit(320)).all()
        scored = [
            (
                item,
                score_resource_for_query(
                    item,
                    terms=search_terms,
                    target_themes=expansion.target_themes if expansion else [],
                    raw_query=normalized_q,
                ),
            )
            for item in candidates
        ]

        if sort_by == "date":
            scored.sort(
                key=lambda pair: (
                    safe_uploaded_at(pair[0]),
                    pair[1],
                ),
                reverse=reverse_order,
            )
        else:
            scored.sort(
                key=lambda pair: (
                    pair[1],
                    safe_uploaded_at(pair[0]),
                ),
                reverse=reverse_order,
            )

        total = len(scored)
        items = [pair[0] for pair in scored[offset : offset + page_size]]
    else:
        total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
        sort_column = Resource.combined_score if sort_by == "pertinence" else Resource.uploaded_at
        if reverse_order:
            query = query.order_by(desc(sort_column), desc(Resource.uploaded_at))
        else:
            query = query.order_by(asc(sort_column), asc(Resource.uploaded_at))
        items = db.scalars(query.offset(offset).limit(page_size)).all()

    return ResourceListOut(total=total, page=page, page_size=page_size, items=_attach_thumbnails(items))


@app.get("/api/resources/recent", response_model=list[ResourceOut])
def recent_resources(
    limit: int = Query(default=8, ge=1, le=40),
    db: Session = Depends(get_db),
):
    rows = db.scalars(select(Resource).order_by(desc(Resource.uploaded_at)).limit(limit)).all()
    return _attach_thumbnails(rows)


@app.get("/api/resources/{resource_id}", response_model=ResourceOut)
def get_resource(resource_id: str, db: Session = Depends(get_db)):
    resource = db.get(Resource, resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail="Resource non trovata")
    return _attach_thumbnail(resource)


@app.delete("/api/resources/{resource_id}")
def delete_resource(resource_id: str, db: Session = Depends(get_db)):
    resource = db.get(Resource, resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail="Resource non trovata")

    cleanup = remove_resource_artifacts(
        resource,
        settings.files_root,
        settings.themes_root,
        settings.thumbnails_root,
    )

    db.delete(resource)
    db.commit()

    return {"status": "deleted", "id": resource_id, "removed_paths": cleanup["removed_paths"]}


@app.get("/api/resources/{resource_id}/thumbnail")
def get_resource_thumbnail(resource_id: str, db: Session = Depends(get_db)):
    resource = db.get(Resource, resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail="Resource non trovata")

    if resource.source_type == "file":
        if not resource.stored_path:
            raise HTTPException(status_code=404, detail="Thumbnail non disponibile")
        source_file = Path(resource.stored_path)
        if not source_file.exists():
            raise HTTPException(status_code=404, detail="File origine non trovato")

        mime = (resource.mime_type or "").lower()
        if mime.startswith("image/"):
            return FileResponse(path=source_file, media_type=resource.mime_type or "image/*")

        thumb = ensure_doc_thumbnail(
            source_path=str(source_file),
            resource_id=resource.id,
            thumbnails_root=settings.thumbnails_root,
        )
        if thumb and thumb.exists():
            media_type = "image/png" if thumb.suffix.lower() == ".png" else "image/jpeg"
            return FileResponse(path=thumb, media_type=media_type)
        raise HTTPException(status_code=404, detail="Thumbnail non disponibile")

    thumb_url = _resource_thumbnail_url(resource)
    if thumb_url and thumb_url.startswith(("http://", "https://")):
        return RedirectResponse(url=thumb_url, status_code=307)

    raise HTTPException(status_code=404, detail="Thumbnail non disponibile")


@app.get("/api/themes", response_model=list[ThemeStatOut])
def list_themes(db: Session = Depends(get_db)):
    theme_expr = func.coalesce(Resource.canonical_theme, Resource.inferred_theme, "Uncategorized")
    rows = db.execute(
        select(theme_expr.label("theme"), func.count(Resource.id))
        .group_by(theme_expr)
        .order_by(desc(func.count(Resource.id)), asc(theme_expr))
    ).all()

    return [ThemeStatOut(theme=row[0] or "Uncategorized", count=row[1]) for row in rows]


@app.get("/api/authors", response_model=list[AuthorStatOut])
def list_authors(
    theme: str | None = Query(default=None),
    source_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    theme_expr = func.coalesce(Resource.canonical_theme, Resource.inferred_theme, "Uncategorized")
    author_expr = func.coalesce(func.nullif(func.trim(Resource.author_name), ""), "Sconosciuto")

    query = select(author_expr.label("author"), func.count(Resource.id)).group_by(author_expr)
    if theme:
        query = query.where(func.lower(theme_expr) == theme.lower())
    if source_type in {"file", "link"}:
        query = query.where(Resource.source_type == source_type)

    rows = db.execute(query.order_by(desc(func.count(Resource.id)), asc(author_expr)).limit(200)).all()
    return [AuthorStatOut(author=row[0] or "Sconosciuto", count=row[1]) for row in rows]


@app.get("/api/theme-tree", response_model=list[ThemeTreeNodeOut])
def theme_tree(
    source_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    theme_expr = func.coalesce(Resource.canonical_theme, Resource.inferred_theme, "Uncategorized")
    genre_expr = func.coalesce(func.nullif(func.trim(Resource.inferred_subtheme), ""), "Generale")
    author_expr = func.coalesce(func.nullif(func.trim(Resource.author_name), ""), "Sconosciuto")

    query = (
        select(
            theme_expr.label("theme"),
            genre_expr.label("author"),
            author_expr.label("detail"),
            func.count(Resource.id).label("count"),
        )
        .group_by(theme_expr, genre_expr, author_expr)
    )
    if source_type in {"file", "link"}:
        query = query.where(Resource.source_type == source_type)

    rows = db.execute(query).all()
    tree: dict[str, dict] = {}

    for row in rows:
        theme_name = row.theme or "Uncategorized"
        author_name = row.author or "Sconosciuto"
        detail_name = row.detail or "Generale"
        count = int(row.count or 0)

        theme_node = tree.setdefault(
            theme_name,
            {
                "theme": theme_name,
                "count": 0,
                "authors": {},
            },
        )
        theme_node["count"] += count

        author_node = theme_node["authors"].setdefault(
            author_name,
            {
                "author": author_name,
                "count": 0,
                "details": {},
            },
        )
        author_node["count"] += count
        author_node["details"][detail_name] = author_node["details"].get(detail_name, 0) + count

    result: list[ThemeTreeNodeOut] = []
    for theme_name, theme_payload in sorted(
        tree.items(),
        key=lambda item: (-item[1]["count"], item[0].lower()),
    ):
        authors_out: list[AuthorTreeNodeOut] = []
        for author_name, author_payload in sorted(
            theme_payload["authors"].items(),
            key=lambda item: (-item[1]["count"], item[0].lower()),
        ):
            details_out = [
                DetailNodeOut(detail=detail_name, count=detail_count)
                for detail_name, detail_count in sorted(
                    author_payload["details"].items(),
                    key=lambda item: (-item[1], item[0].lower()),
                )
            ]
            authors_out.append(
                AuthorTreeNodeOut(
                    author=author_name,
                    count=author_payload["count"],
                    details=details_out,
                )
            )
        result.append(
            ThemeTreeNodeOut(
                theme=theme_name,
                count=theme_payload["count"],
                authors=authors_out,
            )
        )

    return result


@app.get("/api/folders")
def list_folders():
    output: list[dict] = []

    # Macro hierarchy:
    # storage/themes/<tipo>/<genere>/<autore>/<titolo>/<resource-file>
    for source_dir in sorted(settings.themes_root.glob("*")):
        if not source_dir.is_dir():
            continue
        source_key = source_dir.name
        matched_new_layout = False

        for content_dir in sorted(source_dir.glob("*")):
            if not content_dir.is_dir():
                continue
            for author_dir in sorted(content_dir.glob("*")):
                if not author_dir.is_dir():
                    continue
                detail_dirs = [path for path in sorted(author_dir.glob("*")) if path.is_dir()]
                if detail_dirs:
                    matched_new_layout = True
                    for detail_dir in detail_dirs:
                        entries = []
                        count = 0
                        for entry in sorted(detail_dir.iterdir()):
                            count += 1
                            if len(entries) < 25:
                                entries.append(entry.name)
                        output.append(
                            {
                                "theme": f"{source_key}/{content_dir.name}/{author_dir.name}/{detail_dir.name}",
                                "path": str(detail_dir.resolve()),
                                "count": count,
                                "preview": entries,
                            }
                        )
                else:
                    # Compatibility with intermediate migrations.
                    entries = []
                    count = 0
                    for entry in sorted(author_dir.iterdir()):
                        count += 1
                        if len(entries) < 25:
                            entries.append(entry.name)
                    output.append(
                        {
                            "theme": f"{source_key}/{content_dir.name}/{author_dir.name}",
                            "path": str(author_dir.resolve()),
                            "count": count,
                            "preview": entries,
                        }
                    )

        # Legacy compatibility: older flat structure storage/themes/<theme>/<resource-file>
        if not matched_new_layout:
            entries = []
            count = 0
            for entry in sorted(source_dir.iterdir()):
                count += 1
                if len(entries) < 25:
                    entries.append(entry.name)
            output.append(
                {
                    "theme": source_key,
                    "path": str(source_dir.resolve()),
                    "count": count,
                    "preview": entries,
                }
            )

    return output


@app.get("/api/files/{resource_id}")
def get_resource_file(resource_id: str, db: Session = Depends(get_db)):
    resource = db.get(Resource, resource_id)
    if not resource or not resource.stored_path:
        raise HTTPException(status_code=404, detail="File non trovato")

    file_path = Path(resource.stored_path)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File non trovato su filesystem")

    filename = file_path.name.split("_", 1)[-1]
    return FileResponse(
        path=file_path,
        media_type=resource.mime_type or "application/octet-stream",
        filename=filename,
    )
