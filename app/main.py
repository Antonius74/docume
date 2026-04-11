from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import Text as SqlText
from sqlalchemy import asc, cast, desc, func, or_, select
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
from app.services.semantic import (
    SemanticSearchService,
    score_resource_for_query,
)
from app.services.storage import remove_resource_artifacts, save_in_thematic_folder

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


def _asset_version() -> int:
    mtimes: list[int] = []
    for asset_name in ("style.css", "app.js"):
        asset_path = static_dir / asset_name
        try:
            mtimes.append(int(asset_path.stat().st_mtime))
        except OSError:
            continue
    return max(mtimes) if mtimes else int(datetime.now(timezone.utc).timestamp())


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
            # from legacy flat layout to macro layout:
            # /themes/<doc|link>/<contenuto>/<autore>/<dettaglio>/<file>
            thematic_path = resource.thematic_path or ""
            has_macro_layout = False
            if "/themes/" in thematic_path:
                relative = thematic_path.split("/themes/", 1)[1]
                segments = [segment for segment in relative.split("/") if segment]
                if len(segments) >= 5 and segments[0] in {"doc", "link"}:
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

            if labels.get("classification_source") != "llm-content-v3-3fields" or labels.get("fallback_used") is True:
                resources.append(resource)

        if not resources:
            if prefill_changed:
                db.commit()
            return

        # Prima versione: riclassifica un batch ridotto in startup per non rallentare troppo.
        resources = resources[:50]
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
                "classification_source": "llm-content-v3-3fields",
                "tipologia_documento": classification.document_type,
                "contenuto": classification.theme,
                "dettaglio_contenuto": classification.subtheme,
                "author": resource.author_name,
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
        return resource
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
        return resource
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

    normalized_q = (q or "").strip()

    if normalized_q:
        expansion = semantic_search_service.expand_query(normalized_q, use_llm=semantic)
        search_terms = expansion.merged_terms(max_items=16)

        if search_terms:
            term_filters = []
            normalized_query = normalized_q.lower()
            for term in search_terms:
                if len(term) < 2:
                    continue
                like = f"%{term}%"
                term_filters.extend(
                    [
                        Resource.title.ilike(like),
                        Resource.description.ilike(like),
                        Resource.summary.ilike(like),
                        Resource.content_text.ilike(like),
                        Resource.source_url.ilike(like),
                        Resource.author_name.ilike(like),
                        Resource.inferred_theme.ilike(like),
                        Resource.inferred_subtheme.ilike(like),
                        Resource.canonical_theme.ilike(like),
                        cast(Resource.keywords, SqlText).ilike(like),
                        cast(Resource.llm_labels, SqlText).ilike(like),
                        cast(Resource.llm_raw, SqlText).ilike(like),
                    ]
                )
            theme_filters = [
                func.lower(theme_expr) == target_theme.lower()
                for target_theme in (expansion.target_themes if expansion else [])
            ]
            combined_filters = [*term_filters, *theme_filters]
            if combined_filters:
                query = query.where(or_(*combined_filters))
    else:
        expansion = None
        search_terms = []

    if theme:
        query = query.where(func.lower(theme_expr) == theme.lower())

    if author:
        author_expr = func.coalesce(func.nullif(func.trim(Resource.author_name), ""), "Sconosciuto")
        query = query.where(func.lower(author_expr) == author.lower())

    if detail:
        detail_expr = func.coalesce(func.nullif(func.trim(Resource.inferred_subtheme), ""), "Generale")
        query = query.where(func.lower(detail_expr) == detail.lower())

    if source_type in {"file", "link"}:
        query = query.where(Resource.source_type == source_type)

    offset = (page - 1) * page_size
    reverse_order = order == "desc"

    if normalized_q:
        candidates = db.scalars(query.limit(600)).all()
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

    return ResourceListOut(total=total, page=page, page_size=page_size, items=items)


@app.get("/api/resources/recent", response_model=list[ResourceOut])
def recent_resources(
    limit: int = Query(default=8, ge=1, le=40),
    db: Session = Depends(get_db),
):
    rows = db.scalars(select(Resource).order_by(desc(Resource.uploaded_at)).limit(limit)).all()
    return rows


@app.get("/api/resources/{resource_id}", response_model=ResourceOut)
def get_resource(resource_id: str, db: Session = Depends(get_db)):
    resource = db.get(Resource, resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail="Resource non trovata")
    return resource


@app.delete("/api/resources/{resource_id}")
def delete_resource(resource_id: str, db: Session = Depends(get_db)):
    resource = db.get(Resource, resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail="Resource non trovata")

    cleanup = remove_resource_artifacts(resource, settings.files_root, settings.themes_root)

    db.delete(resource)
    db.commit()

    return {"status": "deleted", "id": resource_id, "removed_paths": cleanup["removed_paths"]}


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
    author_expr = func.coalesce(func.nullif(func.trim(Resource.author_name), ""), "Sconosciuto")
    detail_expr = func.coalesce(func.nullif(func.trim(Resource.inferred_subtheme), ""), "Generale")

    query = (
        select(
            theme_expr.label("theme"),
            author_expr.label("author"),
            detail_expr.label("detail"),
            func.count(Resource.id).label("count"),
        )
        .group_by(theme_expr, author_expr, detail_expr)
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

    # New macro hierarchy:
    # storage/themes/<doc|link>/<contenuto>/<autore>/<dettaglio>/<resource-file>
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
                    # Compatibility with intermediate migrations:
                    # storage/themes/<doc|link>/<contenuto>/<dettaglio>/<resource-file>
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
