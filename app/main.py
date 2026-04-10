from pathlib import Path
from datetime import datetime, timezone

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
from app.schemas import IngestLinkRequest, ResourceListOut, ResourceOut, ThemeStatOut
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

            # Keep thematic path aligned for existing rows too.
            if not resource.thematic_path or "/themes/" not in resource.thematic_path:
                refreshed_thematic_path = save_in_thematic_folder(resource, settings.themes_root)
                if resource.thematic_path != refreshed_thematic_path:
                    resource.thematic_path = refreshed_thematic_path
                    prefill_changed = True

            labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
            if labels.get("classification_source") != "llm-content-v2" or labels.get("fallback_used") is True:
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
            if resource.relevance_score != classification.relevance_score:
                resource.relevance_score = classification.relevance_score
                changed = True
            if resource.conceptual_score != classification.conceptual_score:
                resource.conceptual_score = classification.conceptual_score
                changed = True
            if resource.combined_score != classification.combined_score:
                resource.combined_score = classification.combined_score
                changed = True

            llm_labels = resource.llm_labels if isinstance(resource.llm_labels, dict) else {}
            updated_labels = {
                **llm_labels,
                "fallback_used": classification.fallback_used,
                "model": classification.model_used,
                "classification_source": "llm-content-v2",
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

    if q:
        expansion = semantic_search_service.expand_query(q, use_llm=semantic)
        search_terms = expansion.merged_terms(max_items=16)

        if search_terms:
            term_filters = []
            normalized_query = q.strip().lower()
            for term in search_terms:
                if len(term) < 3:
                    continue
                if len(term) < 5 and term != normalized_query:
                    continue
                like = f"%{term}%"
                term_filters.extend(
                    [
                        Resource.title.ilike(like),
                        Resource.description.ilike(like),
                        Resource.summary.ilike(like),
                        Resource.content_text.ilike(like),
                        Resource.source_url.ilike(like),
                        Resource.inferred_theme.ilike(like),
                        Resource.inferred_subtheme.ilike(like),
                        Resource.canonical_theme.ilike(like),
                        cast(Resource.keywords, SqlText).ilike(like),
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

    if source_type in {"file", "link"}:
        query = query.where(Resource.source_type == source_type)

    offset = (page - 1) * page_size
    reverse_order = order == "desc"

    if q:
        candidates = db.scalars(query.limit(600)).all()
        scored = [
            (
                item,
                score_resource_for_query(
                    item,
                    terms=search_terms,
                    target_themes=expansion.target_themes if expansion else [],
                    raw_query=q,
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


@app.get("/api/folders")
def list_folders():
    output: list[dict] = []
    for theme_dir in sorted(settings.themes_root.glob("*")):
        if not theme_dir.is_dir():
            continue

        entries = []
        count = 0
        for entry in sorted(theme_dir.iterdir()):
            count += 1
            if len(entries) < 25:
                entries.append(entry.name)

        output.append(
            {
                "theme": theme_dir.name,
                "path": str(theme_dir.resolve()),
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
