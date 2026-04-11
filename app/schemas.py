from datetime import datetime

from pydantic import BaseModel, ConfigDict, HttpUrl


class IngestLinkRequest(BaseModel):
    url: HttpUrl
    title: str | None = None
    description: str | None = None


class ResourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    source_type: str
    title: str
    description: str | None
    source_url: str | None
    youtube_video_id: str | None
    thumbnail_url: str | None = None
    stored_path: str | None
    thematic_path: str | None
    mime_type: str | None
    size_bytes: int | None
    language: str | None
    author_name: str | None
    canonical_theme: str | None
    inferred_theme: str
    inferred_subtheme: str | None
    keywords: list[str]
    summary: str | None
    relevance_score: float
    conceptual_score: float
    combined_score: float
    status: str
    uploaded_at: datetime


class ResourceListOut(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[ResourceOut]


class ThemeStatOut(BaseModel):
    theme: str
    count: int


class AuthorStatOut(BaseModel):
    author: str
    count: int


class DetailNodeOut(BaseModel):
    detail: str
    count: int


class AuthorTreeNodeOut(BaseModel):
    author: str
    count: int
    details: list[DetailNodeOut]


class ThemeTreeNodeOut(BaseModel):
    theme: str
    count: int
    authors: list[AuthorTreeNodeOut]
