from sqlalchemy import BigInteger, Column, DateTime, Float, String, Text, func
from sqlalchemy.types import JSON

from .db import Base


class Resource(Base):
    __tablename__ = "resources"

    id = Column(String(36), primary_key=True, index=True)
    source_type = Column(String(32), nullable=False, index=True)

    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    source_url = Column(Text, nullable=True, index=True)
    youtube_video_id = Column(String(32), nullable=True, index=True)

    stored_path = Column(Text, nullable=True)
    thematic_path = Column(Text, nullable=True)
    mime_type = Column(String(255), nullable=True)
    size_bytes = Column(BigInteger, nullable=True)
    sha256 = Column(String(64), nullable=True, index=True)

    language = Column(String(32), nullable=True)
    inferred_theme = Column(String(120), nullable=False, default="Uncategorized", index=True)
    inferred_subtheme = Column(String(120), nullable=True, index=True)
    canonical_theme = Column(String(120), nullable=True, index=True)
    keywords = Column(JSON, nullable=False, default=list)
    summary = Column(Text, nullable=True)
    content_text = Column(Text, nullable=True)

    relevance_score = Column(Float, nullable=False, default=0.0, index=True)
    conceptual_score = Column(Float, nullable=False, default=0.0)
    combined_score = Column(Float, nullable=False, default=0.0, index=True)

    llm_labels = Column(JSON, nullable=False, default=dict)
    llm_raw = Column(JSON, nullable=False, default=dict)

    status = Column(String(32), nullable=False, default="processed", index=True)
    error_message = Column(Text, nullable=True)

    uploaded_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), index=True)
    processed_at = Column(DateTime(timezone=True), nullable=True, server_default=func.now())
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
