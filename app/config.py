from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Knowledge Classifier"
    app_env: str = "development"
    database_url: str = "postgresql+psycopg://postgres:postgres@127.0.0.1:5432/docume"
    ollama_url: str = "http://localhost:11434"
    # Legacy fallback (kept for backward compatibility)
    ollama_model: str = "gpt-oss:120b-cloud"
    # Requested split models:
    # - text/link/document classification -> GPT 120 family
    # - image classification -> Kimi multimodal
    ollama_model_text: str = "gpt-oss:120b-cloud"
    ollama_model_image: str = "kimi-k2.5:cloud"
    storage_root: Path = Path("./storage")
    max_extract_chars: int = 12000
    max_document_pages: int = 10
    request_timeout_seconds: int = 45
    cors_origins: str = "http://localhost:8000,http://127.0.0.1:8000"

    @property
    def resolved_ollama_model_text(self) -> str:
        candidate = (self.ollama_model_text or "").strip()
        if candidate:
            return candidate
        return (self.ollama_model or "gpt-oss:120b-cloud").strip()

    @property
    def resolved_ollama_model_image(self) -> str:
        candidate = (self.ollama_model_image or "").strip()
        if candidate:
            return candidate
        return (self.ollama_model or "kimi-k2.5:cloud").strip()

    @property
    def parsed_cors_origins(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]

    @property
    def files_root(self) -> Path:
        return self.storage_root / "files"

    @property
    def themes_root(self) -> Path:
        return self.storage_root / "themes"

    @property
    def thumbnails_root(self) -> Path:
        return self.storage_root / "thumbnails"

    @property
    def categories_catalog_path(self) -> Path:
        return self.storage_root / "categories_catalog.json"

    def ensure_storage_paths(self) -> None:
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.files_root.mkdir(parents=True, exist_ok=True)
        self.themes_root.mkdir(parents=True, exist_ok=True)
        self.thumbnails_root.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    return Settings()
