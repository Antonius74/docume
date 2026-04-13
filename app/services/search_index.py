import re
from collections.abc import Iterable


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    cleaned = str(value).replace("\x00", " ")
    cleaned = re.sub(r"[\x01-\x08\x0B\x0C\x0E-\x1F]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _flatten_keywords(keywords: Iterable[str] | None) -> str:
    if not keywords:
        return ""
    return " ".join(_clean_text(item) for item in keywords if _clean_text(item))


def _flatten_labels(labels: dict | None) -> str:
    if not isinstance(labels, dict):
        return ""
    allowed_keys = (
        "tipologia_documento",
        "contenuto",
        "dettaglio_contenuto",
        "taxonomy_type",
        "taxonomy_genre",
        "taxonomy_title",
        "taxonomy_domain",
        "taxonomy_subdomain",
        "taxonomy_author",
        "taxonomy_work",
        "taxonomy_path",
        "author",
        "tags",
        "youtube_channel",
        "site_name",
        "domain",
    )
    values: list[str] = []
    for key in allowed_keys:
        value = labels.get(key)
        if isinstance(value, (list, tuple)):
            values.extend(_clean_text(str(item)) for item in value if _clean_text(str(item)))
        elif value is not None:
            cleaned = _clean_text(str(value))
            if cleaned:
                values.append(cleaned)
    return " ".join(values)


def build_search_text(
    *,
    title: str | None,
    description: str | None,
    summary: str | None,
    content_text: str | None,
    source_url: str | None,
    author_name: str | None,
    inferred_theme: str | None,
    inferred_subtheme: str | None,
    canonical_theme: str | None,
    keywords: Iterable[str] | None,
    llm_labels: dict | None = None,
    max_len: int = 20000,
) -> str:
    chunks = [
        _clean_text(title),
        _clean_text(description),
        _clean_text(summary),
        _clean_text(content_text),
        _clean_text(source_url),
        _clean_text(author_name),
        _clean_text(inferred_theme),
        _clean_text(inferred_subtheme),
        _clean_text(canonical_theme),
        _flatten_keywords(keywords),
        _flatten_labels(llm_labels),
    ]
    merged = " ".join(part for part in chunks if part)
    if max_len > 0:
        merged = merged[:max_len]
    return _clean_text(merged)
