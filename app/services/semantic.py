import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx


CANONICAL_TOPICS: dict[str, list[str]] = {
    "Matematica e Statistica": [
        "matematica",
        "math",
        "algebra",
        "geometria",
        "geometry",
        "trigonometria",
        "calculus",
        "precalculus",
        "analisi",
        "statistica",
        "statistics",
        "probability",
        "probabilita",
        "linear algebra",
    ],
    "Fisica e Scienze": [
        "fisica",
        "physics",
        "relativity",
        "relativita",
        "relatività",
        "einstein",
        "space-time",
        "spacetime",
        "quantum",
        "meccanica quantistica",
        "quantistica",
        "cosmologia",
        "astrofisica",
        "termodinamica",
    ],
    "AI e Machine Learning": [
        "ai",
        "artificial intelligence",
        "machine learning",
        "ml",
        "deep learning",
        "neural network",
        "llm",
        "nlp",
        "computer vision",
        "generative ai",
        "prompt engineering",
    ],
    "Cucina e Food": [
        "cucina",
        "cooking",
        "ricetta",
        "recipe",
        "food",
        "gastronomia",
        "chef",
        "meal prep",
        "forno",
        "dolci",
        "pasta",
    ],
    "Musica e Arte": [
        "musica",
        "music",
        "bach",
        "mozart",
        "beethoven",
        "cello",
        "violin",
        "piano",
        "suite",
        "symphony",
        "concerto",
        "classical music",
        "arte",
    ],
    "Programmazione e Software": [
        "python",
        "javascript",
        "typescript",
        "software",
        "coding",
        "programming",
        "backend",
        "frontend",
        "api",
        "github",
        "devops",
    ],
    "Data Engineering e Analytics": [
        "data science",
        "data analysis",
        "analytics",
        "sql",
        "postgres",
        "database",
        "etl",
        "dashboard",
        "business intelligence",
    ],
    "Business e Marketing": [
        "business",
        "marketing",
        "sales",
        "strategy",
        "branding",
        "growth",
        "product management",
        "go-to-market",
    ],
    "Design e UX": [
        "design",
        "ux",
        "ui",
        "prototyping",
        "figma",
        "usability",
    ],
    "Natura e Ambiente": [
        "natura",
        "nature",
        "ambiente",
        "wildlife",
        "landscape",
        "foresta",
        "mountain",
        "montagna",
        "oceano",
        "mare",
    ],
    "Finanza": [
        "finance",
        "finanza",
        "investing",
        "trading",
        "accounting",
        "budget",
    ],
    "Legal e Compliance": [
        "legal",
        "law",
        "privacy",
        "gdpr",
        "compliance",
        "contract",
    ],
    "Salute e Benessere": [
        "health",
        "medicina",
        "wellness",
        "nutrizione",
        "fitness",
    ],
    "Media e Comunicazione": [
        "video",
        "audio",
        "podcast",
        "youtube",
        "intervista",
        "media",
    ],
}


@dataclass
class QueryExpansion:
    normalized_query: str
    related_terms: list[str] = field(default_factory=list)
    target_themes: list[str] = field(default_factory=list)
    used_fallback: bool = False
    raw: dict = field(default_factory=dict)

    def merged_terms(self, max_items: int = 14) -> list[str]:
        return _unique_terms([self.normalized_query, *self.related_terms], max_items=max_items)


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _normalize_token(value: str) -> str:
    return _clean_text(value).lower()


def _unique_terms(values: list[str], max_items: int = 14) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        token = _normalize_token(raw)
        if len(token) < 2:
            continue
        if token in seen:
            continue
        seen.add(token)
        output.append(token)
        if len(output) >= max_items:
            break
    return output


def _theme_scores(text: str) -> dict[str, float]:
    normalized = _normalize_token(text)
    scores: dict[str, float] = {theme: 0.0 for theme in CANONICAL_TOPICS}

    for theme, terms in CANONICAL_TOPICS.items():
        for term in terms:
            token = _normalize_token(term)
            if not token:
                continue

            pattern = rf"\b{re.escape(token)}\b"
            if re.search(pattern, normalized):
                # "Media" deve pesare meno delle aree concettuali.
                weight = 0.6 if theme == "Media e Comunicazione" else 1.25
                scores[theme] += weight

    return scores


def _direct_theme_match(query: str) -> str | None:
    normalized_query = _normalize_token(query)
    for theme, terms in CANONICAL_TOPICS.items():
        for term in terms:
            if normalized_query == _normalize_token(term):
                return theme
    return None


def _best_theme(text: str, fallback_theme: str | None = None) -> str:
    scores = _theme_scores(text)
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    best_theme, best_score = ranked[0]

    if best_score > 0:
        return best_theme

    if fallback_theme and _clean_text(fallback_theme):
        return fallback_theme.strip().title()

    return "General"


class SemanticSearchService:
    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        timeout_seconds: int = 20,
        cache_ttl_seconds: int = 1200,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache: dict[str, tuple[float, QueryExpansion]] = {}

    def expand_query(self, query: str, *, use_llm: bool = True) -> QueryExpansion:
        normalized_query = _normalize_token(query)
        if not normalized_query:
            return QueryExpansion(normalized_query="")
        if len(normalized_query) <= 3:
            use_llm = False

        cached = self._cache.get(normalized_query)
        now = time.time()
        if cached and now - cached[0] < self.cache_ttl_seconds:
            return cached[1]

        fallback = self._fallback_expand(normalized_query)
        direct_theme = _direct_theme_match(normalized_query)
        if direct_theme:
            fallback.target_themes = [direct_theme]
            use_llm = False
        if not use_llm:
            self._cache[normalized_query] = (now, fallback)
            return fallback

        try:
            llm_result = self._expand_with_llm(normalized_query)
            merged_terms = _unique_terms([*fallback.related_terms, *llm_result.related_terms], max_items=16)
            merged_themes = _unique_terms([*fallback.target_themes, *llm_result.target_themes], max_items=8)

            output = QueryExpansion(
                normalized_query=normalized_query,
                related_terms=merged_terms,
                target_themes=merged_themes,
                used_fallback=False,
                raw=llm_result.raw,
            )
            self._cache[normalized_query] = (now, output)
            return output
        except Exception as exc:  # noqa: BLE001
            fallback.used_fallback = True
            fallback.raw = {"error": str(exc)}
            self._cache[normalized_query] = (now, fallback)
            return fallback

    def _expand_with_llm(self, query: str) -> QueryExpansion:
        prompt = (
            "Espandi la query per ricerca semantica di una knowledge base. "
            "Restituisci SOLO JSON valido nel formato: "
            "{\"related_terms\":[...],\"target_themes\":[...]}\n"
            "Requisiti:"
            "\n- related_terms: massimo 12 termini/sinonimi strettamente pertinenti"
            "\n- target_themes: massimo 4 categorie concettuali"
            "\n- includi mapping utili: math->statistica/geometria/calculus, ai->ml/deep learning, cucina->ricette/food"
            f"\nQuery: {query}"
        )

        request_body = {
            "model": self.model,
            "stream": False,
            "format": "json",
            "messages": [
                {
                    "role": "system",
                    "content": "Sei un motore di query expansion semantica. Output solo JSON.",
                },
                {"role": "user", "content": prompt},
            ],
        }

        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(f"{self.base_url}/api/chat", json=request_body)
            response.raise_for_status()
            response_json = response.json()

        content = response_json.get("message", {}).get("content", "")
        parsed = self._parse_json(content)

        related_terms = parsed.get("related_terms") or []
        target_themes = parsed.get("target_themes") or []

        if not isinstance(related_terms, list):
            related_terms = [str(related_terms)]
        if not isinstance(target_themes, list):
            target_themes = [str(target_themes)]

        normalized_themes = []
        for theme in target_themes:
            normalized_themes.append(_best_theme(theme, fallback_theme=theme))

        return QueryExpansion(
            normalized_query=query,
            related_terms=_unique_terms([str(term) for term in related_terms], max_items=12),
            target_themes=_unique_terms(normalized_themes, max_items=6),
            raw=response_json,
        )

    def _parse_json(self, text: str) -> dict:
        if not text:
            raise ValueError("Missing JSON output from query expansion model")

        stripped = text.strip()
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", stripped, re.DOTALL)
            if not match:
                raise
            return json.loads(match.group(0))

    def _fallback_expand(self, query: str) -> QueryExpansion:
        matched_theme = _best_theme(query, fallback_theme="General")
        related = [query]
        target = []

        if matched_theme in CANONICAL_TOPICS:
            related.extend(CANONICAL_TOPICS[matched_theme])
            target.append(matched_theme)

        if matched_theme == "General":
            # Fallback extra mapping per query corte e comuni.
            if query in {"ai", "ml"}:
                target = ["AI e Machine Learning"]
                related.extend(CANONICAL_TOPICS["AI e Machine Learning"])
            if query in {"math", "matematica", "stat", "statistica", "geometria"}:
                target = ["Matematica e Statistica"]
                related.extend(CANONICAL_TOPICS["Matematica e Statistica"])
            if query in {"cucina", "food", "recipe", "ricette"}:
                target = ["Cucina e Food"]
                related.extend(CANONICAL_TOPICS["Cucina e Food"])
            if query in {"natura", "nature", "wildlife", "ambiente", "landscape"}:
                target = ["Natura e Ambiente"]
                related.extend(CANONICAL_TOPICS["Natura e Ambiente"])
            if query in {"fisica", "physics", "relativita", "relatività", "quantum", "einstein", "scienza"}:
                target = ["Fisica e Scienze"]
                related.extend(CANONICAL_TOPICS["Fisica e Scienze"])

        return QueryExpansion(
            normalized_query=query,
            related_terms=_unique_terms(related, max_items=16),
            target_themes=_unique_terms(target, max_items=4),
            used_fallback=True,
        )


def score_resource_for_query(resource, *, terms: list[str], target_themes: list[str], raw_query: str) -> float:
    title = _normalize_token(resource.title or "")
    description = _normalize_token(resource.description or "")
    summary = _normalize_token(resource.summary or "")
    content_text = _normalize_token(getattr(resource, "content_text", "") or "")
    source_url = _normalize_token(resource.source_url or "")
    inferred_theme = _normalize_token(resource.inferred_theme or "")
    canonical_theme = _normalize_token(getattr(resource, "canonical_theme", "") or "")
    subtheme = _normalize_token(resource.inferred_subtheme or "")
    keywords = _normalize_token(" ".join(getattr(resource, "keywords", []) or []))
    llm_labels = _normalize_token(json.dumps(getattr(resource, "llm_labels", {}) or {}, ensure_ascii=False))
    llm_raw = _normalize_token(json.dumps(getattr(resource, "llm_raw", {}) or {}, ensure_ascii=False))

    score = (resource.combined_score or 0.0) * 2.4
    score += (resource.relevance_score or 0.0) * 1.2
    score += (resource.conceptual_score or 0.0) * 0.9

    normalized_query = _normalize_token(raw_query)
    if normalized_query and normalized_query in title:
        score += 2.6

    for term in _unique_terms(terms, max_items=16):
        if term in title:
            score += 2.2
        if term in canonical_theme or term in inferred_theme or term in subtheme:
            score += 2.0
        if term in keywords:
            score += 1.4
        if term in description or term in summary:
            score += 1.0
        if term in content_text:
            score += 1.35
        if term in llm_labels:
            score += 1.1
        if term in llm_raw:
            score += 0.55
        if term in source_url:
            score += 0.35

    normalized_targets = {_normalize_token(theme) for theme in target_themes if theme}
    if canonical_theme in normalized_targets:
        score += 2.8
    elif inferred_theme in normalized_targets:
        score += 1.9

    uploaded_at = getattr(resource, "uploaded_at", None)
    if uploaded_at:
        now = datetime.now(timezone.utc)
        stamp = uploaded_at
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        age_days = max(0.0, (now - stamp).total_seconds() / 86400)
        score += max(0.0, 1.0 - min(age_days, 365) / 365) * 0.75

    return round(score, 6)
