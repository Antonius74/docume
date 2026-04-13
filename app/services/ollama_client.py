import json
import re
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import httpx

from app.services.default_taxonomy import DEFAULT_TAXONOMY_TREE


_JSON_OBJECT = re.compile(r"\{.*\}", re.DOTALL)
_TIPOLOGIA_PATTERN = re.compile(r"tipologia\s+documento\s*:\s*(.+)", re.IGNORECASE)
_CONTENUTO_PATTERN = re.compile(r"^\s*contenuto\s*:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_DETTAGLIO_PATTERN = re.compile(r"dettaglio\s+contenuto\s*:\s*(.+)", re.IGNORECASE)
_TAG_SPLIT_RE = re.compile(r"[,;|\n]")
_SMALL_CONNECTOR_WORDS = {
    "e",
    "ed",
    "di",
    "da",
    "del",
    "della",
    "delle",
    "dei",
    "degli",
    "du",
    "de",
    "of",
    "the",
    "and",
    "a",
    "an",
    "in",
    "con",
    "per",
}
ALLOWED_CANONICAL_THEMES = [
    "Matematica e Statistica",
    "Fisica e Scienze",
    "AI e Machine Learning",
    "Cucina e Food",
    "Musica e Arte",
    "Programmazione e Software",
    "Data Engineering e Analytics",
    "Business e Marketing",
    "Design e UX",
    "Natura e Ambiente",
    "Finanza",
    "Legal e Compliance",
    "Salute e Benessere",
    "Media e Comunicazione",
    "General",
]

THEME_ALIASES = {
    "matematica": "Matematica e Statistica",
    "statistica": "Matematica e Statistica",
    "math": "Matematica e Statistica",
    "statistics": "Matematica e Statistica",
    "fisica": "Fisica e Scienze",
    "physics": "Fisica e Scienze",
    "scienza": "Fisica e Scienze",
    "science": "Fisica e Scienze",
    "relativita": "Fisica e Scienze",
    "relatività": "Fisica e Scienze",
    "relativity": "Fisica e Scienze",
    "quantum": "Fisica e Scienze",
    "meccanica quantistica": "Fisica e Scienze",
    "cosmologia": "Fisica e Scienze",
    "astrofisica": "Fisica e Scienze",
    "ai": "AI e Machine Learning",
    "ml": "AI e Machine Learning",
    "machine learning": "AI e Machine Learning",
    "cucina": "Cucina e Food",
    "food": "Cucina e Food",
    "musica": "Musica e Arte",
    "arte": "Musica e Arte",
    "music": "Musica e Arte",
    "art": "Musica e Arte",
    "software": "Programmazione e Software",
    "programmazione": "Programmazione e Software",
    "programming": "Programmazione e Software",
    "development": "Programmazione e Software",
    "data": "Data Engineering e Analytics",
    "marketing": "Business e Marketing",
    "design": "Design e UX",
    "natura": "Natura e Ambiente",
    "nature": "Natura e Ambiente",
    "wildlife": "Natura e Ambiente",
    "landscape": "Natura e Ambiente",
    "ambiente": "Natura e Ambiente",
    "finanza": "Finanza",
    "finance": "Finanza",
    "legal": "Legal e Compliance",
    "health": "Salute e Benessere",
    "media": "Media e Comunicazione",
    "comunicazione": "Media e Comunicazione",
    "news": "Media e Comunicazione",
    "notizie": "Media e Comunicazione",
    "attualita": "Media e Comunicazione",
    "attualità": "Media e Comunicazione",
    "cronaca": "Media e Comunicazione",
    "politica": "Media e Comunicazione",
}

CATEGORY_MERGE_ALIASES = {
    "film e serie": "Film e Cinema",
    "film e tv": "Film e Cinema",
    "cinema e tv": "Film e Cinema",
    "movie e serie": "Film e Cinema",
    "link youtube": "Siti Web e Articoli",
    "youtube link": "Siti Web e Articoli",
}

FALLBACK_THEME_TERMS: dict[str, list[str]] = {
    "Matematica e Statistica": [
        "matematica",
        "math",
        "statistics",
        "statistica",
        "probability",
        "probabilita",
        "random variable",
        "variance",
        "calculus",
        "algebra",
        "geometria",
        "geometry",
    ],
    "Fisica e Scienze": [
        "fisica",
        "physics",
        "relativity",
        "relativita",
        "relatività",
        "einstein",
        "quantum",
        "quantistica",
        "meccanica quantistica",
        "thermodynamics",
        "termodinamica",
        "cosmologia",
        "astrofisica",
    ],
    "AI e Machine Learning": [
        "machine learning",
        "deep learning",
        "neural network",
        "artificial intelligence",
        "ai",
        "ml",
        "llm",
        "nlp",
        "computer vision",
    ],
    "Cucina e Food": [
        "cucina",
        "ricetta",
        "recipe",
        "food",
        "chef",
        "gastronomia",
    ],
    "Musica e Arte": [
        "musica",
        "music",
        "bach",
        "beethoven",
        "mozart",
        "cello",
        "piano",
        "suite",
        "symphony",
    ],
    "Programmazione e Software": [
        "python",
        "javascript",
        "typescript",
        "software",
        "programming",
        "api",
        "backend",
        "frontend",
    ],
    "Data Engineering e Analytics": [
        "data engineering",
        "analytics",
        "etl",
        "database",
        "postgres",
        "sql",
        "data science",
        "business intelligence",
    ],
    "Business e Marketing": [
        "business",
        "marketing",
        "sales",
        "strategy",
        "branding",
    ],
    "Design e UX": [
        "design",
        "ux",
        "ui",
        "figma",
        "usability",
    ],
    "Natura e Ambiente": [
        "nature",
        "natura",
        "ambiente",
        "wildlife",
        "forest",
        "oceano",
        "ocean",
        "landscape",
    ],
    "Finanza": [
        "finance",
        "finanza",
        "budget",
        "investing",
        "trading",
        "accounting",
    ],
    "Legal e Compliance": [
        "legal",
        "law",
        "gdpr",
        "privacy",
        "compliance",
        "contract",
    ],
    "Salute e Benessere": [
        "health",
        "medicina",
        "wellness",
        "fitness",
        "nutrizione",
    ],
    "Media e Comunicazione": [
        "youtube",
        "video",
        "podcast",
        "intervista",
        "audio",
        "media",
        "news",
        "notizie",
        "giornale",
        "newspaper",
        "quotidiano",
        "cronaca",
        "politica",
        "attualita",
        "attualità",
        "economia",
        "sport",
        "esteri",
        "breaking news",
        "redazione",
        "editoriale",
    ],
}

NEWS_LINK_TERMS = [
    "news",
    "notizie",
    "giornale",
    "newspaper",
    "quotidiano",
    "cronaca",
    "politica",
    "economia",
    "attualita",
    "attualità",
    "ultime notizie",
    "breaking news",
    "esteri",
    "redazione",
    "editoriale",
]

MUSIC_LINK_TERMS = [
    "musica",
    "music",
    "album",
    "concert",
    "concerto",
    "cello",
    "guitar",
    "spotify",
    "itunes",
    "bach",
    "beethoven",
    "metallica",
]

NEWS_DOMAIN_HINTS = [
    "repubblica.it",
    "corriere.it",
    "ansa.it",
    "ilsole24ore.com",
    "lastampa.it",
    "rainews.it",
    "bbc.",
    "nytimes.com",
    "theguardian.com",
    "reuters.com",
    "cnn.com",
]


@dataclass
class ClassificationResult:
    title: str
    document_type: str | None
    theme: str
    canonical_theme: str
    subtheme: str | None
    summary: str
    keywords: list[str]
    relevance_score: float
    conceptual_score: float
    combined_score: float
    language: str | None
    model_used: str
    raw: dict
    fallback_used: bool
    semantic_theme: str | None = None
    semantic_subtheme: str | None = None
    taxonomy_domain: str | None = None
    taxonomy_subdomain: str | None = None
    taxonomy_author: str | None = None
    taxonomy_work: str | None = None
    taxonomy_path: str | None = None


class OllamaClassifier:
    def __init__(
        self,
        base_url: str,
        model: str | None = None,
        *,
        text_model: str | None = None,
        image_model: str | None = None,
        category_catalog_path: str | Path | None = None,
        timeout_seconds: int = 45,
    ):
        self.base_url = base_url.rstrip("/")
        selected_text_model = (text_model or model or "gpt-oss:120b").strip()
        selected_image_model = (image_model or model or selected_text_model).strip()
        self.text_model = selected_text_model
        self.image_model = selected_image_model
        self.timeout_seconds = timeout_seconds
        self._resolved_model_cache: dict[str, str] = {}
        self._available_models_cache: list[str] = []
        self._available_models_cache_until: float = 0.0
        self._catalog_lock = Lock()
        self.category_catalog_path = Path(category_catalog_path) if category_catalog_path else None
        self._allowed_canonical_themes: list[str] = list(ALLOWED_CANONICAL_THEMES)
        self._theme_aliases: dict[str, str] = dict(THEME_ALIASES)
        for alias_key, target in CATEGORY_MERGE_ALIASES.items():
            self._theme_aliases[self._normalize_catalog_token(alias_key)] = target
        self._fallback_theme_terms: dict[str, list[str]] = {
            key: list(values) for key, values in FALLBACK_THEME_TERMS.items()
        }
        self._taxonomy_tree: dict[str, dict[str, dict[str, list[str]]]] = deepcopy(DEFAULT_TAXONOMY_TREE)
        self._taxonomy_paths: list[str] = []
        self._refresh_taxonomy_paths()
        self._load_category_catalog()
        self._merge_taxonomy_into_catalog()

    def _merge_category_label(self, value: object) -> str:
        candidate = self._titleize_category(value)
        if not candidate:
            return ""
        merge_target = CATEGORY_MERGE_ALIASES.get(self._normalize_catalog_token(candidate))
        if merge_target:
            return self._titleize_category(merge_target) or candidate
        return candidate

    def _normalize_catalog_token(self, value: object) -> str:
        candidate = str(value or "").strip().lower()
        candidate = re.sub(r"\s+", " ", candidate)
        return candidate

    def _smart_capitalize_word(self, token: str) -> str:
        lowered = token.lower()
        if lowered in {"ai", "ml", "ux", "ui", "llm"}:
            return lowered.upper()
        if "-" in token:
            return "-".join(self._smart_capitalize_word(part) for part in token.split("-") if part)
        return token[:1].upper() + token[1:].lower()

    def _titleize_category(self, value: object) -> str:
        candidate = str(value or "").replace("\x00", " ").strip()
        candidate = re.sub(r"[^A-Za-zÀ-ÿ0-9&/ \-]+", " ", candidate)
        candidate = re.sub(r"\s+", " ", candidate).strip()
        if not candidate:
            return ""

        words: list[str] = []
        for index, word in enumerate(candidate.split(" ")):
            if not word:
                continue
            normalized_word = self._smart_capitalize_word(word)
            if index > 0 and normalized_word.lower() in _SMALL_CONNECTOR_WORDS:
                normalized_word = normalized_word.lower()
            words.append(normalized_word)
        titled = " ".join(words).strip()
        return titled[:120]

    def _catalog_payload(self) -> dict:
        return {
            "categories": list(self._allowed_canonical_themes),
            "aliases": dict(self._theme_aliases),
            "terms": {key: list(values) for key, values in self._fallback_theme_terms.items()},
        }

    def _save_category_catalog(self) -> None:
        if not self.category_catalog_path:
            return
        try:
            self.category_catalog_path.parent.mkdir(parents=True, exist_ok=True)
            payload = self._catalog_payload()
            self.category_catalog_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:  # noqa: BLE001
            return

    def _load_category_catalog(self) -> None:
        if not self.category_catalog_path:
            return
        with self._catalog_lock:
            if not self.category_catalog_path.exists():
                self._save_category_catalog()
                return

            try:
                payload = json.loads(self.category_catalog_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                self._save_category_catalog()
                return

            categories = payload.get("categories") if isinstance(payload, dict) else None
            aliases = payload.get("aliases") if isinstance(payload, dict) else None
            terms = payload.get("terms") if isinstance(payload, dict) else None
            existing_lowers = {item.lower() for item in self._allowed_canonical_themes}

            if isinstance(categories, list):
                for category in categories:
                    formatted = self._merge_category_label(category)
                    if not formatted:
                        continue
                    lowered = formatted.lower()
                    if lowered not in existing_lowers:
                        self._allowed_canonical_themes.append(formatted)
                        existing_lowers.add(lowered)

            if isinstance(aliases, dict):
                for alias, category in aliases.items():
                    alias_key = self._normalize_catalog_token(alias)
                    canonical = self._merge_category_label(category)
                    if not alias_key or not canonical:
                        continue
                    lowered = canonical.lower()
                    if lowered not in existing_lowers:
                        self._allowed_canonical_themes.append(canonical)
                        existing_lowers.add(lowered)
                    self._theme_aliases[alias_key] = canonical

            if isinstance(terms, dict):
                for category, items in terms.items():
                    canonical = self._merge_category_label(category)
                    if not canonical:
                        continue
                    if canonical not in self._fallback_theme_terms:
                        self._fallback_theme_terms[canonical] = []
                    if isinstance(items, list):
                        for item in items:
                            token = self._normalize_catalog_token(item)
                            if token and token not in self._fallback_theme_terms[canonical]:
                                self._fallback_theme_terms[canonical].append(token)

            # Persist normalized catalog.
            self._save_category_catalog()

    def _refresh_taxonomy_paths(self) -> None:
        paths: list[str] = []
        for domain, subdomains in self._taxonomy_tree.items():
            for subdomain, authors in subdomains.items():
                for author, works in authors.items():
                    if not works:
                        paths.append(f"{domain} > {subdomain} > {author} > Sconosciuto")
                        continue
                    for work in works:
                        paths.append(f"{domain} > {subdomain} > {author} > {work}")
        self._taxonomy_paths = sorted(set(paths))

    def _merge_taxonomy_into_catalog(self) -> None:
        with self._catalog_lock:
            existing_lowers = {item.lower() for item in self._allowed_canonical_themes}
            for domain, subdomains in self._taxonomy_tree.items():
                domain_clean = self._merge_category_label(domain)
                if not domain_clean:
                    continue
                if domain_clean.lower() not in existing_lowers:
                    self._allowed_canonical_themes.append(domain_clean)
                    existing_lowers.add(domain_clean.lower())
                domain_alias_key = self._normalize_catalog_token(domain_clean)
                self._theme_aliases.setdefault(domain_alias_key, domain_clean)

                domain_terms = self._fallback_theme_terms.setdefault(domain_clean, [])
                for subdomain, authors in subdomains.items():
                    sub_token = self._normalize_catalog_token(subdomain)
                    if sub_token and sub_token not in domain_terms:
                        domain_terms.append(sub_token)
                    if sub_token and sub_token not in self._theme_aliases:
                        self._theme_aliases[sub_token] = domain_clean

                    for author, works in authors.items():
                        author_token = self._normalize_catalog_token(author)
                        if author_token and author_token not in domain_terms:
                            domain_terms.append(author_token)
                        if author_token and author_token not in self._theme_aliases:
                            self._theme_aliases[author_token] = domain_clean
                        for work in works or []:
                            work_token = self._normalize_catalog_token(work)
                            if work_token and work_token not in domain_terms:
                                domain_terms.append(work_token)
                            if work_token and work_token not in self._theme_aliases:
                                self._theme_aliases[work_token] = domain_clean

            self._save_category_catalog()

    def _allowed_categories_snapshot(self) -> list[str]:
        with self._catalog_lock:
            return list(self._allowed_canonical_themes)

    def _aliases_snapshot(self) -> dict[str, str]:
        with self._catalog_lock:
            return dict(self._theme_aliases)

    def _fallback_terms_snapshot(self) -> dict[str, list[str]]:
        with self._catalog_lock:
            return {key: list(values) for key, values in self._fallback_theme_terms.items()}

    def _categories_prompt_block(self, *, max_items: int = 120) -> str:
        categories = self._allowed_categories_snapshot()
        if not categories:
            return "Categorie contenuto esistenti: General."

        visible = categories[:max_items]
        categories_line = ", ".join(visible)
        extra = len(categories) - len(visible)
        suffix = f" (+{extra} altre)" if extra > 0 else ""
        return (
            "Categorie contenuto esistenti (preferiscile quando coerenti): "
            f"{categories_line}{suffix}.\n"
            "Se nessuna categoria esistente è adatta, crea una NUOVA categoria nel campo Contenuto "
            "con nome breve e specifico (2-5 parole)."
        )

    def _taxonomy_prompt_block(self, *, max_items: int = 240) -> str:
        if not self._taxonomy_paths:
            return "Tassonomia predefinita: General > Generale > Sconosciuto > Contenuto non classificato"
        visible = self._taxonomy_paths[:max_items]
        extra = len(self._taxonomy_paths) - len(visible)
        suffix = f"\n... (+{extra} percorsi aggiuntivi)" if extra > 0 else ""
        return (
            "Tassonomia predefinita (Tipo -> Genere -> Autore -> Titolo). "
            "Scegli il percorso più vicino:\n"
            + "\n".join(visible)
            + suffix
        )

    def _match_from_options(self, value: str, options: list[str]) -> str:
        cleaned = self._clean_field_value(value, max_len=220).lower()
        if not cleaned or not options:
            return options[0] if options else "General"

        # Exact
        for option in options:
            if cleaned == option.lower():
                return option

        # Containment
        for option in options:
            option_lower = option.lower()
            if cleaned in option_lower or option_lower in cleaned:
                return option

        # Token overlap fallback.
        candidate_tokens = set(re.findall(r"[a-z0-9à-ÿ]{3,}", cleaned))
        if not candidate_tokens:
            return options[0]
        scored: list[tuple[float, str]] = []
        for option in options:
            option_tokens = set(re.findall(r"[a-z0-9à-ÿ]{3,}", option.lower()))
            if not option_tokens:
                continue
            overlap = len(candidate_tokens & option_tokens) / max(1, len(candidate_tokens | option_tokens))
            scored.append((overlap, option))
        if not scored:
            return options[0]
        scored.sort(key=lambda item: item[0], reverse=True)
        if scored[0][0] <= 0:
            return options[0]
        return scored[0][1]

    def _parse_taxonomy_selection(self, text: str) -> dict[str, str]:
        raw = (text or "").strip()
        if not raw:
            return {}

        # JSON support.
        maybe_json = self._parse_json(raw)
        if isinstance(maybe_json, dict):
            return {
                "domain": self._clean_field_value(
                    maybe_json.get("domain")
                    or maybe_json.get("dominio")
                    or maybe_json.get("tipo")
                    or maybe_json.get("tipologia")
                ),
                "subdomain": self._clean_field_value(
                    maybe_json.get("subdomain")
                    or maybe_json.get("sottodominio")
                    or maybe_json.get("genere")
                ),
                "author": self._clean_field_value(maybe_json.get("author") or maybe_json.get("autore")),
                "work": self._clean_field_value(
                    maybe_json.get("work")
                    or maybe_json.get("opera")
                    or maybe_json.get("titolo")
                ),
            }

        output: dict[str, str] = {}
        for line in raw.splitlines():
            cleaned = line.strip().lstrip("-*•").strip()
            if ":" not in cleaned:
                continue
            key, value = cleaned.split(":", 1)
            key_norm = " ".join(key.lower().split())
            val = self._clean_field_value(value, max_len=220)
            if not val:
                continue
            if key_norm in {"dominio", "domain", "tema", "tipo", "tipologia", "tipologia contenuto"}:
                output["domain"] = val
            elif key_norm in {"sottodominio", "subdomain", "subcategory", "genere", "categoria"}:
                output["subdomain"] = val
            elif key_norm in {"autore", "author", "creator"}:
                output["author"] = val
            elif key_norm in {"opera", "work", "titolo opera", "titolo"}:
                output["work"] = val
        return output

    def _fallback_taxonomy_type(
        self,
        *,
        source_type: str,
        source_url: str | None,
        inferred_theme: str | None = None,
        mime_type: str | None = None,
    ) -> str:
        return self._map_signal_to_taxonomy_type(
            signal=inferred_theme,
            source_type=source_type,
            source_url=source_url,
            mime_type=mime_type,
        )

    def _map_signal_to_taxonomy_type(
        self,
        *,
        signal: str | None,
        source_type: str,
        source_url: str | None,
        mime_type: str | None = None,
    ) -> str:
        source_clean = (source_type or "").strip().lower()
        mime = (mime_type or "").lower()
        url = (source_url or "").lower()
        is_youtube = "youtube.com" in url or "youtu.be" in url
        normalized_theme = self._normalize_canonical_theme(signal, allow_create=False)
        normalized_signal = self._normalize_catalog_token(signal)

        if mime.startswith("image/"):
            return "Immagini e Arte Visiva"
        if mime.startswith("audio/"):
            return "Musica e Audio"
        if mime.startswith("video/"):
            return "Film e Cinema"

        if normalized_theme in {"Musica e Arte"}:
            return "Musica e Audio"
        if normalized_theme in {"Media e Comunicazione"}:
            return "Siti Web e Articoli"
        if normalized_theme in {
            "Matematica e Statistica",
            "Fisica e Scienze",
            "AI e Machine Learning",
            "Programmazione e Software",
            "Data Engineering e Analytics",
        }:
            if source_clean == "file":
                return "Libri e Documenti"
            if source_clean == "link" and not is_youtube:
                return "Siti Web e Articoli"
            return "Corsi e Formazione"
        if normalized_theme in {"Legal e Compliance", "Finanza", "Business e Marketing"}:
            if source_clean == "link":
                return "Siti Web e Articoli"
            return "Libri e Documenti"

        if any(token in normalized_signal for token in ("film", "cinema", "serie", "movie", "documentario")):
            return "Film e Cinema"
        if any(token in normalized_signal for token in ("musica", "music", "podcast", "concerto", "album")):
            return "Musica e Audio"
        if any(token in normalized_signal for token in ("articolo", "blog", "news", "notizie", "giornale")):
            return "Siti Web e Articoli"

        if is_youtube:
            return "Corsi e Formazione"

        if source_clean == "file":
            return "Libri e Documenti"
        if source_clean == "link":
            return "Siti Web e Articoli"
        return "General"

    def _taxonomy_selection_fallback(
        self,
        *,
        title: str,
        description: str | None,
        extracted_text: str,
        source_type: str,
        source_url: str | None,
        mime_type: str | None = None,
    ) -> dict[str, str]:
        merged = " ".join(part for part in [title, description or "", extracted_text or ""] if part)
        inferred_theme, _, matched = self._infer_theme_from_text(merged, source_url=source_url)
        domain = self._fallback_taxonomy_type(
            source_type=source_type,
            source_url=source_url,
            inferred_theme=inferred_theme,
            mime_type=mime_type,
        )
        if self._is_generic_value(domain):
            domain = self._normalize_canonical_theme(inferred_theme, allow_create=True)
        subdomains = list((self._taxonomy_tree.get(domain) or {"Generale": {"Sconosciuto": ["Contenuto"]}}).keys())
        subdomain = self._match_from_options(inferred_theme or "Generale", subdomains)
        authors = list((self._taxonomy_tree.get(domain, {}).get(subdomain) or {"Sconosciuto": ["Contenuto"]}).keys())
        author = self._match_from_options(" ".join(matched) or "Sconosciuto", authors)
        works = (self._taxonomy_tree.get(domain, {}).get(subdomain, {}).get(author) or ["Contenuto non classificato"])
        work = self._match_from_options(title or "Contenuto non classificato", works)
        return {"domain": domain, "subdomain": subdomain, "author": author, "work": work}

    async def _select_taxonomy(
        self,
        *,
        title: str,
        description: str | None,
        extracted_text: str,
        source_type: str,
        source_url: str | None,
        mime_type: str | None = None,
        selected_model: str,
    ) -> dict[str, str]:
        prompt_payload = {
            "source_type": source_type,
            "title": title,
            "description": description,
            "source_url": source_url,
            "content_preview": (extracted_text or "")[:7000],
        }
        taxonomy_block = self._taxonomy_prompt_block(max_items=260)
        system_prompt = (
            "Sei un classificatore tassonomico. "
            "Scegli il percorso più coerente dalla tassonomia fornita. "
            "Rispondi solo con 4 righe: Tipo, Genere, Autore, Titolo."
        )
        user_prompt = (
            "Seleziona il percorso tassonomico migliore.\n"
            "Formato risposta obbligatorio:\n"
            "Tipo: <...>\n"
            "Genere: <...>\n"
            "Autore: <...>\n"
            "Titolo: <...>\n"
            "YouTube e solo una fonte/link: NON usarlo come Tipo o Genere.\n"
            "Se autore o titolo non sono chiari usa Sconosciuto.\n"
            f"{taxonomy_block}\n"
            f"Input: {json.dumps(prompt_payload, ensure_ascii=False)}"
        )

        request_body = {
            "model": selected_model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resolved_model = await self._resolve_model_name(client, selected_model)
                request_body["model"] = resolved_model
                response = await client.post(f"{self.base_url}/api/chat", json=request_body)
                response.raise_for_status()
                response_json = response.json()

            parsed = self._parse_taxonomy_selection(str(response_json.get("message", {}).get("content", "")))
        except Exception:  # noqa: BLE001
            parsed = {}

        if not parsed:
            parsed = self._taxonomy_selection_fallback(
                title=title,
                description=description,
                extracted_text=extracted_text,
                source_type=source_type,
                source_url=source_url,
                mime_type=mime_type,
            )

        domain_options = list(self._taxonomy_tree.keys()) or ["General"]
        parsed_domain = parsed.get("domain") or "General"
        if self._is_generic_value(parsed_domain):
            parsed_domain = "General"
        domain = self._match_from_options(parsed_domain, domain_options)
        normalized_parsed_domain = self._normalize_catalog_token(parsed_domain)
        normalized_domain_options = {self._normalize_catalog_token(item) for item in domain_options}
        if (
            parsed_domain
            and not self._is_generic_value(parsed_domain)
            and normalized_parsed_domain not in normalized_domain_options
        ):
            mapped_domain = self._map_signal_to_taxonomy_type(
                signal=parsed_domain,
                source_type=source_type,
                source_url=source_url,
                mime_type=mime_type,
            )
            if self._normalize_catalog_token(mapped_domain) in normalized_domain_options:
                domain = mapped_domain

        subdomain_options = list((self._taxonomy_tree.get(domain) or {"Generale": {"Sconosciuto": ["Contenuto"]}}).keys())
        parsed_subdomain = parsed.get("subdomain") or "Generale"
        if self._is_generic_value(parsed_subdomain):
            parsed_subdomain = "Generale"
        subdomain = self._match_from_options(parsed_subdomain, subdomain_options)
        if (
            parsed_subdomain
            and not self._is_generic_value(parsed_subdomain)
            and self._normalize_catalog_token(parsed_subdomain)
            not in {self._normalize_catalog_token(item) for item in subdomain_options}
        ):
            subdomain = self._clean_field_value(parsed_subdomain, max_len=120) or "Generale"

        author_options = list((self._taxonomy_tree.get(domain, {}).get(subdomain) or {"Sconosciuto": ["Contenuto"]}).keys())
        parsed_author = parsed.get("author") or "Sconosciuto"
        if self._is_generic_value(parsed_author):
            author = "Sconosciuto"
        else:
            author = self._match_from_options(parsed_author, author_options)

        parsed_work = parsed.get("work") or title or "Contenuto non classificato"
        if self._is_generic_value(parsed_work):
            work = "Contenuto non classificato"
        else:
            work_options = self._taxonomy_tree.get(domain, {}).get(subdomain, {}).get(author) or []
            if work_options:
                work = self._match_from_options(parsed_work, work_options)
            else:
                work = self._clean_field_value(parsed_work, max_len=180) or "Contenuto non classificato"
        return {
            "domain": domain,
            "subdomain": subdomain,
            "author": author,
            "work": work,
            "path": f"{domain} > {subdomain} > {author} > {work}",
        }

    def _is_generic_value(self, value: object) -> bool:
        candidate = self._normalize_catalog_token(value)
        if not candidate:
            return True
        generic = {
            "general",
            "generale",
            "sconosciuto",
            "unknown",
            "n/a",
            "na",
            "none",
            "null",
            "contenuto",
            "contenuto non classificato",
            "misc",
            "varie",
            "altro",
            "uncategorized",
            "non classificato",
        }
        return candidate in generic

    def _ensure_taxonomy_branch(
        self,
        *,
        domain: object,
        subdomain: object,
        author: object,
        work: object,
    ) -> dict[str, str]:
        normalized_domain = self._normalize_canonical_theme(domain, allow_create=True)

        normalized_subdomain = self._clean_field_value(subdomain or "Generale", max_len=120) or "Generale"
        if self._is_generic_value(normalized_subdomain):
            normalized_subdomain = "Generale"

        normalized_author = self._sanitize_author_name(author) or "Sconosciuto"
        normalized_work = self._clean_field_value(work or "Contenuto non classificato", max_len=180)
        if not normalized_work:
            normalized_work = "Contenuto non classificato"

        domain_node = self._taxonomy_tree.setdefault(normalized_domain, {})
        subdomain_node = domain_node.setdefault(normalized_subdomain, {})
        works = subdomain_node.setdefault(normalized_author, [])

        if normalized_work not in works:
            works.append(normalized_work)

        self._refresh_taxonomy_paths()
        self._merge_taxonomy_into_catalog()

        return {
            "domain": normalized_domain,
            "subdomain": normalized_subdomain,
            "author": normalized_author,
            "work": normalized_work,
            "path": f"{normalized_domain} > {normalized_subdomain} > {normalized_author} > {normalized_work}",
        }

    def _parse_tags_output(self, content: str) -> list[str]:
        raw = (content or "").strip()
        if not raw:
            return []

        try:
            parsed_list = json.loads(raw)
            if isinstance(parsed_list, list):
                return [
                    self._clean_field_value(item, max_len=40)
                    for item in parsed_list
                    if self._clean_field_value(item, max_len=40)
                ]
        except Exception:  # noqa: BLE001
            pass

        parsed_json = self._parse_json(raw)
        if isinstance(parsed_json, dict):
            for key in ("tags", "keywords", "tag_list"):
                value = parsed_json.get(key)
                if isinstance(value, list):
                    return [self._clean_field_value(item, max_len=40) for item in value if self._clean_field_value(item, max_len=40)]
                if isinstance(value, str) and value.strip():
                    return [
                        self._clean_field_value(part, max_len=40)
                        for part in _TAG_SPLIT_RE.split(value)
                        if self._clean_field_value(part, max_len=40)
                    ]

        tags: list[str] = []
        for line in raw.splitlines():
            cleaned = line.strip().lstrip("-*•").strip()
            if not cleaned:
                continue
            key_lower = cleaned.lower()
            if key_lower.startswith(("tags:", "keyword:", "keywords:")):
                _, _, cleaned = cleaned.partition(":")
                cleaned = cleaned.strip()
            for piece in _TAG_SPLIT_RE.split(cleaned):
                tag = self._clean_field_value(piece, max_len=40)
                if tag:
                    tags.append(tag)

        if len(tags) == 1 and " " in tags[0]:
            expanded = [
                self._clean_field_value(part, max_len=40)
                for part in re.split(r"[•·]", tags[0])
                if self._clean_field_value(part, max_len=40)
            ]
            if len(expanded) > 1:
                tags = expanded

        return tags

    def _dedupe_keywords(self, values: list[str], *, limit: int = 16) -> list[str]:
        output: list[str] = []
        seen: set[str] = set()
        for item in values:
            cleaned = self._clean_field_value(item, max_len=40)
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            output.append(cleaned)
            if len(output) >= limit:
                break
        return output

    def _ensure_min_keywords(
        self,
        *,
        current_keywords: list[str],
        minimum: int,
        title: str,
        description: str | None,
        extracted_text: str,
        source_type: str,
        taxonomy: dict[str, str] | None = None,
    ) -> list[str]:
        keywords = list(current_keywords)
        if len(keywords) >= minimum:
            return keywords

        fallback_candidates: list[str] = []
        if taxonomy:
            fallback_candidates.extend(
                [
                    taxonomy.get("domain", ""),
                    taxonomy.get("subdomain", ""),
                    taxonomy.get("author", ""),
                    taxonomy.get("work", ""),
                ]
            )
        fallback_candidates.extend(
            [source_type, title, description or ""]
        )

        token_source = " ".join(part for part in [title, description or "", extracted_text] if part)
        token_candidates = re.findall(r"[A-Za-zÀ-ÿ0-9][A-Za-zÀ-ÿ0-9_-]{2,}", token_source.lower())
        fallback_candidates.extend(token_candidates[:60])

        keywords = self._dedupe_keywords([*keywords, *fallback_candidates], limit=24)

        if len(keywords) < minimum:
            while len(keywords) < minimum:
                keywords.append(f"tag-{len(keywords) + 1}")

        return keywords

    async def _generate_search_tags(
        self,
        *,
        source_type: str,
        title: str,
        description: str | None,
        extracted_text: str,
        source_url: str | None,
        document_type: str | None,
        semantic_theme: str | None,
        semantic_subtheme: str | None,
        taxonomy: dict[str, str] | None,
        selected_model: str,
        image_b64: str | None = None,
        minimum_tags: int = 10,
    ) -> list[str]:
        payload = {
            "source_type": source_type,
            "title": title,
            "description": description,
            "source_url": source_url,
            "document_type": document_type,
            "semantic_theme": semantic_theme,
            "semantic_subtheme": semantic_subtheme,
            "taxonomy_path": taxonomy.get("path") if taxonomy else None,
            "content_preview": (extracted_text or "")[:7000],
        }

        message: dict[str, object] = {
            "role": "user",
            "content": (
                "Genera tag di ricerca utili per retrieval semantico.\n"
                f"Restituisci SOLO JSON valido nel formato {{\"tags\":[...]}} con almeno {minimum_tags} tag.\n"
                "Tag brevi (1-4 parole), pertinenti, senza spiegazioni."
                f"\nInput: {json.dumps(payload, ensure_ascii=False)}"
            ),
        }
        if image_b64:
            message["images"] = [image_b64]

        request_body = {
            "model": selected_model,
            "stream": False,
            "messages": [
                {
                    "role": "system",
                    "content": "Sei un motore di tagging semantico. Produci solo JSON.",
                },
                message,
            ],
        }

        parsed_tags: list[str] = []
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resolved_model = await self._resolve_model_name(client, selected_model)
                request_body["model"] = resolved_model
                response = await client.post(f"{self.base_url}/api/chat", json=request_body)
                response.raise_for_status()
                response_json = response.json()
            content = str(response_json.get("message", {}).get("content", ""))
            parsed_tags = self._parse_tags_output(content)
        except Exception:  # noqa: BLE001
            parsed_tags = []

        normalized = self._dedupe_keywords(parsed_tags, limit=20)
        normalized = self._ensure_min_keywords(
            current_keywords=normalized,
            minimum=minimum_tags,
            title=title,
            description=description,
            extracted_text=extracted_text,
            source_type=source_type,
            taxonomy=taxonomy,
        )
        return self._dedupe_keywords(normalized, limit=20)

    def _register_generated_category(self, value: object) -> str:
        candidate = self._merge_category_label(value)
        if not candidate:
            return "General"

        lowered_candidate = candidate.lower()
        if lowered_candidate in {"general", "other", "misc", "varie", "altro", "uncategorized"}:
            return "General"

        with self._catalog_lock:
            for existing in self._allowed_canonical_themes:
                if lowered_candidate == existing.lower():
                    return existing

            self._allowed_canonical_themes.append(candidate)

            alias_key = self._normalize_catalog_token(candidate)
            self._theme_aliases[alias_key] = candidate

            # Additional aliases from component tokens.
            token_aliases = [
                self._normalize_catalog_token(token)
                for token in re.findall(r"[A-Za-zÀ-ÿ0-9]{3,}", candidate)
            ]
            for token in token_aliases:
                if token and token not in self._theme_aliases:
                    self._theme_aliases[token] = candidate

            terms = self._fallback_theme_terms.setdefault(candidate, [])
            for token in token_aliases:
                if token and token not in terms:
                    terms.append(token)

            self._save_category_catalog()

        return candidate

    async def classify(
        self,
        *,
        source_type: str,
        title: str,
        description: str | None,
        extracted_text: str,
        mime_type: str | None,
        source_url: str | None,
        source_name: str | None = None,
        image_b64: str | None = None,
    ) -> ClassificationResult:
        selected_model = self.image_model if image_b64 else self.text_model
        resolved_model = selected_model
        payload = {
            "source_type": source_type,
            "title": title,
            "source_name": source_name or title,
            "description": description,
            "mime_type": mime_type,
            "source_url": source_url,
            "content": extracted_text,
            "has_image": bool(image_b64),
        }
        categories_block = self._categories_prompt_block()

        system_prompt = (
            "Sei un classificatore di knowledge base aziendale. "
            "Classifica solo in base al contenuto semantico del testo o dell'immagine fornita. "
            "Scegli un solo dominio dominante, ignorando elementi marginali. "
            "Rispondi esclusivamente con le tre righe richieste, senza JSON e senza testo extra."
        )
        link_specific_rules = ""
        if source_type == "link":
            link_specific_rules = (
                "\nRegole link/siti:"
                "\n- Usa il contenuto principale estratto e i metadati; ignora menu, footer e voci di navigazione."
                "\n- Se la pagina è un quotidiano/news portal o contiene temi multipli di attualità, classifica in Media e Comunicazione."
                "\n- NON classificare in Musica e Arte solo perché compaiono parole come 'musica' in menu/tassonomie."
                "\n- Per YouTube classifica dal topic del video (titolo+descrizione+canale), non dalla piattaforma."
            )
        user_prompt = (
            "Rispondi solo con tre informazioni, non aggiungere altro.\n"
            "Tipologia documento: <formazione, divulgazione, news, documentazione tecnica, tutorial, ecc>\n"
            "Contenuto: <dominio principale in una sola categoria; usa categorie esistenti o creane una nuova se necessario>\n"
            "Dettaglio contenuto: <sottodominio specifico: statistica applicata, musica moderna, arte figurativa, ecc>\n"
            f"{categories_block}\n"
            "Se has_image=true usa il contenuto visivo principale per classificare.\n"
            f"{link_specific_rules}\n"
            f"Input: {json.dumps(payload, ensure_ascii=False)}"
        )

        message = {"role": "user", "content": user_prompt}
        if image_b64:
            message["images"] = [image_b64]

        request_body = {
            "model": resolved_model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system_prompt},
                message,
            ],
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resolved_model = await self._resolve_model_name(client, selected_model)
                request_body["model"] = resolved_model
                response = await client.post(f"{self.base_url}/api/chat", json=request_body)
                if response.status_code == 404 and "not found" in response.text.lower():
                    # One retry with refreshed model list in case tags changed or
                    # requested model alias is unavailable.
                    refreshed_model = await self._resolve_model_name(
                        client,
                        selected_model,
                        force_refresh=True,
                    )
                    if refreshed_model and refreshed_model.lower() != resolved_model.lower():
                        resolved_model = refreshed_model
                        request_body["model"] = resolved_model
                        response = await client.post(f"{self.base_url}/api/chat", json=request_body)
                response.raise_for_status()
                response_json = response.json()

            content = response_json.get("message", {}).get("content", "")
            parsed = self._parse_classification(content)
            normalized = self._normalize(
                parsed,
                title=title,
                source_type=source_type,
                extracted_text=extracted_text,
                source_url=source_url,
                description=description,
            )
            semantic_theme = normalized.get("theme")
            semantic_subtheme = normalized.get("subtheme")

            taxonomy = await self._select_taxonomy(
                title=title,
                description=description,
                extracted_text=extracted_text,
                source_type=source_type,
                source_url=source_url,
                mime_type=mime_type,
                selected_model=self.text_model,
            )

            selected_domain = self._normalize_canonical_theme(
                taxonomy.get("domain") or normalized.get("canonical_theme") or normalized.get("theme"),
                allow_create=True,
            )
            selected_subdomain = self._clean_field_value(taxonomy.get("subdomain") or "Generale", max_len=120) or "Generale"
            selected_author = self._sanitize_author_name(taxonomy.get("author")) or "Sconosciuto"
            selected_work = self._clean_field_value(taxonomy.get("work"), max_len=180)

            if (
                selected_domain == "General"
                and self._normalize_canonical_theme(semantic_theme, allow_create=True) != "General"
            ):
                taxonomy = self._ensure_taxonomy_branch(
                    domain=semantic_theme,
                    subdomain=semantic_subtheme or "Generale",
                    author=selected_author,
                    work=title or semantic_subtheme or "Contenuto non classificato",
                )
                selected_domain = taxonomy["domain"]
                selected_subdomain = taxonomy["subdomain"]
                selected_author = taxonomy["author"]
                selected_work = taxonomy["work"]
            else:
                taxonomy = self._ensure_taxonomy_branch(
                    domain=selected_domain,
                    subdomain=selected_subdomain,
                    author=selected_author,
                    work=selected_work or title or "Contenuto non classificato",
                )
                selected_domain = taxonomy["domain"]
                selected_subdomain = taxonomy["subdomain"]
                selected_author = taxonomy["author"]
                selected_work = taxonomy["work"]

            normalized["canonical_theme"] = selected_domain
            normalized["subtheme"] = self._clean_field_value(selected_subdomain, max_len=120)

            llm_tags = await self._generate_search_tags(
                source_type=source_type,
                title=normalized["title"],
                description=description,
                extracted_text=extracted_text,
                source_url=source_url,
                document_type=normalized["document_type"],
                semantic_theme=semantic_theme,
                semantic_subtheme=semantic_subtheme,
                taxonomy=taxonomy,
                selected_model=selected_model,
                image_b64=image_b64,
                minimum_tags=10,
            )
            merged_keywords = self._dedupe_keywords(
                [
                    *llm_tags,
                    *normalized.get("keywords", []),
                    taxonomy.get("domain", ""),
                    taxonomy.get("subdomain", ""),
                    taxonomy.get("author", ""),
                    taxonomy.get("work", ""),
                ],
                limit=20,
            )
            normalized["keywords"] = self._ensure_min_keywords(
                current_keywords=merged_keywords,
                minimum=10,
                title=normalized["title"],
                description=description,
                extracted_text=extracted_text,
                source_type=source_type,
                taxonomy=taxonomy,
            )[:20]

            return ClassificationResult(
                title=normalized["title"],
                document_type=normalized["document_type"],
                theme=normalized["theme"],
                canonical_theme=normalized["canonical_theme"],
                subtheme=normalized["subtheme"],
                summary=normalized["summary"],
                keywords=normalized["keywords"],
                relevance_score=normalized["relevance_score"],
                conceptual_score=normalized["conceptual_score"],
                combined_score=normalized["combined_score"],
                language=normalized["language"],
                model_used=resolved_model,
                raw=response_json,
                fallback_used=False,
                semantic_theme=semantic_theme,
                semantic_subtheme=semantic_subtheme,
                taxonomy_domain=taxonomy.get("domain"),
                taxonomy_subdomain=taxonomy.get("subdomain"),
                taxonomy_author=taxonomy.get("author"),
                taxonomy_work=taxonomy.get("work"),
                taxonomy_path=taxonomy.get("path"),
            )
        except Exception as exc:  # noqa: BLE001
            fallback = self._fallback_classification(
                source_type=source_type,
                title=title,
                description=description,
                extracted_text=extracted_text,
                mime_type=mime_type,
                source_url=source_url,
                selected_model=resolved_model,
            )
            fallback.raw = {"error": str(exc)}
            fallback.fallback_used = True
            return fallback

    def _sanitize_author_name(self, value: object) -> str | None:
        candidate = str(value or "").replace("\x00", " ").strip()
        candidate = re.sub(r"\s+", " ", candidate)
        candidate = re.sub(r"^(autore|author)\s*:\s*", "", candidate, flags=re.IGNORECASE).strip()
        candidate = candidate.strip(" .,:;|-")
        if not candidate:
            return None
        if len(candidate) > 160:
            candidate = candidate[:160].rstrip(" .,:;|-")

        lowered = candidate.lower()
        blocked = {
            "sconosciuto",
            "unknown",
            "n/a",
            "na",
            "none",
            "null",
            "staff",
            "team",
            "varie",
            "general",
            "no author",
            "non disponibile",
            "youtube",
            "youtube.com",
            "www.youtube.com",
            "m.youtube.com",
            "music.youtube.com",
            "youtu.be",
        }
        normalized = lowered.removeprefix("www.")
        if lowered in blocked or normalized in blocked:
            return None
        if re.fullmatch(r"(?:https?://)?(?:www\.)?(?:youtube\.com|youtu\.be)(?:/.*)?", lowered):
            return None
        return candidate

    def _heuristic_author_name(
        self,
        *,
        title: str,
        description: str | None,
        extracted_text: str,
        source_name: str | None,
        source_type: str,
        metadata_hints: dict | None = None,
    ) -> str | None:
        hints = metadata_hints or {}

        title_scan = (title or "")[:260]
        patterns = [
            r"\bby\s+([A-Z][A-Za-zÀ-ÿ'`.-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'`.-]+){0,4})\b",
            r"\bprof\.?\s+([A-Z][A-Za-zÀ-ÿ'`.-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'`.-]+){0,4})\b",
            r"\bdr\.?\s+([A-Z][A-Za-zÀ-ÿ'`.-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'`.-]+){0,4})\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, title_scan, flags=re.IGNORECASE)
            if not match:
                continue
            value = self._sanitize_author_name(match.group(1))
            if value:
                return value

        for key in ("author", "creator", "channel", "youtube_channel"):
            value = self._sanitize_author_name(hints.get(key))
            if value:
                return value

        text_head = (extracted_text or "")[:1800]
        meta_match = re.search(
            r"(?:^|\n)(?:youtube channel|author|creator)\s*:\s*([^\n]{2,120})",
            text_head,
            flags=re.IGNORECASE,
        )
        if meta_match:
            value = self._sanitize_author_name(meta_match.group(1))
            if value:
                return value

        if source_type == "link":
            fallback = self._sanitize_author_name(source_name)
            if fallback:
                return fallback

        return None

    async def infer_author_name(
        self,
        *,
        source_type: str,
        title: str,
        description: str | None,
        extracted_text: str,
        source_url: str | None,
        source_name: str | None,
        metadata_hints: dict | None = None,
    ) -> str | None:
        heuristic = self._heuristic_author_name(
            title=title,
            description=description,
            extracted_text=extracted_text,
            source_name=source_name,
            source_type=source_type,
            metadata_hints=metadata_hints,
        )
        if heuristic:
            return heuristic

        payload = {
            "source_type": source_type,
            "title": title,
            "description": description,
            "source_name": source_name,
            "source_url": source_url,
            "metadata_hints": metadata_hints or {},
            "content_preview": (extracted_text or "")[:5000],
        }

        system_prompt = (
            "Sei un estrattore di autore/creatore principale del contenuto. "
            "Rispondi solo con il nome dell'autore, senza testo extra. "
            "Se non identificabile rispondi esattamente con: Sconosciuto."
        )
        user_prompt = (
            "Estrai l'autore principale del contenuto.\n"
            "Output ammesso: solo il nome autore oppure Sconosciuto.\n"
            f"Input: {json.dumps(payload, ensure_ascii=False)}"
        )

        request_body = {
            "model": self.text_model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resolved_model = await self._resolve_model_name(client, self.text_model)
                request_body["model"] = resolved_model
                response = await client.post(f"{self.base_url}/api/chat", json=request_body)
                if response.status_code == 404 and "not found" in response.text.lower():
                    refreshed_model = await self._resolve_model_name(
                        client,
                        self.text_model,
                        force_refresh=True,
                    )
                    if refreshed_model and refreshed_model.lower() != resolved_model.lower():
                        request_body["model"] = refreshed_model
                        response = await client.post(f"{self.base_url}/api/chat", json=request_body)
                response.raise_for_status()
                response_json = response.json()
            raw_content = str(response_json.get("message", {}).get("content", "") or "").strip()
            first_line = raw_content.splitlines()[0] if raw_content else ""
            parsed = self._sanitize_author_name(first_line)
            if parsed:
                return parsed
        except Exception:  # noqa: BLE001
            pass

        return None

    async def _resolve_model_name(
        self,
        client: httpx.AsyncClient,
        requested_model: str,
        *,
        force_refresh: bool = False,
    ) -> str:
        requested = (requested_model or "").strip()
        if not requested:
            return requested_model

        if not force_refresh and requested in self._resolved_model_cache:
            return self._resolved_model_cache[requested]

        available = await self._get_available_models(client, force_refresh=force_refresh)
        if not available:
            self._resolved_model_cache[requested] = requested
            return requested

        resolved = self._pick_best_model(requested, available)
        self._resolved_model_cache[requested] = resolved
        return resolved

    async def _get_available_models(
        self,
        client: httpx.AsyncClient,
        *,
        force_refresh: bool = False,
    ) -> list[str]:
        now = time.time()
        if not force_refresh and self._available_models_cache and now < self._available_models_cache_until:
            return self._available_models_cache

        try:
            response = await client.get(f"{self.base_url}/api/tags")
            response.raise_for_status()
            payload = response.json()
            models = payload.get("models") if isinstance(payload, dict) else []
            names = []
            if isinstance(models, list):
                for item in models:
                    if isinstance(item, dict) and item.get("name"):
                        names.append(str(item["name"]).strip())
            self._available_models_cache = names
            self._available_models_cache_until = now + 120
            return names
        except Exception:  # noqa: BLE001
            # Keep previous cache if request fails.
            if self._available_models_cache:
                return self._available_models_cache
            return []

    def _pick_best_model(self, requested: str, available_models: list[str]) -> str:
        requested_clean = requested.strip()
        available = [name.strip() for name in available_models if str(name).strip()]
        if not available:
            return requested_clean

        lookup = {name.lower(): name for name in available}
        direct = lookup.get(requested_clean.lower())
        if direct:
            return direct

        candidates: list[str] = []
        if requested_clean.endswith("-cloud"):
            candidates.append(requested_clean.removesuffix("-cloud"))
        else:
            candidates.append(f"{requested_clean}-cloud")

        if ":" in requested_clean:
            family, tag = requested_clean.split(":", 1)
            tag_plain = tag.removesuffix("-cloud")
            candidates.extend(
                [
                    f"{family}:{tag_plain}-cloud",
                    f"{family}:{tag_plain}",
                ]
            )

        for candidate in candidates:
            match = lookup.get(candidate.lower())
            if match:
                return match

        family = requested_clean.split(":", 1)[0].lower()
        family_options = [name for name in available if name.lower().startswith(f"{family}:")]
        if family_options:
            preferred = [name for name in family_options if "120b" in name.lower()]
            if preferred:
                cloud_pref = [name for name in preferred if "cloud" in name.lower()]
                return sorted(cloud_pref or preferred)[0]
            return sorted(family_options)[0]

        return requested_clean

    def _infer_theme_from_text(self, text: str, source_url: str | None = None) -> tuple[str, float, list[str]]:
        normalized = (text or "").lower()
        if not normalized:
            return ("General", 0.0, [])

        fallback_terms = self._fallback_terms_snapshot()
        for category in self._allowed_categories_snapshot():
            fallback_terms.setdefault(category, [])

        scores: dict[str, float] = {theme: 0.0 for theme in fallback_terms}
        matched_terms: dict[str, list[str]] = {theme: [] for theme in fallback_terms}

        for theme, terms in fallback_terms.items():
            for term in terms:
                token = term.lower().strip()
                if not token:
                    continue
                if re.search(rf"\b{re.escape(token)}\b", normalized):
                    # Penalize generic "Media" bucket to avoid swallowing conceptual topics.
                    weight = 0.6 if theme == "Media e Comunicazione" else 1.25
                    scores[theme] += weight
                    if len(matched_terms[theme]) < 8:
                        matched_terms[theme].append(term)

        best_theme = "General"
        best_score = 0.0
        for theme, score in scores.items():
            if score > best_score:
                best_theme = theme
                best_score = score

        if best_score > 0:
            return (best_theme, best_score, matched_terms.get(best_theme, []))

        if source_url and "youtube.com" in source_url.lower():
            return ("Media e Comunicazione", 0.6, ["youtube", "video"])

        return ("General", 0.0, [])

    def _clean_field_value(self, raw_value: object, max_len: int = 160) -> str:
        cleaned = str(raw_value or "").strip()
        cleaned = re.sub(r"^[\s\"'`*_•-]+", "", cleaned)
        cleaned = re.sub(r"[\s\"'`*_]+$", "", cleaned)
        cleaned = " ".join(cleaned.split())
        return cleaned[:max_len]

    def _parse_json(self, text: str) -> dict | None:
        if not text:
            return None

        stripped = text.strip()
        if not stripped:
            return None

        try:
            parsed = json.loads(stripped)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            match = _JSON_OBJECT.search(stripped)
            if not match:
                return None
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else None

    def _parse_classification(self, text: str) -> dict:
        if not text or not text.strip():
            raise ValueError("Missing model output")

        stripped = text.strip()
        maybe_json = self._parse_json(stripped)
        if isinstance(maybe_json, dict):
            return maybe_json

        tipologia = None
        contenuto = None
        dettaglio = None

        for raw_line in stripped.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            normalized = line.lstrip("-*•").strip()
            if ":" not in normalized:
                continue
            key, value = normalized.split(":", 1)
            key_norm = " ".join(key.lower().split())
            value_clean = self._clean_field_value(value)
            if not value_clean:
                continue
            if "tipologia documento" in key_norm or key_norm == "tipologia":
                tipologia = tipologia or value_clean
            elif "dettaglio contenuto" in key_norm or key_norm.startswith("dettaglio"):
                dettaglio = dettaglio or value_clean
            elif key_norm == "contenuto" or (
                key_norm.startswith("contenuto") and "dettaglio" not in key_norm
            ):
                contenuto = contenuto or value_clean

        if not tipologia:
            tipologia_match = _TIPOLOGIA_PATTERN.search(stripped)
            if tipologia_match:
                tipologia = self._clean_field_value(tipologia_match.group(1))
        if not contenuto:
            contenuto_match = _CONTENUTO_PATTERN.search(stripped)
            if contenuto_match:
                contenuto = self._clean_field_value(contenuto_match.group(1))
        if not dettaglio:
            dettaglio_match = _DETTAGLIO_PATTERN.search(stripped)
            if dettaglio_match:
                dettaglio = self._clean_field_value(dettaglio_match.group(1))

        if not tipologia or not contenuto or not dettaglio:
            raise ValueError(f"Invalid 3-field classification output: {stripped[:240]}")

        return {
            "document_type": tipologia,
            "theme": contenuto,
            "subtheme": dettaglio,
        }

    def _count_term_hits(self, text: str, terms: list[str]) -> int:
        total = 0
        for term in terms:
            token = term.strip().lower()
            if not token:
                continue
            if re.search(rf"\b{re.escape(token)}\b", text):
                total += 1
        return total

    def _refine_link_theme(
        self,
        *,
        canonical_theme: str,
        parsed_theme: str,
        title: str,
        description: str | None,
        extracted_text: str,
        source_url: str | None,
    ) -> tuple[str, str]:
        context = " ".join(part for part in [title, description or "", extracted_text or ""] if part).lower()
        news_hits = self._count_term_hits(context, NEWS_LINK_TERMS)
        music_hits = self._count_term_hits(context, MUSIC_LINK_TERMS)

        lowered_url = (source_url or "").lower()
        if any(hint in lowered_url for hint in NEWS_DOMAIN_HINTS):
            news_hits += 3

        is_youtube = "youtube.com" in lowered_url or "youtu.be" in lowered_url

        if canonical_theme == "Musica e Arte" and news_hits >= 3 and news_hits >= music_hits + 2:
            return ("Media e Comunicazione", "News e Attualita")

        if canonical_theme == "General" and news_hits >= 3:
            return ("Media e Comunicazione", "News e Attualita")

        if not is_youtube and news_hits >= 4 and news_hits >= music_hits + 2:
            return ("Media e Comunicazione", "News e Attualita")

        return (canonical_theme, parsed_theme)

    def _build_summary(self, *, description: str | None, extracted_text: str, title: str) -> str:
        preferred = (description or "").strip()
        if preferred:
            return " ".join(preferred.split())[:1200]

        cleaned_lines: list[str] = []
        for raw_line in (extracted_text or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            lowered = line.lower()
            if lowered in {"[metadata]", "[main content]"}:
                continue
            if lowered.startswith(("domain:", "site name:", "page kind:", "source url:")):
                continue
            cleaned_lines.append(line)
            if len(cleaned_lines) >= 12:
                break

        if cleaned_lines:
            return " ".join(" ".join(cleaned_lines).split())[:1200]
        return f"Contenuto classificato: {title}"[:1200]

    def _build_keywords(
        self,
        *,
        document_type: str | None,
        theme: str,
        subtheme: str | None,
        extracted_text: str,
        fallback_terms: list[str] | None = None,
    ) -> list[str]:
        candidates: list[str] = []
        for value in [document_type, theme, subtheme]:
            if value and value.strip():
                candidates.append(value.strip())

        for term in fallback_terms or []:
            cleaned = self._clean_field_value(term, max_len=40)
            if cleaned:
                candidates.append(cleaned)

        if len(candidates) < 6:
            for token in re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9_-]{3,}", (extracted_text or "").lower()):
                if token in {"this", "that", "with", "from", "sono", "della", "delle", "the", "for"}:
                    continue
                candidates.append(token)
                if len(candidates) >= 16:
                    break

        keywords: list[str] = []
        seen: set[str] = set()
        for value in candidates:
            cleaned = self._clean_field_value(value, max_len=40)
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            keywords.append(cleaned)
            if len(keywords) >= 12:
                break
        return keywords

    def _estimate_scores(
        self,
        *,
        canonical_theme: str,
        combined_text: str,
        source_url: str | None,
    ) -> tuple[float, float]:
        inferred_theme, signal_score, _ = self._infer_theme_from_text(combined_text, source_url=source_url)

        if canonical_theme == "General":
            relevance, conceptual = 0.54, 0.52
        else:
            relevance, conceptual = 0.84, 0.86

        if signal_score >= 3:
            relevance += 0.05
            conceptual += 0.04
        elif signal_score >= 1.5:
            relevance += 0.03
            conceptual += 0.02

        if inferred_theme != "General" and canonical_theme != inferred_theme:
            relevance -= 0.07
            conceptual -= 0.09

        return (self._bound_score(relevance), self._bound_score(conceptual))

    def _normalize(
        self,
        value: dict,
        *,
        title: str,
        source_type: str,
        extracted_text: str,
        source_url: str | None,
        description: str | None,
    ) -> dict:
        parsed_title = str(value.get("title") or title).strip()[:500]
        document_type_raw = value.get("document_type") or value.get("tipologia_documento")
        parsed_document_type = self._clean_field_value(document_type_raw, max_len=120) if document_type_raw else None

        canonical_theme_raw = value.get("canonical_theme")
        parsed_theme = self._clean_field_value(value.get("theme") or canonical_theme_raw or "General", max_len=120)
        if not parsed_theme:
            parsed_theme = "General"
        canonical_theme = self._normalize_canonical_theme(
            canonical_theme_raw or "General",
            allow_create=bool(canonical_theme_raw),
        )

        # If the model returns an inconsistent canonical theme, trust the inferred
        # semantic theme. If it does not map, create a new category dynamically.
        theme_based_canonical = self._normalize_canonical_theme(parsed_theme, allow_create=True)
        if theme_based_canonical != "General" and canonical_theme != theme_based_canonical:
            canonical_theme = theme_based_canonical

        if source_type == "link":
            canonical_theme, parsed_theme = self._refine_link_theme(
                canonical_theme=canonical_theme,
                parsed_theme=parsed_theme,
                title=title,
                description=description,
                extracted_text=extracted_text,
                source_url=source_url,
            )

        parsed_subtheme = value.get("subtheme")
        parsed_subtheme = self._clean_field_value(parsed_subtheme, max_len=120) if parsed_subtheme else None

        summary = str(value.get("summary") or "").strip()[:1200]
        if not summary:
            summary = self._build_summary(description=description, extracted_text=extracted_text, title=parsed_title or title)

        keywords_raw = value.get("keywords") or []
        if not isinstance(keywords_raw, list):
            keywords_raw = [str(keywords_raw)]
        keywords = [str(k).strip()[:40] for k in keywords_raw if str(k).strip()]
        if not keywords:
            _, _, matched_terms = self._infer_theme_from_text(
                " ".join(part for part in [parsed_title, description or "", extracted_text or ""] if part),
                source_url=source_url,
            )
            keywords = self._build_keywords(
                document_type=parsed_document_type,
                theme=parsed_theme,
                subtheme=parsed_subtheme,
                extracted_text=extracted_text,
                fallback_terms=matched_terms,
            )
        keywords = keywords[:12]

        relevance_in = value.get("relevance_score")
        conceptual_in = value.get("conceptual_score")
        if relevance_in is None or conceptual_in is None:
            relevance, conceptual = self._estimate_scores(
                canonical_theme=canonical_theme,
                combined_text=" ".join(part for part in [parsed_title, description or "", extracted_text or ""] if part),
                source_url=source_url,
            )
        else:
            relevance = self._bound_score(relevance_in)
            conceptual = self._bound_score(conceptual_in)

        combined = round((relevance * 0.6) + (conceptual * 0.4), 4)

        language = value.get("language")
        language = str(language).strip()[:16] if language else None

        return {
            "title": parsed_title or title,
            "document_type": parsed_document_type,
            "theme": parsed_theme or "Uncategorized",
            "canonical_theme": canonical_theme,
            "subtheme": parsed_subtheme,
            "summary": summary,
            "keywords": keywords,
            "relevance_score": relevance,
            "conceptual_score": conceptual,
            "combined_score": combined,
            "language": language,
        }

    def _normalize_canonical_theme(self, value: object, *, allow_create: bool = False) -> str:
        candidate = self._merge_category_label(value)
        if not candidate:
            return "General"

        candidate_lower = candidate.lower()
        if candidate_lower in {"general", "other", "misc", "varie", "altro", "uncategorized"}:
            return "General"

        allowed_categories = self._allowed_categories_snapshot()
        for theme in allowed_categories:
            if candidate_lower == theme.lower():
                return theme

        aliases = self._aliases_snapshot()
        alias_key = self._normalize_catalog_token(candidate)
        if alias_key in aliases:
            return aliases[alias_key]

        # Fuzzy fallback: allow richer outputs such as
        # "fisica teorica", "special relativity physics", etc.
        for alias, canonical in sorted(aliases.items(), key=lambda item: len(item[0]), reverse=True):
            token = alias.strip().lower()
            if not token:
                continue
            if re.search(rf"\b{re.escape(token)}\b", alias_key):
                return canonical

        if allow_create:
            return self._register_generated_category(candidate)

        return "General"

    def _bound_score(self, score: object) -> float:
        try:
            numeric = float(score)
        except (TypeError, ValueError):
            numeric = 0.5
        return round(max(0.0, min(1.0, numeric)), 4)

    def _fallback_classification(
        self,
        *,
        source_type: str,
        title: str,
        description: str | None,
        extracted_text: str,
        mime_type: str | None,
        source_url: str | None,
        selected_model: str,
    ) -> ClassificationResult:
        combined_text = " ".join(part for part in [title, description or "", extracted_text or ""] if part).strip()
        inferred_canonical, signal_score, matched = self._infer_theme_from_text(combined_text, source_url=source_url)

        summary = (description or extracted_text or "Contenuto caricato senza testo estraibile.").strip()[:280]
        if not summary:
            summary = "Contenuto caricato senza testo estraibile."

        if signal_score >= 3.5:
            relevance, conceptual = 0.78, 0.74
        elif signal_score >= 2.0:
            relevance, conceptual = 0.7, 0.66
        elif signal_score > 0:
            relevance, conceptual = 0.62, 0.58
        else:
            relevance, conceptual = 0.5, 0.5

        keywords = [token for token in matched if token][:10]
        if not keywords:
            keywords = [w for w in [source_type, mime_type or "", inferred_canonical] if w][:10]
        keywords = self._ensure_min_keywords(
            current_keywords=self._dedupe_keywords(keywords, limit=20),
            minimum=10,
            title=title,
            description=description,
            extracted_text=extracted_text,
            source_type=source_type,
            taxonomy={
                "domain": inferred_canonical,
                "subdomain": "Generale",
                "author": "Sconosciuto",
                "work": "Contenuto non classificato",
            },
        )[:20]

        return ClassificationResult(
            title=title,
            document_type="Non classificato",
            theme=inferred_canonical,
            canonical_theme=inferred_canonical,
            subtheme=None,
            summary=summary,
            keywords=keywords,
            relevance_score=relevance,
            conceptual_score=conceptual,
            combined_score=round((relevance * 0.6) + (conceptual * 0.4), 4),
            language=None,
            model_used=selected_model,
            raw={},
            fallback_used=True,
            semantic_theme=inferred_canonical,
            semantic_subtheme=None,
            taxonomy_domain=inferred_canonical,
            taxonomy_subdomain="Generale",
            taxonomy_author="Sconosciuto",
            taxonomy_work="Contenuto non classificato",
            taxonomy_path=f"{inferred_canonical} > Generale > Sconosciuto > Contenuto non classificato",
        )
