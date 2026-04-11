import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import httpx


_JSON_OBJECT = re.compile(r"\{.*\}", re.DOTALL)
_TIPOLOGIA_PATTERN = re.compile(r"tipologia\s+documento\s*:\s*(.+)", re.IGNORECASE)
_CONTENUTO_PATTERN = re.compile(r"^\s*contenuto\s*:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_DETTAGLIO_PATTERN = re.compile(r"dettaglio\s+contenuto\s*:\s*(.+)", re.IGNORECASE)
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
        self._fallback_theme_terms: dict[str, list[str]] = {
            key: list(values) for key, values in FALLBACK_THEME_TERMS.items()
        }
        self._load_category_catalog()

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

        words = [self._smart_capitalize_word(word) for word in candidate.split(" ") if word]
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
                    formatted = self._titleize_category(category)
                    if not formatted:
                        continue
                    lowered = formatted.lower()
                    if lowered not in existing_lowers:
                        self._allowed_canonical_themes.append(formatted)
                        existing_lowers.add(lowered)

            if isinstance(aliases, dict):
                for alias, category in aliases.items():
                    alias_key = self._normalize_catalog_token(alias)
                    canonical = self._titleize_category(category)
                    if not alias_key or not canonical:
                        continue
                    lowered = canonical.lower()
                    if lowered not in existing_lowers:
                        self._allowed_canonical_themes.append(canonical)
                        existing_lowers.add(lowered)
                    self._theme_aliases[alias_key] = canonical

            if isinstance(terms, dict):
                for category, items in terms.items():
                    canonical = self._titleize_category(category)
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

    def _register_generated_category(self, value: object) -> str:
        candidate = self._titleize_category(value)
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
        }
        if lowered in blocked:
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
        candidate = self._titleize_category(value)
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

        keywords = [token for token in matched if token][:8]
        if not keywords:
            keywords = [w for w in [source_type, mime_type or "", inferred_canonical] if w][:8]

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
        )
