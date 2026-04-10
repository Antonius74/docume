import json
import re
from dataclasses import dataclass

import httpx


_JSON_OBJECT = re.compile(r"\{.*\}", re.DOTALL)
ALLOWED_CANONICAL_THEMES = [
    "Matematica e Statistica",
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
    "ai": "AI e Machine Learning",
    "ml": "AI e Machine Learning",
    "machine learning": "AI e Machine Learning",
    "cucina": "Cucina e Food",
    "food": "Cucina e Food",
    "musica": "Musica e Arte",
    "music": "Musica e Arte",
    "software": "Programmazione e Software",
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
        timeout_seconds: int = 45,
    ):
        self.base_url = base_url.rstrip("/")
        selected_text_model = (text_model or model or "gpt-oss:120b").strip()
        selected_image_model = (image_model or model or selected_text_model).strip()
        self.text_model = selected_text_model
        self.image_model = selected_image_model
        self.timeout_seconds = timeout_seconds

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

        system_prompt = (
            "Sei un classificatore di knowledge base aziendale. "
            "Devi assegnare tema concettuale, pertinenza e sintesi in modo pratico. "
            "Usa categorie di dominio (es. Matematica e Statistica, AI e Machine Learning, Cucina e Food) "
            "e non categorie di formato come video/audio/link. "
            "Classifica SOLO in base al contenuto semantico del testo fornito. "
            "Scegli un solo tema dominante: non mescolare domini diversi. "
            "Ignora elementi marginali/non centrali del contenuto. "
            "Se è presente un'immagine, usa anche il contenuto visivo per classificare. "
            "Restituisci solo JSON valido."
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
        canonical_choices = ", ".join(ALLOWED_CANONICAL_THEMES)
        user_prompt = (
            "Analizza il contenuto e restituisci SOLO questo JSON con queste chiavi:\n"
            "{\n"
            '  "title": "titolo pulito",\n'
            f'  "canonical_theme": "una delle categorie: {canonical_choices}",\n'
            '  "theme": "tema principale in forma breve",\n'
            '  "subtheme": "sotto-tema o null",\n'
            '  "summary": "descrizione sintetica max 90 parole",\n'
            '  "keywords": ["k1", "k2", "k3"],\n'
            '  "relevance_score": 0.0,\n'
            '  "conceptual_score": 0.0,\n'
            '  "language": "it|en|..."\n'
            "}\n"
            "I punteggi devono essere tra 0 e 1.\n"
            "La canonical_theme deve essere coerente con il tema dominante del contenuto.\n"
            "Se has_image=true, deduci tema e sotto-tema dagli elementi visivi principali.\n"
            f"{link_specific_rules}\n"
            f"Input: {json.dumps(payload, ensure_ascii=False)}"
        )

        message = {"role": "user", "content": user_prompt}
        if image_b64:
            message["images"] = [image_b64]

        request_body = {
            "model": selected_model,
            "stream": False,
            "format": "json",
            "messages": [
                {"role": "system", "content": system_prompt},
                message,
            ],
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(f"{self.base_url}/api/chat", json=request_body)
                response.raise_for_status()
                response_json = response.json()

            content = response_json.get("message", {}).get("content", "")
            parsed = self._parse_json(content)
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
                theme=normalized["theme"],
                canonical_theme=normalized["canonical_theme"],
                subtheme=normalized["subtheme"],
                summary=normalized["summary"],
                keywords=normalized["keywords"],
                relevance_score=normalized["relevance_score"],
                conceptual_score=normalized["conceptual_score"],
                combined_score=normalized["combined_score"],
                language=normalized["language"],
                model_used=selected_model,
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
                selected_model=selected_model,
            )
            fallback.raw = {"error": str(exc)}
            fallback.fallback_used = True
            return fallback

    def _infer_theme_from_text(self, text: str, source_url: str | None = None) -> tuple[str, float, list[str]]:
        normalized = (text or "").lower()
        if not normalized:
            return ("General", 0.0, [])

        scores: dict[str, float] = {theme: 0.0 for theme in FALLBACK_THEME_TERMS}
        matched_terms: dict[str, list[str]] = {theme: [] for theme in FALLBACK_THEME_TERMS}

        for theme, terms in FALLBACK_THEME_TERMS.items():
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

    def _parse_json(self, text: str) -> dict:
        if not text:
            raise ValueError("Missing JSON from model output")

        stripped = text.strip()
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            match = _JSON_OBJECT.search(stripped)
            if not match:
                raise
            return json.loads(match.group(0))

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
        canonical_theme_raw = value.get("canonical_theme")
        parsed_theme = str(value.get("theme") or canonical_theme_raw or "General").strip()[:120]
        canonical_theme = self._normalize_canonical_theme(canonical_theme_raw or "General")

        # If the model returns an inconsistent canonical theme, trust the inferred
        # semantic theme when it maps to a known category.
        theme_based_canonical = self._normalize_canonical_theme(parsed_theme)
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
        parsed_subtheme = str(parsed_subtheme).strip()[:120] if parsed_subtheme else None

        summary = str(value.get("summary") or "").strip()[:1200]
        keywords_raw = value.get("keywords") or []
        if not isinstance(keywords_raw, list):
            keywords_raw = [str(keywords_raw)]
        keywords = [str(k).strip()[:40] for k in keywords_raw if str(k).strip()]
        keywords = keywords[:12]

        relevance = self._bound_score(value.get("relevance_score"))
        conceptual = self._bound_score(value.get("conceptual_score"))
        combined = round((relevance * 0.6) + (conceptual * 0.4), 4)

        language = value.get("language")
        language = str(language).strip()[:16] if language else None

        return {
            "title": parsed_title or title,
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

    def _normalize_canonical_theme(self, value: object) -> str:
        candidate = str(value or "").strip()
        if not candidate:
            return "General"

        for theme in ALLOWED_CANONICAL_THEMES:
            if candidate.lower() == theme.lower():
                return theme

        alias_key = candidate.lower()
        if alias_key in THEME_ALIASES:
            return THEME_ALIASES[alias_key]

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
