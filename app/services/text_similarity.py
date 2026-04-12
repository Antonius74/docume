import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher


STOPWORDS = {
    "a",
    "ad",
    "al",
    "alla",
    "alle",
    "allo",
    "and",
    "are",
    "con",
    "da",
    "dei",
    "del",
    "della",
    "delle",
    "di",
    "e",
    "ed",
    "for",
    "gli",
    "i",
    "il",
    "in",
    "is",
    "la",
    "le",
    "lo",
    "of",
    "on",
    "or",
    "per",
    "the",
    "to",
    "un",
    "una",
    "uno",
}


@dataclass
class SimilarityProfile:
    score: float
    token_coverage: float
    fuzzy_token_avg: float
    prefix_coverage: float
    exact_substring: bool


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    value = unicodedata.normalize("NFKD", str(text))
    value = "".join(ch for ch in value if not unicodedata.combining(ch)).lower()
    value = value.replace("\x00", " ")
    value = re.sub(r"[^0-9a-zà-ÿ\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _stem_token(token: str) -> str:
    output = token
    for suffix in (
        "azioni",
        "azione",
        "mente",
        "zione",
        "zioni",
        "izzare",
        "izzati",
        "izzati",
        "ing",
        "ed",
        "ly",
        "tion",
    ):
        if len(output) > 5 and output.endswith(suffix):
            output = output[: -len(suffix)]
            break
    return output


def _tokens(text: str) -> list[str]:
    return [token for token in text.split(" ") if len(token) >= 2 and token not in STOPWORDS]


def _stemmed_tokens(text: str) -> list[str]:
    output: list[str] = []
    for token in _tokens(text):
        stemmed = _stem_token(token)
        if len(stemmed) >= 3:
            output.append(stemmed)
    return output


def tokenize_text(value: str | None, *, limit: int = 220) -> list[str]:
    tokens = _stemmed_tokens(_normalize(value))
    # Keep order but deduplicate and bound size for performance.
    output: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        output.append(token)
        if len(output) >= limit:
            break
    return output


def _char_ngrams(text: str, n: int = 3) -> list[str]:
    if len(text) < n:
        return [text] if text else []
    padded = f"  {text}  "
    return [padded[i : i + n] for i in range(len(padded) - n + 1)]


def token_jaccard_similarity(a: str | None, b: str | None) -> float:
    left = set(_stemmed_tokens(_normalize(a)))
    right = set(_stemmed_tokens(_normalize(b)))
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def trigram_dice_similarity(a: str | None, b: str | None) -> float:
    left = Counter(_char_ngrams(_normalize(a), n=3))
    right = Counter(_char_ngrams(_normalize(b), n=3))
    if not left or not right:
        return 0.0
    shared = sum((left & right).values())
    total = sum(left.values()) + sum(right.values())
    if total == 0:
        return 0.0
    return (2.0 * shared) / total


def token_containment_score(a: str | None, b: str | None) -> float:
    query_tokens = set(_stemmed_tokens(_normalize(a)))
    text_tokens = set(_stemmed_tokens(_normalize(b)))
    if not query_tokens or not text_tokens:
        return 0.0
    return len(query_tokens & text_tokens) / max(1, len(query_tokens))


def token_prefix_score(a: str | None, b: str | None) -> float:
    query_tokens = _stemmed_tokens(_normalize(a))
    text_tokens = _stemmed_tokens(_normalize(b))
    if not query_tokens or not text_tokens:
        return 0.0

    hits = 0
    for token in query_tokens:
        if any(
            (len(token) >= 4 and len(tt) >= 4 and (tt.startswith(token) or token.startswith(tt)))
            for tt in text_tokens
        ):
            hits += 1
    return hits / max(1, len(query_tokens))


def token_set_ratio(a: str | None, b: str | None) -> float:
    left = sorted(set(_stemmed_tokens(_normalize(a))))
    right = sorted(set(_stemmed_tokens(_normalize(b))))
    if not left or not right:
        return 0.0
    left_text = " ".join(left)
    right_text = " ".join(right)
    return SequenceMatcher(a=left_text, b=right_text).ratio()


def ordered_token_score(a: str | None, b: str | None) -> float:
    query_tokens = _stemmed_tokens(_normalize(a))
    text_tokens = _stemmed_tokens(_normalize(b))
    if not query_tokens or not text_tokens:
        return 0.0

    index = 0
    hits = 0
    for query_token in query_tokens:
        found_at = -1
        for pos in range(index, len(text_tokens)):
            if text_tokens[pos] == query_token:
                found_at = pos
                break
        if found_at >= 0:
            hits += 1
            index = found_at + 1
    return hits / max(1, len(query_tokens))


def _best_ratio(query_token: str, text_tokens: list[str]) -> float:
    if not query_token or not text_tokens:
        return 0.0

    best = 0.0
    for token in text_tokens:
        if token == query_token:
            return 1.0
        if len(query_token) >= 4 and len(token) >= 4 and (token.startswith(query_token) or query_token.startswith(token)):
            best = max(best, 0.94)
            continue

        ratio = SequenceMatcher(a=query_token, b=token).ratio()
        if ratio > best:
            best = ratio
    return best


def similarity_profile(query: str | None, text: str | None) -> SimilarityProfile:
    query_norm = _normalize(query)
    text_norm = _normalize(text)
    if not query_norm or not text_norm:
        return SimilarityProfile(
            score=0.0,
            token_coverage=0.0,
            fuzzy_token_avg=0.0,
            prefix_coverage=0.0,
            exact_substring=False,
        )

    exact = query_norm in text_norm
    query_tokens = tokenize_text(query_norm, limit=18)
    text_tokens = tokenize_text(text_norm, limit=260)

    if not query_tokens or not text_tokens:
        return SimilarityProfile(
            score=1.0 if exact else 0.0,
            token_coverage=1.0 if exact else 0.0,
            fuzzy_token_avg=1.0 if exact else 0.0,
            prefix_coverage=1.0 if exact else 0.0,
            exact_substring=exact,
        )

    best_ratios = [_best_ratio(token, text_tokens) for token in query_tokens]
    strong_hits = sum(1 for ratio in best_ratios if ratio >= 0.78)
    token_coverage = strong_hits / max(1, len(query_tokens))
    fuzzy_avg = sum(best_ratios) / max(1, len(best_ratios))
    prefix_cov = token_prefix_score(query_norm, text_norm)
    set_ratio = token_set_ratio(query_norm, text_norm)
    trigram = trigram_dice_similarity(query_norm, text_norm)

    score = (
        (0.28 * (1.0 if exact else 0.0))
        + (0.24 * token_coverage)
        + (0.18 * fuzzy_avg)
        + (0.12 * prefix_cov)
        + (0.10 * set_ratio)
        + (0.08 * trigram)
    )
    score = round(max(0.0, min(1.0, score)), 6)

    return SimilarityProfile(
        score=score,
        token_coverage=round(token_coverage, 6),
        fuzzy_token_avg=round(fuzzy_avg, 6),
        prefix_coverage=round(prefix_cov, 6),
        exact_substring=exact,
    )


def text_similarity_score(query: str | None, text: str | None) -> float:
    return similarity_profile(query, text).score
