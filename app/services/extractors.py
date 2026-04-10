import base64
import re
import shutil
import subprocess
import zipfile
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader


_TEXT_EXTENSIONS = {
    ".txt",
    ".md",
    ".csv",
    ".json",
    ".xml",
    ".yml",
    ".yaml",
    ".py",
    ".js",
    ".ts",
    ".html",
    ".css",
    ".sql",
}


def _truncate(value: str, max_chars: int) -> str:
    return value[:max_chars].strip()


def _extract_docx_preview(file_path: Path, max_document_pages: int) -> str:
    try:
        from docx import Document  # type: ignore
    except Exception:  # noqa: BLE001
        return (
            f"DOCX file rilevato ({file_path.name}). "
            "Anteprima testo non disponibile perché python-docx non è installato."
        )

    try:
        document = Document(str(file_path))
    except Exception as exc:  # noqa: BLE001
        return f"DOCX file {file_path.name} non leggibile: {exc}"

    estimated_words_per_page = 450
    max_words = max(1, max_document_pages) * estimated_words_per_page
    collected: list[str] = []
    words_seen = 0

    for paragraph in document.paragraphs:
        text = paragraph.text.strip()
        if not text:
            continue
        collected.append(text)
        words_seen += len(text.split())
        if words_seen >= max_words:
            break

    if not collected:
        return f"DOCX file {file_path.name} senza testo estratto."
    return "\n".join(collected)


def _extract_doc_preview(file_path: Path, max_document_pages: int) -> str:
    # On macOS use textutil for legacy .doc extraction when available.
    textutil = shutil.which("textutil")
    if not textutil:
        return (
            f"File DOC rilevato ({file_path.name}). "
            "Anteprima non disponibile: converti in DOCX/PDF per una classificazione migliore."
        )

    try:
        result = subprocess.run(
            [textutil, "-convert", "txt", "-stdout", str(file_path)],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception as exc:  # noqa: BLE001
        return f"File DOC {file_path.name} non leggibile: {exc}"

    plain = (result.stdout or "").strip()
    if not plain:
        return f"File DOC {file_path.name} senza testo estraibile."

    estimated_words_per_page = 450
    max_words = max(1, max_document_pages) * estimated_words_per_page
    words = plain.split()
    limited = " ".join(words[:max_words])
    return limited


def _extract_pptx_preview(file_path: Path, max_document_pages: int) -> str:
    try:
        with zipfile.ZipFile(file_path, "r") as archive:
            slide_paths = sorted(
                [
                    name
                    for name in archive.namelist()
                    if name.startswith("ppt/slides/slide") and name.endswith(".xml")
                ],
                key=lambda value: int(re.search(r"slide(\d+)\.xml$", value).group(1))
                if re.search(r"slide(\d+)\.xml$", value)
                else 10_000,
            )
            preview_paths = slide_paths[: max(1, max_document_pages)]
            chunks: list[str] = []
            for slide_path in preview_paths:
                xml_raw = archive.read(slide_path)
                root = ET.fromstring(xml_raw)
                texts = [
                    node.text.strip()
                    for node in root.iter()
                    if node.tag.endswith("}t") and node.text and node.text.strip()
                ]
                if texts:
                    chunks.append(" ".join(texts))
    except Exception as exc:  # noqa: BLE001
        return f"PPTX {file_path.name} non leggibile: {exc}"

    if not chunks:
        return f"PPTX {file_path.name} senza testo estraibile."
    return "\n".join(chunks)


def _extract_xlsx_preview(file_path: Path, max_document_pages: int) -> str:
    try:
        with zipfile.ZipFile(file_path, "r") as archive:
            shared_strings: list[str] = []
            if "xl/sharedStrings.xml" in archive.namelist():
                root_shared = ET.fromstring(archive.read("xl/sharedStrings.xml"))
                for si in [node for node in root_shared.iter() if node.tag.endswith("}si")]:
                    parts = [
                        node.text.strip()
                        for node in si.iter()
                        if node.tag.endswith("}t") and node.text and node.text.strip()
                    ]
                    shared_strings.append(" ".join(parts))

            sheet_paths = sorted(
                [
                    name
                    for name in archive.namelist()
                    if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
                ],
                key=lambda value: int(re.search(r"sheet(\d+)\.xml$", value).group(1))
                if re.search(r"sheet(\d+)\.xml$", value)
                else 10_000,
            )
            preview_paths = sheet_paths[: max(1, max_document_pages)]
            max_cells = max(120, max_document_pages * 260)
            cells: list[str] = []

            for sheet_path in preview_paths:
                if len(cells) >= max_cells:
                    break
                root_sheet = ET.fromstring(archive.read(sheet_path))
                for cell in [node for node in root_sheet.iter() if node.tag.endswith("}c")]:
                    value_node = next((n for n in cell if n.tag.endswith("}v")), None)
                    if value_node is None or not value_node.text:
                        continue
                    raw_value = value_node.text.strip()
                    if not raw_value:
                        continue

                    if cell.attrib.get("t") == "s":
                        try:
                            idx = int(raw_value)
                            parsed_value = shared_strings[idx] if idx < len(shared_strings) else raw_value
                        except Exception:  # noqa: BLE001
                            parsed_value = raw_value
                    else:
                        parsed_value = raw_value

                    cleaned = " ".join(parsed_value.split())
                    if cleaned:
                        cells.append(cleaned)
                    if len(cells) >= max_cells:
                        break
    except Exception as exc:  # noqa: BLE001
        return f"XLSX {file_path.name} non leggibile: {exc}"

    if not cells:
        return f"XLSX {file_path.name} senza testo estraibile."
    return " | ".join(cells)


def _extract_legacy_binary_preview(file_path: Path, max_document_pages: int) -> str:
    try:
        max_bytes = max(120_000, max_document_pages * 90_000)
        raw = file_path.read_bytes()[:max_bytes]
    except Exception as exc:  # noqa: BLE001
        return f"File {file_path.name} non leggibile: {exc}"

    decoded = raw.decode("latin-1", errors="ignore")
    candidates = re.findall(r"[A-Za-z0-9][A-Za-z0-9 ,.;:()/_%+-]{3,}", decoded)

    snippets: list[str] = []
    seen: set[str] = set()
    max_snippets = max(80, max_document_pages * 35)
    for candidate in candidates:
        cleaned = " ".join(candidate.split())
        if len(cleaned) < 4:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        snippets.append(cleaned)
        if len(snippets) >= max_snippets:
            break

    if not snippets:
        return (
            f"File legacy {file_path.name} senza testo estraibile. "
            "Per accuratezza migliore converti in formato moderno (DOCX/PPTX/XLSX/PDF)."
        )

    return "\n".join(snippets)


def _youtube_video_id(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.netloc.lower()

    if "youtu.be" in host:
        return parsed.path.strip("/") or None

    if "youtube.com" in host:
        if parsed.path == "/watch":
            return parse_qs(parsed.query).get("v", [None])[0]
        if parsed.path.startswith("/shorts/"):
            return parsed.path.split("/shorts/")[-1].split("/")[0]
        if parsed.path.startswith("/embed/"):
            return parsed.path.split("/embed/")[-1].split("/")[0]

    return None


async def extract_from_link(url: str, timeout_seconds: int, max_chars: int) -> dict:
    title = "Untitled link"
    description = ""
    extracted_text = ""

    video_id = _youtube_video_id(url)

    try:
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        body = response.text

        if "text/html" in content_type:
            soup = BeautifulSoup(body, "html.parser")
            if soup.title and soup.title.text.strip():
                title = soup.title.text.strip()

            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content"):
                description = meta_desc["content"].strip()

            nodes = soup.find_all(["h1", "h2", "h3", "p", "li"])
            chunks = [node.get_text(" ", strip=True) for node in nodes]
            extracted_text = "\n".join(chunk for chunk in chunks if chunk)
        else:
            extracted_text = body
    except Exception as exc:  # noqa: BLE001
        extracted_text = f"Link non scaricabile automaticamente. URL: {url}. Motivo: {exc}"

    if video_id:
        extracted_text = f"YouTube video id: {video_id}\nURL: {url}\n{extracted_text}"

    return {
        "title": title,
        "description": description,
        "text": _truncate(extracted_text, max_chars),
        "youtube_video_id": video_id,
    }


def extract_from_file(path: str, mime_type: str | None, max_chars: int, max_document_pages: int) -> dict:
    file_path = Path(path)
    ext = file_path.suffix.lower()
    mime = (mime_type or "").lower()

    if mime.startswith("image/"):
        image_bytes = file_path.read_bytes()
        return {
            "text": (
                f"Image file {file_path.name} ({mime_type or 'unknown'}). "
                "Classifica il contenuto visivo dell'immagine in una categoria tematica."
            ),
            "image_b64": base64.b64encode(image_bytes).decode("utf-8"),
            "language": None,
        }

    if mime == "application/pdf" or ext == ".pdf":
        try:
            reader = PdfReader(str(file_path))
        except Exception as exc:  # noqa: BLE001
            return {"text": f"PDF {file_path.name} non leggibile: {exc}", "language": None}
        text_parts = []
        for page in reader.pages[: max(1, max_document_pages)]:
            try:
                text_parts.append(page.extract_text() or "")
            except Exception:  # noqa: BLE001
                continue
        pdf_text = "\n".join(text_parts)
        pdf_text = (
            f"[Preview prime {max(1, max_document_pages)} pagine PDF]\n{pdf_text}"
            if pdf_text
            else f"PDF {file_path.name} senza testo estraibile nelle prime {max(1, max_document_pages)} pagine."
        )
        return {"text": _truncate(pdf_text, max_chars), "language": None}

    if (
        ext == ".docx"
        or mime
        in {
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        }
    ):
        docx_text = _extract_docx_preview(file_path, max_document_pages=max_document_pages)
        docx_text = f"[Preview prime ~{max(1, max_document_pages)} pagine DOCX]\n{docx_text}"
        return {"text": _truncate(docx_text, max_chars), "language": None}

    if ext == ".doc" or mime == "application/msword":
        doc_text = _extract_doc_preview(file_path, max_document_pages=max_document_pages)
        doc_text = f"[Preview prime ~{max(1, max_document_pages)} pagine DOC]\n{doc_text}"
        return {
            "text": _truncate(doc_text, max_chars),
            "language": None,
        }

    if (
        ext == ".pptx"
        or mime
        in {
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        }
    ):
        pptx_text = _extract_pptx_preview(file_path, max_document_pages=max_document_pages)
        pptx_text = f"[Preview prime ~{max(1, max_document_pages)} slide PPTX]\n{pptx_text}"
        return {"text": _truncate(pptx_text, max_chars), "language": None}

    if ext == ".ppt" or mime in {"application/vnd.ms-powerpoint"}:
        ppt_text = _extract_legacy_binary_preview(file_path, max_document_pages=max_document_pages)
        ppt_text = f"[Preview contenuto PPT legacy]\n{ppt_text}"
        return {"text": _truncate(ppt_text, max_chars), "language": None}

    if (
        ext == ".xlsx"
        or mime
        in {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
    ):
        xlsx_text = _extract_xlsx_preview(file_path, max_document_pages=max_document_pages)
        xlsx_text = f"[Preview prime ~{max(1, max_document_pages)} sheet XLSX]\n{xlsx_text}"
        return {"text": _truncate(xlsx_text, max_chars), "language": None}

    if ext == ".xls" or mime in {"application/vnd.ms-excel", "application/msexcel"}:
        xls_text = _extract_legacy_binary_preview(file_path, max_document_pages=max_document_pages)
        xls_text = f"[Preview contenuto XLS legacy]\n{xls_text}"
        return {"text": _truncate(xls_text, max_chars), "language": None}

    if mime.startswith("audio/"):
        return {
            "text": f"Audio file: {file_path.name}. MIME: {mime_type}. Trascrizione non disponibile in questa prima versione.",
            "language": None,
        }

    if mime.startswith("video/"):
        return {
            "text": f"Video file: {file_path.name}. MIME: {mime_type}. Trascrizione non disponibile in questa prima versione.",
            "language": None,
        }

    if mime.startswith("text/") or ext in _TEXT_EXTENSIONS:
        content = file_path.read_text(encoding="utf-8", errors="ignore")
        return {"text": _truncate(content, max_chars), "language": None}

    binary = file_path.read_bytes()
    try:
        content = binary.decode("utf-8", errors="ignore")
    except Exception:  # noqa: BLE001
        content = f"Binary file {file_path.name} ({mime_type or 'unknown'})"

    return {"text": _truncate(content, max_chars), "language": None}
