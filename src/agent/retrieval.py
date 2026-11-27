# -*- coding: utf-8 -*-
"""Search over the project's written documentation (retrieval for the agent).

The district and postcode tools answer questions about numbers. Questions about
method and meaning ("what does Urgent Action mean?", "how was the model
tested?") are answered from text instead: the help page, the notes in the model
performance report and the README.

The text is split into short passages. A question is matched against them in
two ways and the two rankings are combined with reciprocal rank fusion:

- keyword search (BM25), which always works and needs nothing installed
- embedding search with the Gemini embedding API, when an API key is set

If the embedding call fails, search carries on with keywords alone and says so
in its result. Only the standard library is used, so the Flask image stays small.
"""
from __future__ import annotations

import hashlib
import math
import os
import re
import time
from collections import Counter
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Iterable

EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:{method}"
EMBEDDING_DIMENSIONS = 768
MAX_PASSAGE_CHARS = 700
RRF_K = 60
EMBEDDING_RETRY_SECONDS = 600

README_SECTIONS = ("", "What you can do with it", "How it works", "The two apps", "AI assistant", "Data and privacy")

_STOPWORDS = set(
    "a an and are as at be by does for from has have how i in is it its of on or that the this to was what when "
    "where which who why will with do can there their they them these those than then so if not no".split()
)
_TOKEN = re.compile(r"[a-z0-9]+(?:[.'][a-z0-9]+)*")


def tokenize(text: str) -> list[str]:
    return [word for word in _TOKEN.findall(text.lower()) if word not in _STOPWORDS]


@dataclass(frozen=True)
class Passage:
    id: str
    source: str
    section: str
    text: str


# ------------------------------------------------------------------ chunking
class _BlockParser(HTMLParser):
    """Collect headings, table rows, list items and paragraphs from an HTML page."""

    BLOCK_TAGS = {"p", "li", "tr"}
    HEADINGS = {"h1", "h2", "h3"}
    SKIP = {"script", "style", "nav", "header"}

    def __init__(self) -> None:
        super().__init__()
        self.blocks: list[tuple[str, str, bool]] = []  # (section, text, can be merged with neighbours)
        self.section = ""
        self._buffer: list[str] | None = None
        self._cells: list[str] | None = None
        self._cell: list[str] | None = None
        self._kind = ""
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag in self.SKIP:
            self._skip += 1
        elif tag in self.HEADINGS:
            self._buffer, self._kind = [], tag
        elif tag == "tr":
            self._cells, self._kind = [], "tr"
        elif tag in ("td", "th") and self._cells is not None:
            self._cell = []
        elif tag in ("p", "li") and self._cells is None:
            self._buffer, self._kind = [], tag

    def handle_data(self, data: str) -> None:
        if self._skip:
            return
        if self._cell is not None:
            self._cell.append(data)
        elif self._buffer is not None:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in self.SKIP:
            self._skip = max(0, self._skip - 1)
        elif tag in self.HEADINGS and self._buffer is not None and self._kind == tag:
            self.section = _squash("".join(self._buffer))
            self._buffer = None
        elif tag in ("td", "th") and self._cell is not None and self._cells is not None:
            self._cells.append(_squash("".join(self._cell)))
            self._cell = None
        elif tag == "tr" and self._cells is not None:
            cells = [cell for cell in self._cells if cell]
            # Header rows carry no definitions; a row with a term and its meaning reads as "Term: meaning".
            if len(cells) >= 2 and not all(len(cell) < 30 for cell in cells):
                self.blocks.append((self.section, f"{cells[0]}: {' '.join(cells[1:])}", False))
            self._cells = None
        elif tag in ("p", "li") and self._buffer is not None and self._kind == tag:
            text = _squash("".join(self._buffer))
            if len(text) > 25:
                self.blocks.append((self.section, text, True))
            self._buffer = None


def _squash(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def html_passages(html: str, source: str) -> list[Passage]:
    parser = _BlockParser()
    parser.feed(html)
    return _pack(parser.blocks, source)


def markdown_passages(markdown: str, source: str, sections: Iterable[str] | None = None) -> list[Passage]:
    """Split a Markdown file into passages by heading, skipping code blocks and images."""
    wanted = set(sections) if sections is not None else None
    blocks: list[tuple[str, str, bool]] = []
    section, in_code, paragraph = "", False, []

    def flush() -> None:
        text = _squash(" ".join(paragraph))
        paragraph.clear()
        if text and (wanted is None or section in wanted):
            blocks.append((section, text, True))

    for line in markdown.splitlines():
        if line.strip().startswith("```"):
            in_code = not in_code
            flush()
            continue
        if in_code:
            continue
        heading = re.match(r"^(#{1,3})\s+(.*)", line)
        if heading:
            flush()
            section = "" if heading.group(1) == "#" else heading.group(2).strip()
            continue
        stripped = line.strip()
        if not stripped or stripped.startswith("!["):
            flush()
            continue
        if re.match(r"^\|?\s*-{3,}", stripped):
            continue
        if stripped.startswith(("* ", "- ", "|")) or re.match(r"^\d+\.\s", stripped):
            flush()
        text = re.sub(r"\*\*|`", "", stripped.strip("|").replace("|", " - "))
        text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
        paragraph.append(text.lstrip("*- ").strip())
    flush()
    return _pack(blocks, source)


def _pack(blocks: list[tuple[str, str, bool]], source: str) -> list[Passage]:
    """Merge short neighbouring paragraphs from the same section into passages of a readable size.

    Definition rows are kept as passages of their own, since each one answers a question by itself.
    """
    passages: list[Passage] = []
    current_section, current = None, ""
    for section, text, mergeable in blocks:
        if current and (not mergeable or section != current_section or len(current) + len(text) + 1 > MAX_PASSAGE_CHARS):
            passages.append(_passage(source, current_section or "", current))
            current = ""
        if not mergeable:
            passages.append(_passage(source, section, text))
            continue
        current_section = section
        current = f"{current} {text}".strip()
    if current:
        passages.append(_passage(source, current_section or "", current))
    return passages


def _passage(source: str, section: str, text: str) -> Passage:
    digest = hashlib.sha1(f"{source}|{section}|{text}".encode("utf-8")).hexdigest()[:8]
    return Passage(id=f"{source}#{digest}", source=source, section=section, text=text)


def load_documentation(
    help_html: str | None = None,
    reports_dir: str | Path | None = None,
    readme_path: str | Path | None = None,
) -> list[Passage]:
    """Passages from the help page, the model performance report and the README, where available."""
    passages: list[Passage] = []
    if help_html:
        passages += html_passages(help_html, "Help page")
    if reports_dir is not None:
        report = Path(reports_dir) / "model_performance.html"
        if report.exists():
            passages += html_passages(report.read_text(encoding="utf-8"), "Model performance report")
    if readme_path is not None and Path(readme_path).exists():
        passages += markdown_passages(Path(readme_path).read_text(encoding="utf-8"), "README", README_SECTIONS)
    return passages


# ------------------------------------------------------------------ keyword search
class BM25:
    def __init__(self, documents: list[list[str]], k1: float = 1.5, b: float = 0.75) -> None:
        self.documents = documents
        self.k1, self.b = k1, b
        self.lengths = [len(doc) for doc in documents]
        self.average = (sum(self.lengths) / len(documents)) if documents else 0.0
        self.counts = [Counter(doc) for doc in documents]
        frequency: Counter[str] = Counter()
        for doc in documents:
            frequency.update(set(doc))
        total = len(documents)
        self.idf = {term: math.log(1 + (total - n + 0.5) / (n + 0.5)) for term, n in frequency.items()}

    def scores(self, query: list[str]) -> list[float]:
        results = []
        for counts, length in zip(self.counts, self.lengths):
            score = 0.0
            for term in query:
                tf = counts.get(term, 0)
                if tf:
                    norm = tf + self.k1 * (1 - self.b + self.b * length / (self.average or 1))
                    score += self.idf.get(term, 0.0) * tf * (self.k1 + 1) / norm
            results.append(score)
        return results


# ------------------------------------------------------------------ embedding search
Embedder = Callable[[list[str], str], list[list[float]]]  # (texts, task_type) -> vectors


def gemini_embedder(api_key: str | None = None, model: str | None = None, transport: Callable | None = None) -> Embedder | None:
    """An embedding function backed by the Gemini API, or None when there is no key."""
    api_key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return None
    model = model or os.getenv("BROWNSEA_EMBEDDING_MODEL") or EMBEDDING_MODEL
    if transport is None:
        from src.agent.llm import _http_post as transport  # noqa: PLC0415 (keeps the import graph simple)

    def embed(texts: list[str], task_type: str) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), 100):  # the API takes up to 100 texts per call
            batch = texts[start:start + 100]
            body = {"requests": [{
                "model": f"models/{model}",
                "content": {"parts": [{"text": text}]},
                "taskType": task_type,
                "outputDimensionality": EMBEDDING_DIMENSIONS,
            } for text in batch]}
            data = transport(EMBEDDING_URL.format(model=model, method="batchEmbedContents"),
                             {"Content-Type": "application/json", "x-goog-api-key": api_key}, body)
            vectors += [item["values"] for item in data.get("embeddings", [])]
        if len(vectors) != len(texts):
            raise ValueError("The embedding API returned the wrong number of vectors.")
        return vectors

    return embed


def _cosine(left: list[float], right: list[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right))
    norm = math.sqrt(sum(a * a for a in left)) * math.sqrt(sum(b * b for b in right))
    return dot / norm if norm else 0.0


def _ranks(scores: list[float], keep_zero: bool) -> dict[int, int]:
    order = sorted((i for i, score in enumerate(scores) if keep_zero or score > 0), key=lambda i: -scores[i])
    return {index: rank for rank, index in enumerate(order, start=1)}


# ------------------------------------------------------------------ the index
class DocumentIndex:
    """Hybrid keyword and embedding search over a list of passages."""

    def __init__(self, passages: list[Passage], embedder: Embedder | None = None) -> None:
        self.passages = passages
        self.embedder = embedder
        self.bm25 = BM25([tokenize(f"{p.section} {p.text}") for p in passages])
        self._vectors: list[list[float]] | None = None
        self.embedding_problem = ""
        self._retry_at = 0.0

    def _passage_vectors(self) -> list[list[float]] | None:
        """Embed the passages once, on the first search, and keep them in memory."""
        if self._vectors is None and self.embedder is not None and time.monotonic() >= self._retry_at:
            try:
                texts = [f"{p.section}: {p.text}" if p.section else p.text for p in self.passages]
                self._vectors = self.embedder(texts, "RETRIEVAL_DOCUMENT")
                self.embedding_problem = ""
            except Exception as exc:  # any API problem: use keyword search for a while, then try again
                self.embedding_problem = str(exc)[:200]
                self._retry_at = time.monotonic() + EMBEDDING_RETRY_SECONDS
        return self._vectors

    def search(self, query: str, k: int = 4) -> dict[str, Any]:
        if not self.passages:
            return {"mode": "none", "passages": [], "message": "No documentation is available in this release."}
        keyword = _ranks(self.bm25.scores(tokenize(query)), keep_zero=False)

        dense: dict[int, int] = {}
        vectors = self._passage_vectors()
        if vectors is not None:
            try:
                query_vector = self.embedder([query], "RETRIEVAL_QUERY")[0]  # type: ignore[misc]
                dense = _ranks([_cosine(query_vector, v) for v in vectors], keep_zero=True)
            except Exception as exc:
                self.embedding_problem = str(exc)[:200]

        fused: dict[int, float] = {}
        for ranking in (keyword, dense):
            for index, rank in ranking.items():
                fused[index] = fused.get(index, 0.0) + 1.0 / (RRF_K + rank)
        best = sorted(fused, key=lambda i: -fused[i])[:max(1, min(int(k or 4), 8))]

        result: dict[str, Any] = {
            "mode": "hybrid" if dense else "keyword",
            "passages": [
                {"id": self.passages[i].id, "source": self.passages[i].source,
                 "section": self.passages[i].section, "text": self.passages[i].text}
                for i in best
            ],
        }
        if not best:
            result["message"] = "Nothing in the documentation matches that question."
        return result


def build_documentation_index(reports_dir: str | Path | None, readme_path: str | Path | None = None,
                              embedder: Embedder | None = None, use_embeddings: bool | None = None) -> DocumentIndex:
    """The index the app and the scripts use: help page, model report notes and README.

    Embeddings are used when a Gemini key is set, unless BROWNSEA_EMBEDDINGS=0.
    """
    from src.help_page import build_help_html  # noqa: PLC0415

    if readme_path is None:
        readme_path = Path(__file__).resolve().parents[2] / "README.md"
    if use_embeddings is None:
        use_embeddings = os.getenv("BROWNSEA_EMBEDDINGS", "1").strip().lower() not in ("0", "false", "no")
    if embedder is None and use_embeddings:
        embedder = gemini_embedder()
    passages = load_documentation(help_html=build_help_html(), reports_dir=reports_dir, readme_path=readme_path)
    return DocumentIndex(passages, embedder=embedder if use_embeddings else None)
