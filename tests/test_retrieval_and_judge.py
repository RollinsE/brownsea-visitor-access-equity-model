from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.agent import ArtifactStore, EquityAgent, LLMError
from src.agent.judge import decide, format_evidence, judge_answer
from src.agent.llm import GeminiLLM, _parse_json
from src.agent.retrieval import (
    BM25, DocumentIndex, Passage, build_documentation_index, gemini_embedder, html_passages, markdown_passages, tokenize,
)
from src.agent.tools import TOOL_SCHEMAS

ROOT = Path(__file__).resolve().parents[1]

HELP = """
<h2>Priority action matrix</h2>
<table><tr><th>Zone</th><th>Meaning</th></tr>
<tr><td>Urgent Action</td><td>High-need areas with fewer than 4 visits per 1,000 residents.</td></tr>
<tr><td>Maintain</td><td>Areas with 7 or more visits per 1,000 residents.</td></tr></table>
<h2>Model terms</h2>
<p>The expected rate comes from a Ridge regression model tested with cross-validation by local authority.</p>
"""

README = """# Project

Intro paragraph about who visits Brownsea.

## How it works

1. **Model.** Seven model types were compared with cross-validation.

```bash
pip install something
```

## Run it yourself

Clone the repository.
"""


# --------------------------------------------------------------- chunking
def test_html_rows_become_their_own_passages():
    passages = html_passages(HELP, "Help page")
    texts = [p.text for p in passages]
    assert "Urgent Action: High-need areas with fewer than 4 visits per 1,000 residents." in texts
    assert any(p.section == "Model terms" and "Ridge regression" in p.text for p in passages)
    assert not any(p.text.startswith("Zone:") for p in passages)  # header row skipped


def test_markdown_keeps_chosen_sections_and_drops_code():
    passages = markdown_passages(README, "README", sections=("", "How it works"))
    joined = " ".join(p.text for p in passages)
    assert "Seven model types" in joined and "Intro paragraph" in joined
    assert "pip install" not in joined and "Clone the repository" not in joined
    assert "**" not in joined


# --------------------------------------------------------------- search
def _index(embedder=None) -> DocumentIndex:
    return DocumentIndex(html_passages(HELP, "Help page"), embedder=embedder)


def test_keyword_search_finds_the_definition():
    result = _index().search("What does Urgent Action mean?", k=2)
    assert result["mode"] == "keyword"
    assert result["passages"][0]["text"].startswith("Urgent Action")
    assert {"id", "source", "section", "text"} <= set(result["passages"][0])


def test_bm25_ignores_stopwords_and_ranks_rare_terms_higher():
    assert tokenize("What is the gap?") == ["gap"]
    bm25 = BM25([["model", "ridge"], ["model", "visits"], ["visits"]])
    scores = bm25.scores(["ridge"])
    assert scores[0] > 0 and scores[1] == 0


def test_hybrid_search_uses_embeddings_when_keywords_miss():
    def embedder(texts, task):
        # "how was it evaluated" shares no keyword with the model passage, but the vectors match it.
        return [[1.0, 0.0] if ("Ridge" in t or "evaluated" in t) else [0.0, 1.0] for t in texts]

    result = _index(embedder).search("how was it evaluated", k=1)
    assert result["mode"] == "hybrid"
    assert "Ridge regression" in result["passages"][0]["text"]


def test_search_falls_back_to_keywords_when_embedding_fails():
    def broken(texts, task):
        raise LLMError("Gemini API error 503: busy", status=503)

    index = _index(broken)
    result = index.search("Urgent Action", k=1)
    assert result["mode"] == "keyword"
    assert result["passages"][0]["text"].startswith("Urgent Action")
    assert "503" in index.embedding_problem


def test_gemini_embedder_batches_and_sends_task_type(monkeypatch):
    calls = []

    def transport(url, headers, body):
        calls.append((url, body))
        return {"embeddings": [{"values": [0.1, 0.2]} for _ in body["requests"]]}

    embed = gemini_embedder(api_key="k", model="emb-model", transport=transport)
    vectors = embed([f"text {i}" for i in range(150)], "RETRIEVAL_DOCUMENT")
    assert len(vectors) == 150 and len(calls) == 2
    assert calls[0][0].endswith("emb-model:batchEmbedContents")
    assert calls[0][1]["requests"][0]["taskType"] == "RETRIEVAL_DOCUMENT"
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    assert gemini_embedder() is None


def test_published_documentation_is_indexed():
    index = build_documentation_index(ROOT / "docs" / "reports", use_embeddings=False)
    sources = {p.source for p in index.passages}
    assert {"Help page", "README"} <= sources
    top = index.search("What does gap to target mean?", k=1)["passages"][0]
    assert "target" in top["text"].lower()


# --------------------------------------------------------------- tool and agent
def test_search_documentation_tool_is_declared_and_wired(tmp_path):
    assert any(tool["name"] == "search_documentation" for tool in TOOL_SCHEMAS)
    store = ArtifactStore(districts=[{"District": "BH1"}], documentation=_index())
    assert store.run_tool("search_documentation", {"query": "Maintain"})["passages"][0]["text"].startswith("Maintain")
    assert "error" in store.run_tool("search_documentation", {"query": " "})
    assert "not available" in ArtifactStore(districts=[{"District": "BH1"}]).search_documentation("x")["message"]


class ScriptedLLM:
    """Plays back tool requests and a final answer, like the provider classes do."""

    name, model = "scripted", "scripted-1"

    def __init__(self, replies):
        self.replies = list(replies)

    def start(self, history, question):
        return [{"role": "user", "content": question}]

    def send(self, system, messages, tools):
        from src.agent.llm import LLMReply
        text, requests = self.replies.pop(0)
        return LLMReply(text=text, tool_requests=requests, model=self.model)

    def add_tool_results(self, messages, reply, results):
        messages.append({"role": "tool", "content": [output for _, output in results]})


def test_agent_result_keeps_tool_outputs_as_evidence_but_not_in_the_api_reply():
    store = ArtifactStore(districts=[{"District": "BH1", "priority_zone": "Urgent Action"}], documentation=_index())
    llm = ScriptedLLM([
        ("", [{"id": "1", "name": "search_documentation", "input": {"query": "Urgent Action"}}]),
        ("Urgent Action covers high-need areas with few visits.", []),
    ])
    result = EquityAgent(store, llm=llm).ask("What does Urgent Action mean?")
    assert result.evidence[0]["name"] == "search_documentation"
    assert result.evidence[0]["output"]["passages"]
    assert "evidence" not in result.to_dict()


# --------------------------------------------------------------- judge
class FakeJudge:
    model = "judge-1"

    def __init__(self, reply):
        self.reply = reply
        self.prompts = []

    def complete_json(self, system, prompt, schema):
        self.prompts.append(prompt)
        return self.reply


def test_judge_passes_supported_answers_and_fails_unsupported_claims():
    good = {"claims": [{"claim": "9 districts are Urgent Action", "supported": True}],
            "answers_question": True, "declines_or_cannot_answer": False, "reason": "ok"}
    bad = {**good, "claims": [*good["claims"], {"claim": "the ferry is too expensive", "supported": False}]}
    assert decide("answer", good).passed
    verdict = decide("answer", bad)
    assert not verdict.passed and not verdict.faithful and verdict.unsupported == ["the ferry is too expensive"]


def test_judge_rules_for_off_topic_and_unanswerable_questions():
    declined = {"claims": [], "answers_question": False, "declines_or_cannot_answer": True, "reason": ""}
    guessed = {"claims": [{"claim": "2,400 members live in BH1", "supported": False}],
               "answers_question": True, "declines_or_cannot_answer": False, "reason": ""}
    assert decide("decline", declined).passed
    assert decide("cannot_answer", declined).passed
    assert not decide("cannot_answer", guessed).passed
    assert not decide("answer", declined).passed
    with pytest.raises(ValueError):
        decide("other", declined)


def test_judge_prompt_contains_question_answer_and_evidence():
    judge = FakeJudge({"claims": [], "answers_question": True, "declines_or_cannot_answer": False, "reason": ""})
    evidence = [{"name": "get_district", "input": {"district": "BH1"}, "output": {"district": {"priority_zone": "Urgent Action"}}}]
    judge_answer(judge, "Which zone is BH1 in?", "Urgent Action.", evidence)
    prompt = judge.prompts[0]
    assert "Which zone is BH1 in?" in prompt and "Urgent Action." in prompt and "get_district" in prompt


def test_evidence_is_trimmed_for_the_judge():
    big = [{"name": "query_districts", "input": {}, "output": {"rows": ["x" * 10000]}}] * 5
    text = format_evidence(big)
    assert len(text) < 20000 and "trimmed" in text
    assert "no tools" in format_evidence([])


def test_gemini_complete_json_requests_structured_output():
    sent = {}

    def transport(url, headers, body):
        sent.update(body)
        return {"candidates": [{"content": {"parts": [{"text": '{"answers_question": true}'}]}}]}

    llm = GeminiLLM(api_key="k", model="judge-model", fallback_models=[], transport=transport)
    assert llm.complete_json("system", "prompt", {"type": "object"}) == {"answers_question": True}
    assert sent["generationConfig"]["responseMimeType"] == "application/json"
    assert "tools" not in sent


def test_json_replies_are_read_with_or_without_a_fence():
    assert _parse_json('```json\n{"a": 1}\n```') == {"a": 1}
    with pytest.raises(LLMError):
        _parse_json("not json")


def test_judge_check_set_is_balanced_and_complete():
    items = [json.loads(line) for line in (ROOT / "evals" / "judge_check.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(items) >= 12
    assert {i["kind"] for i in items} == {"answer", "decline", "cannot_answer"}
    assert 0.3 < sum(i["expected_pass"] for i in items) / len(items) < 0.7
    assert all({"id", "question", "answer", "evidence", "expected_pass"} <= set(i) for i in items)


def test_a_slow_reply_becomes_a_retryable_error(monkeypatch):
    import src.agent.llm as llm_module

    def slow(*args, **kwargs):
        raise TimeoutError("The read operation timed out")

    monkeypatch.setattr(llm_module.urllib.request, "urlopen", slow)
    with pytest.raises(LLMError) as caught:
        llm_module._http_post("https://example.invalid", {}, {}, timeout=5)
    assert caught.value.status == 504


def test_judge_moves_to_the_next_model_after_timeouts():
    calls = []

    def transport(url, headers, body):
        calls.append(url)
        if "slow-model" in url:
            raise LLMError("Gemini took longer than 120 s to reply.", status=504)
        return {"candidates": [{"content": {"parts": [{"text": '{"answers_question": true}'}]}}]}

    llm = GeminiLLM(api_key="k", model="slow-model", fallback_models=["fast-model"], transport=transport, sleep=lambda s: None)
    assert llm.complete_json("s", "p", {"type": "object"}) == {"answers_question": True}
    assert "fast-model" in calls[-1] and llm.last_model == "fast-model"


def test_judge_retries_the_same_answer_on_the_next_model_when_one_is_busy():
    def transport(url, headers, body):
        if "busy-model" in url:
            raise LLMError("Gemini API error 503: high demand", status=503)
        return {"candidates": [{"content": {"parts": [{"text": json.dumps(
            {"claims": [], "answers_question": True, "declines_or_cannot_answer": False, "reason": ""})}]}}]}

    llm = GeminiLLM(api_key="k", model="busy-model", fallback_models=["spare-model"], transport=transport, sleep=lambda s: None)
    verdict = judge_answer(llm, "q", "a", [])
    assert verdict.passed and verdict.judge_model == "spare-model"
