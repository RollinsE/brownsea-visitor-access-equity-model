from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.agent import AgentUnavailable, AnthropicLLM, ArtifactStore, EquityAgent, GeminiLLM, LLMError, default_llm
from src.agent.agent import clean_history
from src.agent.llm import gemini_tools
from src.agent.tools import TOOL_SCHEMAS

KEY_VARS = ("GEMINI_API_KEY", "GOOGLE_API_KEY", "ANTHROPIC_API_KEY", "BROWNSEA_AGENT_PROVIDER", "BROWNSEA_AGENT_MODEL")

DISTRICTS = [
    # District, priority_zone, need_tier, visits_per_1000, predicted_visit_rate, total_journey_min, Population, needs_intervention
    ("BH1", "Urgent Action", "High Need", 0.9, 2.7, 35.9, 50000, True),
    ("BH2", "Urgent Action", "High Need", 1.2, 3.1, 34.6, 20000, True),
    ("DT1", "Monitor", "Medium Need", 5.0, 5.2, 62.0, 30000, False),
    ("SP1", "Maintain", "Low Need", 8.4, 7.9, 95.5, 40000, False),
]


def _write_release(base: Path) -> tuple[Path, Path]:
    artifacts, reports = base / "artifacts", base / "reports"
    artifacts.mkdir(parents=True)
    (reports / "tables").mkdir(parents=True)

    with (artifacts / "three_way_intersection_analysis_v2.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "District", "priority_zone", "need_tier", "visits_per_1000", "predicted_visit_rate",
            "total_journey_min", "Population", "needs_intervention", "competitor_context", "shap_narrative", "shap_error",
        ])
        for row in DISTRICTS:
            writer.writerow([*row, " [Kingston Lacy: 25Km; Brownsea Island: 13Km]", "Status: Below Target", ""])

    (artifacts / "postcode_lookup.json").write_text(json.dumps([
        {"postcode": "BH1 1AA", "postcode_clean": "BH11AA", "district": "BH1", "nearest_nt_site_name": "Brownsea Island", "nearest_nt_site_drive_min": 3.0},
        {"postcode": "DT1 1AB", "postcode_clean": "DT11AB", "district": "DT1", "nearest_nt_site_name": "Hardy's Cottage", "nearest_nt_site_drive_min": 9.5},
    ]), encoding="utf-8")
    (artifacts / "model_performance_summary.json").write_text(json.dumps({"best_model": "HybridEnsemble", "best_mae": 0.12}), encoding="utf-8")
    (artifacts / "model_performance.csv").write_text("Model,Mean MAE\nHybridEnsemble,0.12\n", encoding="utf-8")
    (reports / "tables" / "need_tier_definitions.html").write_text(
        "<table><thead><tr><th>Need Tier</th><th>Definition</th></tr></thead>"
        "<tbody><tr><td>High Need</td><td>Composite Need Score ≥ 26</td></tr></tbody></table>",
        encoding="utf-8",
    )
    return artifacts, reports


def _store(tmp_path: Path) -> ArtifactStore:
    return ArtifactStore.from_paths(*_write_release(tmp_path))


class FakeClient:
    """Stands in for anthropic.Anthropic(); replays scripted responses."""

    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self.calls: list[dict] = []
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        self.calls.append(json.loads(json.dumps(kwargs, default=str)))
        return self._responses.pop(0)


def _tool_use(name: str, arguments: dict, call_id: str = "call_1"):
    return SimpleNamespace(content=[SimpleNamespace(type="tool_use", id=call_id, name=name, input=arguments)], stop_reason="tool_use")


def _text(text: str):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)], stop_reason="end_turn")


def _claude(responses: list) -> AnthropicLLM:
    return AnthropicLLM(client=FakeClient(responses), model="test-model")


class FakeGemini:
    """Stands in for the Gemini HTTPS endpoint; replays scripted JSON responses."""

    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self.calls: list[dict] = []

    def __call__(self, url: str, headers: dict, body: dict) -> dict:
        self.calls.append({"url": url, "headers": headers, "body": json.loads(json.dumps(body))})
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _gemini_call(name: str, arguments: dict, **extra) -> dict:
    return {"candidates": [{"content": {"role": "model", "parts": [
        {"functionCall": {"name": name, "args": arguments, **extra}, "thoughtSignature": "sig-abc"},
    ]}}]}


def _gemini_text(text: str) -> dict:
    return {"candidates": [{"content": {"role": "model", "parts": [{"text": "hidden", "thought": True}, {"text": text}]}}]}


def _no_keys(monkeypatch) -> None:
    for name in KEY_VARS:
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------------- tools
def test_query_districts_filters_sorts_and_counts(tmp_path):
    store = _store(tmp_path)
    result = store.query_districts(
        filters=[{"column": "priority_zone", "op": "eq", "value": "urgent action"}, {"column": "total_journey_min", "op": "lt", "value": 40}],
        sort_by="visits_per_1000",
        descending=False,
        limit=1,
    )
    assert result["matched"] == 2
    assert result["returned"] == 1
    assert result["rows"][0]["District"] == "BH1"
    assert result["rows"][0]["total_journey_min"] == 35.9


def test_query_districts_supports_boolean_in_and_contains(tmp_path):
    store = _store(tmp_path)
    assert store.query_districts(filters=[{"column": "needs_intervention", "op": "eq", "value": True}])["matched"] == 2
    assert store.query_districts(filters=[{"column": "District", "op": "in", "value": ["DT1", "SP1"]}])["matched"] == 2
    assert store.query_districts(filters=[{"column": "District", "op": "in", "value": "DT1 | SP1"}])["matched"] == 2
    assert store.query_districts(filters=[{"column": "needs_intervention", "op": "eq", "value": "true"}])["matched"] == 2
    assert store.query_districts(filters=[{"column": "total_journey_min", "op": "lt", "value": "40"}])["matched"] == 2
    assert store.query_districts(filters=[{"column": "need_tier", "op": "contains", "value": "need"}])["matched"] == 4


def test_aggregate_districts_groups_and_computes(tmp_path):
    store = _store(tmp_path)
    result = store.aggregate_districts(
        metrics=[{"column": "Population", "agg": "sum"}, {"column": "visits_per_1000", "agg": "mean"}],
        group_by="priority_zone",
    )
    urgent = next(group for group in result["groups"] if group["priority_zone"] == "Urgent Action")
    assert urgent == {"priority_zone": "Urgent Action", "districts": 2, "sum_Population": 70000.0, "mean_visits_per_1000": 1.05}


def test_bad_tool_input_returns_error_for_the_model_to_fix(tmp_path):
    store = _store(tmp_path)
    assert "Unknown column" in store.run_tool("query_districts", {"filters": [{"column": "nope", "op": "eq", "value": 1}]})["error"]
    assert "text column" in store.run_tool("aggregate_districts", {"metrics": [{"column": "need_tier", "agg": "mean"}]})["error"]
    assert "Unknown district" in store.run_tool("get_district", {"district": "ZZ9"})["error"]
    assert "Unknown tool" in store.run_tool("delete_everything", {})["error"]
    assert "Bad arguments" in store.run_tool("get_district", {"wrong": 1})["error"]


def test_store_hides_brownsea_as_competitor_and_internal_columns(tmp_path):
    store = _store(tmp_path)
    district = store.get_district("bh1")["district"]
    assert "Brownsea" not in district["competitor_context"]
    assert "Kingston Lacy" in district["competitor_context"]
    assert "shap_error" not in district

    postcode = store.lookup_postcode("bh1 1aa")
    assert postcode["match_type"] == "exact"
    assert postcode["result"]["nearest_nt_site_name"] == "No competing NT site identified"
    assert store.lookup_postcode("BH13 7EE")["match_type"] == "destination"
    assert store.lookup_postcode("ZZ1")["match_type"] == "none"


def test_definitions_and_model_performance(tmp_path):
    store = _store(tmp_path)
    assert store.get_definitions("need_tiers")["need_tiers"][0] == {"Need Tier": "High Need", "Definition": "Composite Need Score ≥ 26"}
    assert store.get_model_performance()["summary"]["best_model"] == "HybridEnsemble"


def test_tools_match_published_district_table():
    """The tools must agree with an independent read of the real published table."""
    artifacts = Path("docs") / "artifacts"
    table = artifacts / "three_way_intersection_analysis_v2.csv"
    if not table.exists():
        pytest.skip("published artifacts not present")
    with table.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    expected = sum(1 for row in rows if row["priority_zone"] == "Urgent Action" and float(row["total_journey_min"]) < 40)
    expected_population = sum(float(row["Population"]) for row in rows if row["need_tier"] == "High Need")

    store = ArtifactStore.from_paths(artifacts, Path("docs") / "reports", lookup_index={})
    assert len(store.districts) == len(rows)
    assert store.query_districts(filters=[
        {"column": "priority_zone", "op": "eq", "value": "Urgent Action"},
        {"column": "total_journey_min", "op": "lt", "value": 40},
    ])["matched"] == expected
    total = store.aggregate_districts(
        metrics=[{"column": "Population", "agg": "sum"}],
        filters=[{"column": "need_tier", "op": "eq", "value": "High Need"}],
    )["groups"][0]["sum_Population"]
    assert total == expected_population
    assert set(store.get_definitions()) == {"priority_zones", "intervention_types", "need_tiers"}


# ----------------------------------------------------------------- agent loop
def test_agent_runs_tool_then_answers(tmp_path):
    store = _store(tmp_path)
    llm = _claude([
        _tool_use("query_districts", {"filters": [{"column": "priority_zone", "op": "eq", "value": "Urgent Action"}]}),
        _text("Two districts need urgent action: BH1 and BH2."),
    ])
    client = llm.client
    result = EquityAgent(store, llm=llm).ask("Which districts need urgent action?")

    assert result.answer == "Two districts need urgent action: BH1 and BH2."
    assert result.complete and result.steps == 2
    assert result.tool_calls == [{"name": "query_districts", "input": {"filters": [{"column": "priority_zone", "op": "eq", "value": "Urgent Action"}]}}]

    second_call = client.calls[1]
    assert second_call["model"] == "test-model"
    tool_result = second_call["messages"][-1]["content"][0]
    assert tool_result["type"] == "tool_result" and tool_result["tool_use_id"] == "call_1"
    assert json.loads(tool_result["content"])["matched"] == 2
    assert "priority_zone" in second_call["system"]


def test_agent_reports_tool_errors_back_to_the_model(tmp_path):
    store = _store(tmp_path)
    llm = _claude([_tool_use("get_district", {"district": "ZZ9"}), _text("I could not find that district.")])
    client = llm.client
    EquityAgent(store, llm=llm).ask("Tell me about ZZ9")
    tool_result = client.calls[1]["messages"][-1]["content"][0]
    assert tool_result["is_error"] is True


def test_agent_stops_at_step_limit(tmp_path):
    store = _store(tmp_path)
    llm = _claude([_tool_use("get_model_performance", {}, f"call_{i}") for i in range(3)])
    result = EquityAgent(store, llm=llm, max_steps=3).ask("loop forever")
    assert result.complete is False
    assert len(result.tool_calls) == 3


def test_agent_validates_question_and_needs_data(tmp_path):
    store = _store(tmp_path)
    agent = EquityAgent(store, llm=_claude([]))
    with pytest.raises(ValueError):
        agent.ask("   ")
    with pytest.raises(ValueError):
        agent.ask("x" * 1001)
    with pytest.raises(AgentUnavailable):
        EquityAgent(ArtifactStore(), llm=_claude([]))


def test_agent_unavailable_without_api_key(tmp_path, monkeypatch):
    _no_keys(monkeypatch)
    with pytest.raises(AgentUnavailable):
        EquityAgent(_store(tmp_path))


def test_provider_is_chosen_from_environment(monkeypatch):
    _no_keys(monkeypatch)
    monkeypatch.setenv("GEMINI_API_KEY", "g-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "a-key")
    llm = default_llm()
    assert isinstance(llm, GeminiLLM) and llm.model == "gemini-3.8-flash"
    assert llm.models[1:] == ["gemini-3.7-flash", "gemini-3.6-flash", "gemini-3.5-flash", "gemini-3.5-flash-lite"]

    monkeypatch.setenv("BROWNSEA_AGENT_FALLBACK_MODELS", "")
    assert default_llm().models == ["gemini-3.8-flash"]
    monkeypatch.delenv("BROWNSEA_AGENT_FALLBACK_MODELS")

    monkeypatch.setenv("BROWNSEA_AGENT_MODEL", "gemini-3.5-flash-lite")
    assert default_llm().model == "gemini-3.5-flash-lite"

    monkeypatch.setenv("BROWNSEA_AGENT_PROVIDER", "nonsense")
    with pytest.raises(AgentUnavailable):
        default_llm()


# --------------------------------------------------------------------- Gemini
def test_gemini_agent_runs_tool_then_answers(tmp_path):
    store = _store(tmp_path)
    transport = FakeGemini([
        _gemini_call("query_districts", {"filters": [{"column": "priority_zone", "op": "eq", "value": "Urgent Action"}]}, id="fc_1"),
        _gemini_text("Two districts need urgent action: BH1 and BH2."),
    ])
    llm = GeminiLLM(api_key="test-key", model="gemini-test", transport=transport)
    result = EquityAgent(store, llm=llm).ask("Which districts need urgent action?", history=[
        {"role": "user", "content": "hello"}, {"role": "assistant", "content": "hi"},
    ])

    assert result.answer == "Two districts need urgent action: BH1 and BH2."
    assert result.tool_calls == [{"name": "query_districts", "input": {"filters": [{"column": "priority_zone", "op": "eq", "value": "Urgent Action"}]}}]

    first, second = transport.calls
    assert first["url"].endswith("/models/gemini-test:generateContent")
    assert first["headers"]["x-goog-api-key"] == "test-key"
    assert "test-key" not in first["url"]
    assert "priority_zone" in first["body"]["systemInstruction"]["parts"][0]["text"]
    assert [m["role"] for m in first["body"]["contents"]] == ["user", "model", "user"]

    replayed, tool_turn = second["body"]["contents"][-2:]
    assert replayed["role"] == "model"
    assert replayed["parts"][0]["thoughtSignature"] == "sig-abc"  # must be sent back untouched
    response = tool_turn["parts"][0]["functionResponse"]
    assert tool_turn["role"] == "user" and response["name"] == "query_districts" and response["id"] == "fc_1"
    assert response["response"]["matched"] == 2


def test_gemini_tool_declarations_are_valid_for_the_api():
    declarations = gemini_tools(TOOL_SCHEMAS)[0]["functionDeclarations"]
    assert {d["name"] for d in declarations} == {t["name"] for t in TOOL_SCHEMAS}
    by_name = {d["name"]: d for d in declarations}
    assert "parameters" not in by_name["get_model_performance"]  # no empty object schemas

    def check(schema):
        assert "type" in schema, schema  # Gemini needs a type on every schema node
        for child in (schema.get("properties") or {}).values():
            check(child)
        if "items" in schema:
            check(schema["items"])

    for declaration in declarations:
        if "parameters" in declaration:
            check(declaration["parameters"])


DAILY_QUOTA = LLMError("Quota exceeded, limit: 20. Please retry in 17h14m4.7s.", status=429)
BUSY = LLMError("This model is currently experiencing high demand.", status=503)


def _gemini(responses: list, **kwargs):
    waits: list[float] = []
    transport = FakeGemini(responses)
    kwargs.setdefault("fallback_models", ["model-b"])
    llm = GeminiLLM(api_key="k", model="model-a", transport=transport, sleep=waits.append, **kwargs)
    return llm, transport, waits


def _models_called(transport: FakeGemini) -> list[str]:
    return [call["url"].split("/models/")[1].split(":")[0] for call in transport.calls]


def test_gemini_waits_briefly_for_a_per_minute_limit(tmp_path):
    llm, transport, waits = _gemini([LLMError("Please retry in 12.5s.", status=429), _gemini_text("ok")])
    result = EquityAgent(_store(tmp_path), llm=llm).ask("hello")
    assert result.answer == "ok" and result.model == "model-a"
    assert waits == [12.5] and _models_called(transport) == ["model-a", "model-a"]


def test_gemini_switches_model_on_daily_quota_without_waiting(tmp_path):
    llm, transport, waits = _gemini([DAILY_QUOTA, _gemini_text("first"), _gemini_text("second")])
    agent = EquityAgent(_store(tmp_path), llm=llm)

    first = agent.ask("hello")
    assert first.answer == "first" and first.model == "model-b"
    assert waits == []  # a 17-hour wait is never slept through

    assert agent.ask("again").model == "model-b"  # the exhausted model is not tried again
    assert _models_called(transport) == ["model-a", "model-b", "model-b"]


def test_gemini_restarts_question_on_fallback_when_model_fails_mid_answer(tmp_path):
    llm, transport, waits = _gemini([
        _gemini_call("get_district", {"district": "BH1"}),
        BUSY, BUSY,  # model-a fails after its tool call, even after one quick retry
        _gemini_call("get_district", {"district": "BH1"}),
        _gemini_text("BH1 is Urgent Action."),
    ])
    result = EquityAgent(_store(tmp_path), llm=llm).ask("Tell me about BH1")

    assert result.answer == "BH1 is Urgent Action." and result.model == "model-b"
    assert waits == [5.0]
    assert _models_called(transport) == ["model-a", "model-a", "model-a", "model-b", "model-b"]
    restarted = transport.calls[3]["body"]["contents"]
    assert len(restarted) == 1 and restarted[0]["role"] == "user"  # fresh conversation, no mixed signatures


def test_gemini_reports_clearly_when_every_model_is_exhausted(tmp_path):
    llm, transport, waits = _gemini([DAILY_QUOTA, DAILY_QUOTA])
    agent = EquityAgent(_store(tmp_path), llm=llm)
    with pytest.raises(LLMError):
        agent.ask("hello")
    assert not llm.has_available_model()
    with pytest.raises(LLMError) as raised:
        agent.ask("again")
    assert raised.value.status == 429 and "over its limit" in str(raised.value)
    assert len(transport.calls) == 2  # no further requests are sent


def test_gemini_skips_unknown_model_names_and_reports_other_errors(tmp_path):
    store = _store(tmp_path)
    llm, transport, _ = _gemini([LLMError("model not found", status=404), _gemini_text("ok")])
    assert EquityAgent(store, llm=llm).ask("hello").model == "model-b"

    failing, transport, _ = _gemini([LLMError("API key not valid", status=400)])
    with pytest.raises(LLMError):
        EquityAgent(store, llm=failing).ask("hello")
    assert len(transport.calls) == 1


def test_gemini_empty_reply_gives_a_clear_message(tmp_path):
    store = _store(tmp_path)
    transport = FakeGemini([{"candidates": [{"finishReason": "MAX_TOKENS", "content": {"role": "model"}}]}])
    result = EquityAgent(store, llm=GeminiLLM(api_key="k", transport=transport)).ask("hello")
    assert result.complete is False and "rephrasing" in result.answer


def test_clean_history_keeps_alternating_recent_text_turns():
    history = [
        {"role": "assistant", "content": "orphan"},
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "answer"},
        {"role": "system", "content": "ignore me"},
        {"role": "user", "content": {"not": "text"}},
        {"role": "user", "content": "dangling"},
    ]
    assert clean_history(history) == [{"role": "user", "content": "first"}, {"role": "assistant", "content": "answer"}]
    assert clean_history("nonsense") == []


# ------------------------------------------------------------------ Flask API
def _app(tmp_path, monkeypatch, llm=None):
    pytest.importorskip("flask")
    from app.server import create_app

    artifacts, _ = _write_release(tmp_path)
    return create_app(lookup_path=str(artifacts / "postcode_lookup.json"), agent_llm=llm)


def test_api_ask_returns_answer_and_tool_calls(tmp_path, monkeypatch):
    llm = _claude([_tool_use("get_district", {"district": "BH1"}), _text("BH1 is an Urgent Action district.")])
    client = llm.client
    app = _app(tmp_path, monkeypatch, llm)
    http = app.test_client()

    response = http.post("/api/ask", json={"question": "Tell me about BH1"})
    assert response.status_code == 200
    body = response.get_json()
    assert body["answer"] == "BH1 is an Urgent Action district."
    assert body["tool_calls"][0]["name"] == "get_district"
    assert "Engagement status: Below expected" in client.calls[1]["messages"][-1]["content"][0]["content"]

    assert http.post("/api/ask", json={}).status_code == 400
    assert "Ask about the analysis" in http.get("/").get_data(as_text=True)
    assert http.get("/health").get_json()["assistant"] is True


def test_api_ask_works_with_gemini_and_reports_usage_limit(tmp_path, monkeypatch):
    transport = FakeGemini([
        _gemini_call("get_district", {"district": "BH1"}),
        _gemini_text("BH1 is an Urgent Action district."),
        LLMError("Quota exceeded. Please retry in 17h1m2s.", status=429),
    ])
    llm = GeminiLLM(api_key="k", transport=transport, fallback_models=[])
    http = _app(tmp_path, monkeypatch, llm).test_client()

    body = http.post("/api/ask", json={"question": "Tell me about BH1"}).get_json()
    assert body["answer"] == "BH1 is an Urgent Action district."
    assert "id" not in transport.calls[1]["body"]["contents"][-1]["parts"][0]["functionResponse"]
    assert http.get("/health").get_json()["assistant_provider"] == "gemini"

    limited = http.post("/api/ask", json={"question": "again"})
    assert limited.status_code == 503 and "usage limit" in limited.get_json()["error"]


def test_api_ask_is_off_without_key_and_app_still_works(tmp_path, monkeypatch):
    _no_keys(monkeypatch)
    app = _app(tmp_path, monkeypatch)
    http = app.test_client()

    assert http.post("/api/ask", json={"question": "hello"}).status_code == 503
    assert "Ask about the analysis" not in http.get("/").get_data(as_text=True)
    assert http.get("/api/lookup?postcode=DT11AB").status_code == 200
    assert http.get("/health").get_json()["assistant"] is False


def test_api_ask_rate_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("BROWNSEA_AGENT_RATE_PER_MIN", "1")
    http = _app(tmp_path, monkeypatch, _claude([_text("one"), _text("two")])).test_client()
    assert http.post("/api/ask", json={"question": "a"}).status_code == 200
    assert http.post("/api/ask", json={"question": "b"}).status_code == 429


def test_app_falls_back_to_published_docs_when_no_outputs(tmp_path, monkeypatch):
    pytest.importorskip("flask")
    import app.server as server

    if not (server.PUBLISHED_DOCS_DIR / "artifacts" / "postcode_lookup.csv").exists():
        pytest.skip("published artifacts not present")
    llm = _claude([_text("Nine districts are Urgent Action.")])
    http = server.create_app(outputs_root=str(tmp_path / "no-outputs-here"), agent_llm=llm).test_client()

    health = http.get("/health").get_json()
    assert health["records"] > 40000 and health["assistant"] is True

    row = http.get("/api/lookup?postcode=bh2 5np").get_json()["result"]
    assert row["district"] == "BH2" and row["chain_ferry_used"] is False
    assert isinstance(row["total_brownsea_journey_min"], float)

    assert http.get("/reports/index.html").status_code == 200
    assert http.get("/artifacts/postcode_lookup.csv").status_code == 200
    assert http.post("/api/ask", json={"question": "How many urgent?"}).get_json()["answer"].startswith("Nine")
