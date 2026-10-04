from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.agent import AgentUnavailable, ArtifactStore, EquityAgent
from src.agent.agent import clean_history

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
    client = FakeClient([
        _tool_use("query_districts", {"filters": [{"column": "priority_zone", "op": "eq", "value": "Urgent Action"}]}),
        _text("Two districts need urgent action: BH1 and BH2."),
    ])
    result = EquityAgent(store, client=client, model="test-model").ask("Which districts need urgent action?")

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
    client = FakeClient([_tool_use("get_district", {"district": "ZZ9"}), _text("I could not find that district.")])
    EquityAgent(store, client=client).ask("Tell me about ZZ9")
    tool_result = client.calls[1]["messages"][-1]["content"][0]
    assert tool_result["is_error"] is True


def test_agent_stops_at_step_limit(tmp_path):
    store = _store(tmp_path)
    client = FakeClient([_tool_use("get_model_performance", {}, f"call_{i}") for i in range(3)])
    result = EquityAgent(store, client=client, max_steps=3).ask("loop forever")
    assert result.complete is False
    assert len(result.tool_calls) == 3


def test_agent_validates_question_and_needs_data(tmp_path):
    store = _store(tmp_path)
    agent = EquityAgent(store, client=FakeClient([]))
    with pytest.raises(ValueError):
        agent.ask("   ")
    with pytest.raises(ValueError):
        agent.ask("x" * 1001)
    with pytest.raises(AgentUnavailable):
        EquityAgent(ArtifactStore(), client=FakeClient([]))


def test_agent_unavailable_without_api_key(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(AgentUnavailable):
        EquityAgent(_store(tmp_path))


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
def _app(tmp_path, monkeypatch, client=None):
    pytest.importorskip("flask")
    from app.server import create_app

    artifacts, _ = _write_release(tmp_path)
    return create_app(lookup_path=str(artifacts / "postcode_lookup.json"), agent_client=client)


def test_api_ask_returns_answer_and_tool_calls(tmp_path, monkeypatch):
    client = FakeClient([_tool_use("get_district", {"district": "BH1"}), _text("BH1 is an Urgent Action district.")])
    app = _app(tmp_path, monkeypatch, client)
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


def test_api_ask_is_off_without_key_and_app_still_works(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    app = _app(tmp_path, monkeypatch)
    http = app.test_client()

    assert http.post("/api/ask", json={"question": "hello"}).status_code == 503
    assert "Ask about the analysis" not in http.get("/").get_data(as_text=True)
    assert http.get("/api/lookup?postcode=DT11AB").status_code == 200
    assert http.get("/health").get_json()["assistant"] is False


def test_api_ask_rate_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("BROWNSEA_AGENT_RATE_PER_MIN", "1")
    client = FakeClient([_text("one"), _text("two")])
    http = _app(tmp_path, monkeypatch, client).test_client()
    assert http.post("/api/ask", json={"question": "a"}).status_code == 200
    assert http.post("/api/ask", json={"question": "b"}).status_code == 429
