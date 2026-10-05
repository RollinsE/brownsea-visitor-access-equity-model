"""Check the live assistant against answers computed directly from the data.

Needs GEMINI_API_KEY (or ANTHROPIC_API_KEY), so it is a manual check rather than part of `make test`:

    python scripts/eval_agent.py
    python scripts/eval_agent.py --artifacts outputs/releases/latest/artifacts

Each question has an expected value worked out here from the district table,
independently of the assistant. A question passes when that value appears in
the assistant's answer.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agent import AgentUnavailable, ArtifactStore, EquityAgent, LLMError  # noqa: E402
from src.agent.tools import DISTRICT_TABLE  # noqa: E402


def build_cases(rows: list[dict[str, str]]) -> list[tuple[str, str]]:
    def count(predicate) -> str:
        return str(sum(1 for row in rows if predicate(row)))

    def top(column: str, reverse: bool = True) -> str:
        return sorted(rows, key=lambda row: float(row[column]), reverse=reverse)[0]["District"]

    return [
        ("How many districts are in the Urgent Action priority zone?", count(lambda r: r["priority_zone"] == "Urgent Action")),
        ("How many districts are classed as High Need?", count(lambda r: r["need_tier"] == "High Need")),
        ("How many Urgent Action districts have a total journey to Brownsea under 40 minutes?",
         count(lambda r: r["priority_zone"] == "Urgent Action" and float(r["total_journey_min"]) < 40)),
        ("Which district has the highest observed visits per 1,000?", top("visits_per_1000")),
        ("Which district has the longest total journey to Brownsea?", top("total_journey_min")),
        ("Which district is furthest below its expected visit rate?", top("performance_gap")),
        ("How many districts are flagged as needing intervention?", count(lambda r: r["needs_intervention"] == "True")),
        ("How many districts are in the Dorset authority?", count(lambda r: r["Authority_Name"] == "Dorset")),
        ("What is the priority zone for district %s?" % rows[0]["District"], rows[0]["priority_zone"]),
        ("Which model performed best?", ""),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--artifacts", default="docs/artifacts")
    parser.add_argument("--reports", default="docs/reports")
    parser.add_argument("--pause", type=float, default=6.0, help="Seconds to wait between questions (free tiers limit requests per minute)")
    args = parser.parse_args()

    artifacts = Path(args.artifacts)
    with (artifacts / DISTRICT_TABLE).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    store = ArtifactStore.from_paths(artifacts, args.reports, lookup_index={})
    try:
        agent = EquityAgent(store)
    except AgentUnavailable as exc:
        print(f"Agent unavailable: {exc}")
        return 1

    cases = build_cases(rows)
    cases[-1] = (cases[-1][0], str(store.model_performance.get("summary", {}).get("best_model", "")))

    print(f"Using {agent.llm.name} model {agent.llm.model}\n")
    passed = 0
    for number, (question, expected) in enumerate(cases):
        if number:
            time.sleep(args.pause)
        try:
            result = agent.ask(question)
        except LLMError as exc:
            print(f"ERROR {question}\n      {exc}")
            continue
        ok = bool(result.tool_calls) and re.search(rf"(?<![\w.]){re.escape(expected)}(?![\w.]*\d)", result.answer, re.IGNORECASE) is not None
        passed += ok
        print(f"{'PASS' if ok else 'FAIL'}  {question}\n      expected: {expected} | tools: {[c['name'] for c in result.tool_calls]}")
        if not ok:
            print(f"      answer: {result.answer}")
    print(f"\n{passed}/{len(cases)} passed")
    return 0 if passed == len(cases) else 1


if __name__ == "__main__":
    raise SystemExit(main())
