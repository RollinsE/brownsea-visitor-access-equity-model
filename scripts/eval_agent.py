"""Evaluate the assistant: exact checks against the data, plus an LLM judge.

Needs GEMINI_API_KEY (or ANTHROPIC_API_KEY), so it is a manual check rather than part of `pytest`.

    python scripts/eval_agent.py                       # ask every question, check and judge the answers
    python scripts/eval_agent.py --no-judge            # exact checks only (fewer API calls)
    python scripts/eval_agent.py --rejudge outputs/evals/run_<time>.jsonl
    python scripts/eval_agent.py --check-judge         # how often the judge agrees with hand labels

Two kinds of check are used:

- Exact checks. For questions with one right answer (a count, a district),
  the expected value is worked out here from the district table, without the
  assistant, and must appear in the answer.
- LLM judge. A second model lists the claims in each answer and checks every
  one against the tool results the assistant had (see src/agent/judge.py). It
  also checks that off-topic questions are declined and that questions the data
  cannot answer are not guessed at.

Every run is saved as JSON lines under outputs/evals/, with the tool results,
so the answers can be judged again later without asking the assistant again.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agent import AgentUnavailable, ArtifactStore, EquityAgent, LLMError, default_llm  # noqa: E402
from src.agent.judge import judge_answer  # noqa: E402
from src.agent.retrieval import build_documentation_index  # noqa: E402
from src.agent.tools import DISTRICT_TABLE  # noqa: E402


def build_cases(rows: list[dict[str, str]], best_model: str) -> list[dict]:
    """Questions with their kind and, where there is one right answer, the expected value."""

    def count(predicate) -> str:
        return str(sum(1 for row in rows if predicate(row)))

    def top(column: str) -> str:
        return max(rows, key=lambda row: float(row[column]))["District"]

    def case(question: str, expected: str | None = None, kind: str = "answer", group: str = "data") -> dict:
        return {"question": question, "expected": expected, "kind": kind, "group": group}

    first = rows[0]
    return [
        case("How many districts are in the Urgent Action priority zone?", count(lambda r: r["priority_zone"] == "Urgent Action")),
        case("How many districts are classed as High Need?", count(lambda r: r["need_tier"] == "High Need")),
        case("How many Urgent Action districts have a total journey to Brownsea under 40 minutes?",
             count(lambda r: r["priority_zone"] == "Urgent Action" and float(r["total_journey_min"]) < 40)),
        case("Which district has the highest observed visits per 1,000?", top("visits_per_1000")),
        case("Which district has the longest total journey to Brownsea?", top("total_journey_min")),
        case("Which district is furthest below its expected visit rate?", top("performance_gap")),
        case("How many districts are flagged as needing intervention?", count(lambda r: r["needs_intervention"] == "True")),
        case(f"What is the priority zone for district {first['District']}?", first["priority_zone"]),
        case("Which model performed best?", best_model),
        case(f"Tell me about {first['District']}."),
        case("What does Urgent Action mean?", group="documentation"),
        case("What does 'within the normal range for the model' mean?", group="documentation"),
        case("How was the model trained and tested?", group="documentation"),
        case("What does 'gap to target' mean?", group="documentation"),
        case("How many National Trust members live in BH1?", kind="cannot_answer", group="scope"),
        case("What was the visit rate for BH1 in 2019?", kind="cannot_answer", group="scope"),
        case("What will the weather be like in Poole tomorrow?", kind="decline", group="scope"),
        case("Write me a short poem about the sea.", kind="decline", group="scope"),
    ]


def exact_match(expected: str, answer: str) -> bool:
    return re.search(rf"(?<![\w.]){re.escape(expected)}(?![\w.]*\d)", answer, re.IGNORECASE) is not None


def judge_llm():
    """The judge model: BROWNSEA_JUDGE_MODEL if set, otherwise the same provider's default."""
    return default_llm(os.getenv("BROWNSEA_JUDGE_MODEL") or None)


def add_judgement(record: dict, judge, pause: float) -> None:
    try:
        verdict = judge_answer(judge, record["question"], record["answer"], record["evidence"], record["kind"])
        record["judge"] = verdict.to_dict()
    except LLMError as exc:
        record["judge"] = {"error": str(exc)}
    time.sleep(pause)


def print_record(record: dict) -> None:
    exact = record.get("exact")
    verdict = record.get("judge") or {}
    marks = []
    if exact is not None:
        marks.append(f"exact {'PASS' if exact else 'FAIL'} (expected {record['expected']})")
    if "passed" in verdict:
        marks.append(f"judge {'PASS' if verdict['passed'] else 'FAIL'} ({verdict.get('judge_model', '')})")
    elif "error" in verdict:
        marks.append("judge ERROR")
    print(f"- {record['question']}\n    {' | '.join(marks) or 'no checks'} | tools: {[e['name'] for e in record['evidence']]}")
    if verdict.get("unsupported"):
        print(f"    unsupported: {verdict['unsupported']}")
    if exact is False or verdict.get("passed") is False:
        print(f"    answer: {record['answer'][:400]}")


def summarise(records: list[dict]) -> None:
    def rate(values: list[bool]) -> str:
        return f"{sum(values)}/{len(values)}" if values else "-"

    print("\nSummary")
    exact = [r["exact"] for r in records if r.get("exact") is not None]
    print(f"  exact checks passed:        {rate(exact)}")
    judged = [r for r in records if "passed" in (r.get("judge") or {})]
    print(f"  judge passed:               {rate([r['judge']['passed'] for r in judged])}")
    print(f"  answers with no unsupported claims: {rate([r['judge']['faithful'] for r in judged])}")
    for group in dict.fromkeys(r["group"] for r in records):
        members = [r for r in judged if r["group"] == group]
        print(f"    {group:<14} judge passed {rate([r['judge']['passed'] for r in members])}")
    disagree = [r for r in judged if r.get("exact") is not None and r["exact"] != r["judge"]["passed"]]
    if disagree:
        print(f"  exact check and judge disagree on {len(disagree)} question(s): worth reading those answers yourself.")


def run(args) -> int:
    artifacts = Path(args.artifacts)
    with (artifacts / DISTRICT_TABLE).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    store = ArtifactStore.from_paths(artifacts, args.reports, lookup_index={},
                                     documentation=build_documentation_index(args.reports))
    try:
        agent = EquityAgent(store)
        judge = None if args.no_judge else judge_llm()
    except AgentUnavailable as exc:
        print(f"Agent unavailable: {exc}")
        return 1

    best = str(store.model_performance.get("summary", {}).get("best_model", ""))
    cases = build_cases(rows, best)[: args.limit or None]
    out = Path(args.out_dir) / f"run_{datetime.now():%Y%m%d_%H%M%S}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    print(f"Assistant: {agent.llm.name}, starting with {agent.llm.model}. "
          f"Judge: {'off' if judge is None else 'starting with ' + judge.model}")
    print(f"Saving to {out}\n")

    records = []
    for number, item in enumerate(cases):
        if number:
            time.sleep(args.pause)
        try:
            result = agent.ask(item["question"])
        except LLMError as exc:
            print(f"- {item['question']}\n    ERROR {exc}")
            if exc.status == 429 and not getattr(agent.llm, "has_available_model", lambda: True)():
                print("\nStopping: every configured model has reached its free limit for now.")
                break
            continue
        record = {**item, "answer": result.answer, "model": result.model, "evidence": result.evidence,
                  "exact": exact_match(item["expected"], result.answer) and bool(result.evidence) if item["expected"] else None}
        if judge is not None:
            add_judgement(record, judge, args.pause)
        records.append(record)
        with out.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        print_record(record)
    summarise(records)
    return 0


def rejudge(args) -> int:
    path = Path(args.rejudge)
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    judge = judge_llm()
    print(f"Judging {len(records)} saved answers from {path} with {judge.model}\n")
    for record in records:
        add_judgement(record, judge, args.pause)
        print_record(record)
    rejudged = path.with_name(path.stem + "_rejudged.jsonl")
    rejudged.write_text("".join(json.dumps(r, ensure_ascii=False, default=str) + "\n" for r in records), encoding="utf-8")
    summarise(records)
    print(f"\nSaved to {rejudged}")
    return 0


def check_judge(args) -> int:
    items = [json.loads(line) for line in Path(args.judge_set).read_text(encoding="utf-8").splitlines() if line.strip()]
    judge = judge_llm()
    print(f"Checking the judge (starting with {judge.model}) on {len(items)} labelled answers\n")
    tally = {"agree": 0, "missed_error": 0, "false_alarm": 0, "error": 0}
    models: dict[str, int] = {}
    for item in items:
        try:
            verdict = judge_answer(judge, item["question"], item["answer"], item["evidence"], item["kind"])
        except LLMError as exc:
            tally["error"] += 1
            print(f"ERROR  {item['id']}: {exc}")
            continue
        if verdict.passed == item["expected_pass"]:
            tally["agree"] += 1
            outcome = "agree"
        elif verdict.passed:
            tally["missed_error"] += 1
            outcome = "MISSED ERROR (judge passed a bad answer)"
        else:
            tally["false_alarm"] += 1
            outcome = "FALSE ALARM (judge failed a good answer)"
        models[verdict.judge_model] = models.get(verdict.judge_model, 0) + 1
        print(f"{outcome:<42} {item['id']}  ({verdict.judge_model})" + (f"\n    unsupported: {verdict.unsupported}" if outcome != "agree" else ""))
        time.sleep(args.pause)
    judged = len(items) - tally["error"]
    print(f"\nAgreement with hand labels: {tally['agree']}/{judged}")
    print(f"  bad answers the judge passed:  {tally['missed_error']}")
    print(f"  good answers the judge failed: {tally['false_alarm']}")
    if tally["error"]:
        print(f"  not judged (API errors):       {tally['error']}")
    print(f"  judged by: {', '.join(f'{name} ({count})' for name, count in models.items())}")
    return 0 if judged and tally["agree"] == judged else 1


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)  # show progress as it happens, also in Colab
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--artifacts", default="docs/artifacts")
    parser.add_argument("--reports", default="docs/reports")
    parser.add_argument("--out-dir", default="outputs/evals")
    parser.add_argument("--pause", type=float, default=6.0, help="Seconds between API calls (free tiers limit requests per minute)")
    parser.add_argument("--limit", type=int, default=0, help="Only ask the first N questions")
    parser.add_argument("--no-judge", action="store_true", help="Run the exact checks only")
    parser.add_argument("--rejudge", metavar="RUN_FILE", help="Judge the answers saved in an earlier run again")
    parser.add_argument("--check-judge", action="store_true", help="Measure the judge against hand-labelled answers")
    parser.add_argument("--judge-set", default="evals/judge_check.jsonl")
    args = parser.parse_args()
    if args.check_judge:
        return check_judge(args)
    if args.rejudge:
        return rejudge(args)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
