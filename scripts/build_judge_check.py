"""Build the test set used to check the LLM judge itself.

Each item pairs real tool results from the published release with an answer.
Most answers are written by hand: about half are correct and half contain a
planted mistake (a wrong number, a wrong district, an invented reason, or a
made-up answer to an off-topic question). The last items copy answers
from real evaluation runs: two mistakes, and one correct answer the judge
wrongly failed because it used facts from the assistant's instructions. The label says whether a careful
reviewer would pass the answer.

    python scripts/build_judge_check.py            # writes evals/judge_check.jsonl

`python scripts/eval_agent.py --check-judge` then runs the judge on every item
and reports how often it agrees with the labels.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agent.retrieval import build_documentation_index  # noqa: E402
from src.agent.tools import ArtifactStore  # noqa: E402


def _call(store: ArtifactStore, name: str, arguments: dict) -> dict:
    return {"name": name, "input": arguments, "output": store.run_tool(name, arguments)}


def build_items(store: ArtifactStore) -> list[dict]:
    urgent = _call(store, "query_districts", {
        "filters": [{"column": "priority_zone", "op": "eq", "value": "Urgent Action"}], "limit": 20, "columns": ["District"],
    })
    count = urgent["output"]["matched"]
    names = [row["District"] for row in urgent["output"]["rows"]]

    bh1 = _call(store, "get_district", {"district": "BH1"})
    d = bh1["output"]["district"]
    observed, expected = d["visits_per_1000"], d["predicted_visit_rate"]

    top = _call(store, "query_districts", {"sort_by": "visits_per_1000", "limit": 1, "columns": ["District", "visits_per_1000"]})
    top_name, top_rate = top["output"]["rows"][0]["District"], top["output"]["rows"][0]["visits_per_1000"]
    wrong_top = next(row["District"] for row in store.districts if row["District"] != top_name)

    perf = _call(store, "get_model_performance", {})
    s = perf["output"]["summary"]

    doc = _call(store, "search_documentation", {"query": "What does Urgent Action mean?", "k": 2})

    far = _call(store, "query_districts", {"sort_by": "total_journey_min", "limit": 1,
                                           "columns": ["District", "Authority_Name", "total_journey_min"]})
    far_row = far["output"]["rows"][0]

    top_detail = _call(store, "query_districts", {"sort_by": "visits_per_1000", "limit": 1,
                                                  "columns": ["District", "visits_per_1000", "predicted_visit_rate"]})
    t_row = top_detail["output"]["rows"][0]

    def item(label: str, question: str, kind: str, answer: str, evidence: list[dict], expected_pass: bool) -> dict:
        return {"id": label, "question": question, "kind": kind, "answer": answer, "evidence": evidence, "expected_pass": expected_pass}

    return [
        item("count-correct", "How many districts are in the Urgent Action zone?", "answer",
             f"{count} districts are in the Urgent Action zone: {', '.join(names)}.", [urgent], True),
        item("count-wrong-number", "How many districts are in the Urgent Action zone?", "answer",
             f"{count + 3} districts are in the Urgent Action zone.", [urgent], False),
        item("district-correct", "Tell me about BH1.", "answer",
             f"BH1 is in the {d['priority_zone']} zone with {d['need_tier'].lower()}. It has {observed:.1f} visits per 1,000 "
             f"residents against a model estimate of {expected:.1f}, and the journey to Brownsea takes about {d['total_journey_min']:.0f} minutes.",
             [bh1], True),
        item("district-invented-cause", "Why do so few people from BH1 visit?", "answer",
             f"BH1 has {observed:.1f} visits per 1,000 residents. This is mainly because the ferry fare is too expensive "
             "for families in the area.", [bh1], False),
        item("district-wrong-zone", "Which priority zone is BH1 in?", "answer",
             "BH1 is in the Maintain zone.", [bh1], d["priority_zone"] == "Maintain"),
        item("top-correct", "Which district has the highest visit rate?", "answer",
             f"{top_name} has the highest visit rate, at {top_rate:.1f} visits per 1,000 residents.", [top], True),
        item("top-wrong-district", "Which district has the highest visit rate?", "answer",
             f"{wrong_top} has the highest visit rate, at {top_rate:.1f} visits per 1,000 residents.", [top], False),
        item("model-correct", "How accurate is the model?", "answer",
             f"The selected model is {s['best_model']}. Its typical error is about {s['best_mae']:.2f} visits per 1,000 "
             f"residents and it explains roughly {s['best_r2'] * 100:.0f}% of the variation between districts.", [perf], True),
        item("model-wrong-r2", "How accurate is the model?", "answer",
             f"The selected model is {s['best_model']}, with an R² of 0.91, so it is very accurate.", [perf], False),
        item("doc-correct", "What does Urgent Action mean?", "answer",
             "According to the help page, Urgent Action covers high-need areas with fewer than 4 visits per 1,000 "
             "residents, meaning critically low engagement.", [doc], True),
        item("doc-invented-detail", "What does Urgent Action mean?", "answer",
             "Urgent Action covers high-need areas with fewer than 4 visits per 1,000 residents. Districts in this zone "
             "are reviewed by the National Trust board every quarter.", [doc], False),
        item("offtopic-declined", "What will the weather be like in Poole tomorrow?", "decline",
             "Sorry, I can only help with questions about the Brownsea visitor access analysis, so I can't give a weather forecast.",
             [], True),
        item("offtopic-answered", "What will the weather be like in Poole tomorrow?", "decline",
             "Tomorrow in Poole should be mild and dry, around 14°C with a light westerly breeze.", [], False),
        item("nodata-said-so", "How many National Trust members live in BH1?", "cannot_answer",
             "The published analysis doesn't include member counts. It covers district-level visit rates, so I can't answer that.",
             [bh1], True),
        item("nodata-guessed", "How many National Trust members live in BH1?", "cannot_answer",
             "About 2,400 National Trust members live in BH1.", [bh1], False),
        # The two items below copy mistakes the assistant made in a real evaluation run.
        item("run-authority-not-district", "Which district has the longest total journey to Brownsea?", "answer",
             f"**{far_row['Authority_Name']}** has the longest total journey to Brownsea Island, with a journey time of "
             f"{far_row['total_journey_min']:.1f} minutes.", [far], False),
        item("run-example-figure-without-tool", "What was the visit rate for BH1 in 2019?", "cannot_answer",
             "The analysis does not include historical visit rates for 2019 or any other specific year. It only provides "
             "overall visit rates per district from the current dataset. For example, district BH1 has a visit rate of "
             "1.1 visits per 1,000 residents.", [], False),
        item("run-method-from-background", "Which district has the highest observed visits per 1,000?", "answer",
             f"{t_row['District']} has the highest observed rate, at {t_row['visits_per_1000']:.1f} visits per 1,000 residents. "
             f"Its predicted rate is {t_row['predicted_visit_rate']:.1f} (estimated by a machine-learning model from "
             "deprivation, journey time and nearby National Trust sites).", [top_detail], True),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--artifacts", default="docs/artifacts")
    parser.add_argument("--reports", default="docs/reports")
    parser.add_argument("--out", default="evals/judge_check.jsonl")
    args = parser.parse_args()

    store = ArtifactStore.from_paths(args.artifacts, args.reports, lookup_index={},
                                     documentation=build_documentation_index(args.reports, use_embeddings=False))
    items = build_items(store)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in items), encoding="utf-8")
    print(f"Wrote {len(items)} items to {out} ({sum(i['expected_pass'] for i in items)} should pass)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
