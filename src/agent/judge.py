# -*- coding: utf-8 -*-
"""LLM-as-a-judge for the assistant's answers.

A second model reads the question, the assistant's answer and the tool results
the answer was based on, and lists every factual claim in the answer with
whether the tool results support it. The pass or fail decision is then made
here in code, from that list, so the judge only has to do the part a model is
good at: reading and comparing text.

What counts as a pass depends on the kind of question:

- answer:         every claim is supported and the question is answered
- decline:        an off-topic question is politely declined, with no claims made up
- cannot_answer:  an on-topic question the data cannot answer is met with
                  "the data does not cover that", not a guess
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

KINDS = ("answer", "decline", "cannot_answer")
MAX_EVIDENCE_CHARS = 16000
MAX_CHARS_PER_TOOL = 6000

JUDGE_SYSTEM = """You check answers written by an assistant that explains a published analysis of who visits \
Brownsea Island (National Trust). The assistant may only state facts that appear in its tool results.

You are given the question, the assistant's answer, and the evidence it had: the background it was told \
about the analysis, and its tool results.

1. List every factual claim the answer makes about the data or the method: numbers, district or postcode names, \
classifications, definitions, how something was calculated, and any stated reason or cause. Ignore greetings, \
offers of further help, and general cautions that make no claim about the data (for example "these are model estimates").
2. For each claim, decide whether the evidence supports it. A claim is supported when the evidence states it or it \
follows directly from the evidence; rounding to fewer decimal places is fine. A claim is not supported when the \
evidence contradicts it, does not mention it, or when it adds a cause or explanation the evidence does not give.
3. Say whether the answer gives what the question asks for. If the question asks for a district (a postcode \
district such as SP9) and the answer names a local authority, town or region instead, it does not answer the question.
4. Say whether the answer declines or says the data or documentation cannot answer the question.

Be literal. Do not use your own knowledge to fill gaps; if the evidence does not show it, it is not supported."""

VERDICT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "claims": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "claim": {"type": "string"},
                    "supported": {"type": "boolean"},
                    "note": {"type": "string", "description": "Where the evidence shows it, or why it is not supported."},
                },
                "required": ["claim", "supported"],
            },
        },
        "answers_question": {"type": "boolean"},
        "declines_or_cannot_answer": {"type": "boolean"},
        "reason": {"type": "string", "description": "One or two sentences summarising the judgement."},
    },
    "required": ["claims", "answers_question", "declines_or_cannot_answer", "reason"],
}


@dataclass
class Verdict:
    kind: str
    passed: bool
    faithful: bool
    answers_question: bool
    declines_or_cannot_answer: bool
    claims: list[dict[str, Any]] = field(default_factory=list)
    reason: str = ""
    judge_model: str = ""

    @property
    def unsupported(self) -> list[str]:
        return [str(item.get("claim", "")) for item in self.claims if not item.get("supported")]

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "unsupported": self.unsupported}


def format_evidence(evidence: list[dict[str, Any]]) -> str:
    """Tool calls and results as compact text, trimmed so the judge prompt stays a sensible size."""
    if not evidence:
        return "(The assistant called no tools, so it had no evidence.)"
    parts, used = [], 0
    for number, item in enumerate(evidence, start=1):
        output = json.dumps(item.get("output"), ensure_ascii=False, default=str)
        if len(output) > MAX_CHARS_PER_TOOL:
            output = output[:MAX_CHARS_PER_TOOL] + " ...(trimmed)"
        block = f"[{number}] {item.get('name')}({json.dumps(item.get('input'), ensure_ascii=False)})\n{output}"
        if used + len(block) > MAX_EVIDENCE_CHARS:
            parts.append(f"[{number}+] further tool results trimmed")
            break
        parts.append(block)
        used += len(block)
    return "\n\n".join(parts)


def decide(kind: str, raw: dict[str, Any]) -> Verdict:
    """Turn the judge's structured reply into a pass or fail for this kind of question."""
    if kind not in KINDS:
        raise ValueError(f"Unknown question kind '{kind}'. Use one of: {', '.join(KINDS)}")
    claims = [item for item in raw.get("claims") or [] if isinstance(item, dict)]
    faithful = all(bool(item.get("supported")) for item in claims)
    answers = bool(raw.get("answers_question"))
    declines = bool(raw.get("declines_or_cannot_answer"))
    if kind == "answer":
        passed = faithful and answers and not declines
    else:
        passed = faithful and declines
    return Verdict(kind=kind, passed=passed, faithful=faithful, answers_question=answers,
                   declines_or_cannot_answer=declines, claims=claims, reason=str(raw.get("reason") or ""))


def judge_answer(llm: Any, question: str, answer: str, evidence: list[dict[str, Any]], kind: str = "answer",
                 background: str | None = None) -> Verdict:
    """Ask the judge model about one answer. `llm` needs a complete_json(system, prompt, schema) method.

    `background` is what the assistant was told about the analysis in its instructions; by default
    the assistant's own BACKGROUND text.
    """
    if background is None:
        from src.agent.prompts import BACKGROUND as background  # noqa: PLC0415
    prompt = (
        f"QUESTION:\n{question}\n\n"
        f"ASSISTANT'S ANSWER:\n{answer}\n\n"
        f"EVIDENCE, PART 1 (background the assistant was given):\n{background}\n\n"
        f"EVIDENCE, PART 2 (the assistant's tool calls and their results):\n{format_evidence(evidence)}"
    )
    verdict = decide(kind, llm.complete_json(JUDGE_SYSTEM, prompt, VERDICT_SCHEMA))
    verdict.judge_model = getattr(llm, "last_model", "") or getattr(llm, "model", "")
    return verdict
