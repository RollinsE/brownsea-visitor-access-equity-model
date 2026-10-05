# -*- coding: utf-8 -*-
"""Tool-calling loop for the equity agent.

The loop is the whole "agent":

1. send the question, the system prompt and the tool descriptions to the LLM
2. if the LLM asks for a tool, run it and send the result back
3. repeat until the LLM writes a final answer (or the step limit is reached)

Which LLM is used (Gemini or Anthropic) is decided in llm.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.agent.llm import AgentUnavailable, default_llm
from src.agent.prompts import build_system_prompt
from src.agent.tools import TOOL_SCHEMAS, ArtifactStore

MAX_QUESTION_CHARS = 1000
MAX_HISTORY_TURNS = 6
MAX_HISTORY_CHARS = 4000


@dataclass
class AgentResult:
    answer: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    steps: int = 0
    complete: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"answer": self.answer, "tool_calls": self.tool_calls, "steps": self.steps, "complete": self.complete}


def clean_history(history: Any) -> list[dict[str, str]]:
    """Keep only recent, well-formed text turns supplied by the browser."""
    turns: list[dict[str, str]] = []
    for item in history if isinstance(history, list) else []:
        if not isinstance(item, dict):
            continue
        role, content = item.get("role"), item.get("content")
        if role in ("user", "assistant") and isinstance(content, str) and content.strip():
            turns.append({"role": role, "content": content.strip()[:MAX_HISTORY_CHARS]})
    turns = turns[-MAX_HISTORY_TURNS:]
    while turns and turns[0]["role"] != "user":
        turns.pop(0)
    merged: list[dict[str, str]] = []
    for turn in turns:
        if merged and merged[-1]["role"] == turn["role"]:
            merged[-1] = turn
        else:
            merged.append(turn)
    if merged and merged[-1]["role"] == "user":
        merged.pop()
    return merged


class EquityAgent:
    def __init__(self, store: ArtifactStore, llm: Any = None, model: str | None = None, max_steps: int = 6) -> None:
        if not store.has_districts:
            raise AgentUnavailable("District analysis table not found in this release.")
        self.store = store
        self.llm = llm if llm is not None else default_llm(model)
        self.max_steps = max_steps
        self.system_prompt = build_system_prompt(store)

    def ask(self, question: str, history: Any = None) -> AgentResult:
        question = (question or "").strip()
        if not question:
            raise ValueError("question is required")
        if len(question) > MAX_QUESTION_CHARS:
            raise ValueError(f"question must be at most {MAX_QUESTION_CHARS} characters")

        messages = self.llm.start(clean_history(history), question)
        tool_calls: list[dict[str, Any]] = []

        for step in range(1, self.max_steps + 1):
            reply = self.llm.send(self.system_prompt, messages, TOOL_SCHEMAS)
            if not reply.tool_requests:
                answer = reply.text or "I could not produce an answer to that. Please try rephrasing the question."
                return AgentResult(answer=answer, tool_calls=tool_calls, steps=step, complete=bool(reply.text))

            results = []
            for request in reply.tool_requests:
                output = self.store.run_tool(request["name"], request["input"])
                tool_calls.append({"name": request["name"], "input": request["input"]})
                results.append((request, output))
            self.llm.add_tool_results(messages, reply, results)

        return AgentResult(
            answer="I could not finish answering that within the step limit. Try a narrower question.",
            tool_calls=tool_calls,
            steps=self.max_steps,
            complete=False,
        )
