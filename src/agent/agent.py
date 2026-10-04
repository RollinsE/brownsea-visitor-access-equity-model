# -*- coding: utf-8 -*-
"""Tool-calling loop for the equity agent.

The loop is the whole "agent":

1. send the question, the system prompt and the tool descriptions to the LLM
2. if the LLM asks for a tool, run it and send the result back
3. repeat until the LLM writes a final answer (or the step limit is reached)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

from src.agent.prompts import build_system_prompt
from src.agent.tools import TOOL_SCHEMAS, ArtifactStore

DEFAULT_MODEL = "claude-sonnet-5-5"
MAX_QUESTION_CHARS = 1000
MAX_HISTORY_TURNS = 6
MAX_HISTORY_CHARS = 4000
MAX_TOOL_RESULT_CHARS = 20000


class AgentUnavailable(RuntimeError):
    """The agent cannot run (missing API key, SDK or data)."""


@dataclass
class AgentResult:
    answer: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    steps: int = 0
    complete: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"answer": self.answer, "tool_calls": self.tool_calls, "steps": self.steps, "complete": self.complete}


def default_client() -> Any:
    """Create the Anthropic client, with clear errors when it cannot be created."""
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise AgentUnavailable("ANTHROPIC_API_KEY is not set.")
    try:
        import anthropic
    except ImportError as exc:
        raise AgentUnavailable("The 'anthropic' package is not installed (pip install anthropic).") from exc
    return anthropic.Anthropic()


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


def _block_value(block: Any, key: str) -> Any:
    return block.get(key) if isinstance(block, dict) else getattr(block, key, None)


class EquityAgent:
    def __init__(
        self,
        store: ArtifactStore,
        client: Any = None,
        model: str | None = None,
        max_steps: int = 6,
        max_tokens: int = 1500,
    ) -> None:
        if not store.has_districts:
            raise AgentUnavailable("District analysis table not found in this release.")
        self.store = store
        self.client = client if client is not None else default_client()
        self.model = model or os.getenv("BROWNSEA_AGENT_MODEL") or DEFAULT_MODEL
        self.max_steps = max_steps
        self.max_tokens = max_tokens
        self.system_prompt = build_system_prompt(store)

    def ask(self, question: str, history: Any = None) -> AgentResult:
        question = (question or "").strip()
        if not question:
            raise ValueError("question is required")
        if len(question) > MAX_QUESTION_CHARS:
            raise ValueError(f"question must be at most {MAX_QUESTION_CHARS} characters")

        messages: list[dict[str, Any]] = clean_history(history)
        messages.append({"role": "user", "content": question})
        tool_calls: list[dict[str, Any]] = []

        for step in range(1, self.max_steps + 1):
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=self.system_prompt,
                tools=TOOL_SCHEMAS,
                messages=messages,
            )
            text, requests, assistant_content = self._read_response(response)
            if not requests:
                return AgentResult(answer=text, tool_calls=tool_calls, steps=step)

            messages.append({"role": "assistant", "content": assistant_content})
            results = []
            for request in requests:
                output = self.store.run_tool(request["name"], request["input"])
                tool_calls.append({"name": request["name"], "input": request["input"]})
                results.append({
                    "type": "tool_result",
                    "tool_use_id": request["id"],
                    "content": json.dumps(output, ensure_ascii=False, default=str)[:MAX_TOOL_RESULT_CHARS],
                    "is_error": "error" in output,
                })
            messages.append({"role": "user", "content": results})

        return AgentResult(
            answer="I could not finish answering that within the step limit. Try a narrower question.",
            tool_calls=tool_calls,
            steps=self.max_steps,
            complete=False,
        )

    @staticmethod
    def _read_response(response: Any) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        """Split a model response into its text, its tool requests and a replayable copy."""
        texts: list[str] = []
        requests: list[dict[str, Any]] = []
        content: list[dict[str, Any]] = []
        for block in _block_value(response, "content") or []:
            kind = _block_value(block, "type")
            if kind == "text":
                text = _block_value(block, "text") or ""
                texts.append(text)
                content.append({"type": "text", "text": text})
            elif kind == "tool_use":
                request = {
                    "id": _block_value(block, "id"),
                    "name": _block_value(block, "name"),
                    "input": _block_value(block, "input") or {},
                }
                requests.append(request)
                content.append({"type": "tool_use", **request})
        return "\n".join(part for part in texts if part).strip(), requests, content
