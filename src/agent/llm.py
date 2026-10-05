# -*- coding: utf-8 -*-
"""Connections to the LLM providers the agent can use.

Each provider class hides its own message format behind the same four methods,
so the agent loop in agent.py does not care which one is in use:

    start(history, question)          -> a new message list
    send(system, messages, tools)     -> LLMReply
    add_tool_results(messages, reply, results)
    name / model                      -> for logging and the health check

Gemini is called over plain HTTPS with the standard library, so it needs no
extra package. Anthropic needs `pip install anthropic`.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable

GEMINI_DEFAULT_MODEL = "gemini-3.8-flash"
ANTHROPIC_DEFAULT_MODEL = "claude-sonnet-5-5"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
MAX_TOOL_RESULT_CHARS = 20000


class AgentUnavailable(RuntimeError):
    """The agent cannot run (missing API key, package or data)."""


class LLMError(RuntimeError):
    """The provider rejected or failed a request."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass
class LLMReply:
    text: str = ""
    tool_requests: list[dict[str, Any]] = field(default_factory=list)  # each: {id, name, input}
    raw: Any = None  # the provider's own copy of the reply, replayed on the next turn



# --------------------------------------------------------------------- Gemini
def _http_post(url: str, headers: dict[str, str], body: dict[str, Any], timeout: float = 60) -> dict[str, Any]:
    request = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            detail = json.loads(detail)["error"]["message"]
        except (ValueError, KeyError, TypeError):
            pass
        raise LLMError(f"Gemini API error {exc.code}: {str(detail)[:500]}", status=exc.code) from None
    except urllib.error.URLError as exc:
        raise LLMError(f"Could not reach the Gemini API: {exc.reason}") from None


def gemini_tools(tool_schemas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert the shared tool descriptions to Gemini's functionDeclarations."""
    declarations = []
    for tool in tool_schemas:
        declaration: dict[str, Any] = {"name": tool["name"], "description": tool["description"]}
        schema = tool.get("input_schema") or {}
        if schema.get("properties"):  # Gemini rejects an object schema with no properties
            declaration["parameters"] = schema
        declarations.append(declaration)
    return [{"functionDeclarations": declarations}]


class GeminiLLM:
    name = "gemini"

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        max_tokens: int = 4096,
        transport: Callable[[str, dict[str, str], dict[str, Any]], dict[str, Any]] | None = None,
        retries: int = 2,
        retry_wait: float = 15.0,
    ) -> None:
        self.api_key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise AgentUnavailable("GEMINI_API_KEY is not set.")
        self.model = model or GEMINI_DEFAULT_MODEL
        self.max_tokens = max_tokens
        self.transport = transport or _http_post
        self.retries = retries
        self.retry_wait = retry_wait

    def start(self, history: list[dict[str, str]], question: str) -> list[dict[str, Any]]:
        messages = [
            {"role": "model" if turn["role"] == "assistant" else "user", "parts": [{"text": turn["content"]}]}
            for turn in history
        ]
        messages.append({"role": "user", "parts": [{"text": question}]})
        return messages

    def send(self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMReply:
        body = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": messages,
            "tools": gemini_tools(tools),
            "generationConfig": {"maxOutputTokens": self.max_tokens},
        }
        url = GEMINI_URL.format(model=self.model)
        headers = {"Content-Type": "application/json", "x-goog-api-key": self.api_key}

        for attempt in range(self.retries + 1):
            try:
                data = self.transport(url, headers, body)
                break
            except LLMError as exc:
                # 429 = rate limit (common on the free tier), 503 = model overloaded
                if exc.status not in (429, 503) or attempt == self.retries:
                    raise
                time.sleep(self.retry_wait)

        candidates = data.get("candidates") or []
        if not candidates:
            reason = (data.get("promptFeedback") or {}).get("blockReason", "no candidates returned")
            raise LLMError(f"Gemini returned no answer ({reason}).")
        content = candidates[0].get("content") or {}
        parts = content.get("parts") or []

        texts, requests = [], []
        for index, part in enumerate(parts):
            if "functionCall" in part:
                call = part["functionCall"]
                requests.append({
                    "id": call.get("id") or f"call_{index}",
                    "name": call.get("name"),
                    "input": call.get("args") or {},
                    "has_id": "id" in call,
                })
            elif part.get("text") and not part.get("thought"):
                texts.append(part["text"])
        # The reply is replayed exactly as received: Gemini attaches a
        # thoughtSignature to function calls and rejects the next turn without it.
        return LLMReply(text="\n".join(texts).strip(), tool_requests=requests, raw={"role": "model", "parts": parts})

    def add_tool_results(self, messages: list[dict[str, Any]], reply: LLMReply, results: list[tuple[dict[str, Any], dict[str, Any]]]) -> None:
        messages.append(reply.raw)
        parts = []
        for request, output in results:
            response: dict[str, Any] = {"name": request["name"], "response": json.loads(_dump_safe(output))}
            if request.get("has_id"):
                response["id"] = request["id"]
            parts.append({"functionResponse": response})
        messages.append({"role": "user", "parts": parts})


def _dump_safe(output: dict[str, Any]) -> str:
    """JSON text of a tool result, replaced by a short error if it is too large to send."""
    text = json.dumps(output, ensure_ascii=False, default=str)
    if len(text) > MAX_TOOL_RESULT_CHARS:
        return json.dumps({"error": "Result too large. Ask for fewer rows or fewer columns."})
    return text


# ------------------------------------------------------------------ Anthropic
def _block_value(block: Any, key: str) -> Any:
    return block.get(key) if isinstance(block, dict) else getattr(block, key, None)


class AnthropicLLM:
    name = "anthropic"

    def __init__(self, client: Any = None, model: str | None = None, max_tokens: int = 1500) -> None:
        if client is None:
            if not os.getenv("ANTHROPIC_API_KEY"):
                raise AgentUnavailable("ANTHROPIC_API_KEY is not set.")
            try:
                import anthropic
            except ImportError as exc:
                raise AgentUnavailable("The 'anthropic' package is not installed (pip install anthropic).") from exc
            client = anthropic.Anthropic()
        self.client = client
        self.model = model or ANTHROPIC_DEFAULT_MODEL
        self.max_tokens = max_tokens

    def start(self, history: list[dict[str, str]], question: str) -> list[dict[str, Any]]:
        return [*history, {"role": "user", "content": question}]

    def send(self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMReply:
        response = self.client.messages.create(
            model=self.model, max_tokens=self.max_tokens, system=system, tools=tools, messages=messages,
        )
        texts, requests, content = [], [], []
        for block in _block_value(response, "content") or []:
            kind = _block_value(block, "type")
            if kind == "text":
                text = _block_value(block, "text") or ""
                texts.append(text)
                content.append({"type": "text", "text": text})
            elif kind == "tool_use":
                request = {"id": _block_value(block, "id"), "name": _block_value(block, "name"), "input": _block_value(block, "input") or {}}
                requests.append(request)
                content.append({"type": "tool_use", **request})
        return LLMReply(text="\n".join(part for part in texts if part).strip(), tool_requests=requests, raw=content)

    def add_tool_results(self, messages: list[dict[str, Any]], reply: LLMReply, results: list[tuple[dict[str, Any], dict[str, Any]]]) -> None:
        messages.append({"role": "assistant", "content": reply.raw})
        messages.append({"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": request["id"], "content": _dump_safe(output), "is_error": "error" in output}
            for request, output in results
        ]})


# ------------------------------------------------------------------ selection
def default_llm(model: str | None = None) -> Any:
    """Pick the provider from BROWNSEA_AGENT_PROVIDER, or from whichever API key is set."""
    model = model or os.getenv("BROWNSEA_AGENT_MODEL") or None
    provider = (os.getenv("BROWNSEA_AGENT_PROVIDER") or "").strip().lower()
    if not provider:
        if os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"):
            provider = "gemini"
        elif os.getenv("ANTHROPIC_API_KEY"):
            provider = "anthropic"
        else:
            raise AgentUnavailable("No API key found. Set GEMINI_API_KEY (or ANTHROPIC_API_KEY).")
    if provider == "gemini":
        return GeminiLLM(model=model)
    if provider == "anthropic":
        return AnthropicLLM(model=model)
    raise AgentUnavailable(f"Unknown BROWNSEA_AGENT_PROVIDER '{provider}'. Use 'gemini' or 'anthropic'.")
