# -*- coding: utf-8 -*-
"""Connections to the LLM providers the agent can use.

Each provider class hides its own message format behind the same four methods,
so the agent loop in agent.py does not care which one is in use:

    start(history, question)          -> a new message list
    has_available_model()             -> (optional) whether a retry could use another model
    send(system, messages, tools)     -> LLMReply
    add_tool_results(messages, reply, results)
    name / model                      -> for logging and the health check

Gemini is called over plain HTTPS with the standard library, so it needs no
extra package. Anthropic needs `pip install anthropic`.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable

GEMINI_DEFAULT_MODEL = "gemini-3.8-flash"
# Tried in order when the model above is over its free limit or too busy. Each has its own quota.
GEMINI_FALLBACK_MODELS = ["gemini-3.7-flash", "gemini-3.6-flash", "gemini-3.5-flash", "gemini-3.5-flash-lite"]
ANTHROPIC_DEFAULT_MODEL = "claude-sonnet-5-5"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
REQUEST_TIMEOUT = 120  # seconds; long prompts to a thinking model can take a while
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
    model: str = ""  # which model produced it



# --------------------------------------------------------------------- Gemini
def _http_post(url: str, headers: dict[str, str], body: dict[str, Any], timeout: float = REQUEST_TIMEOUT) -> dict[str, Any]:
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
        if isinstance(exc.reason, TimeoutError):
            raise LLMError(f"Gemini took longer than {timeout:.0f} s to reply.", status=504) from None
        raise LLMError(f"Could not reach the Gemini API: {exc.reason}") from None
    except TimeoutError:
        # Treated like a busy model: retried once, then the next model is used.
        raise LLMError(f"Gemini took longer than {timeout:.0f} s to reply.", status=504) from None


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


class _Conversation(list):
    """A Gemini message list that remembers which model it started with.

    A conversation must stay on one model: Gemini's thought signatures are
    not interchangeable between models.
    """

    model: str = ""


def _retry_seconds(message: str) -> float | None:
    """Read the wait Gemini asks for, e.g. 'Please retry in 17h14m4.7s' -> 62044.7."""
    match = re.search(r"retry in\s+(?:(\d+)h)?(?:(\d+)m(?!s))?(?:([\d.]+)(ms|s))?", message or "", flags=re.IGNORECASE)
    if not match or not any(match.group(i) for i in (1, 2, 3)):
        return None
    seconds = float(match.group(3) or 0)
    if match.group(4) == "ms":
        seconds /= 1000
    return int(match.group(1) or 0) * 3600 + int(match.group(2) or 0) * 60 + seconds


class GeminiLLM:
    """Gemini over HTTPS, with a chain of models to fall back on.

    Free-tier limits are per model and can be small (for example 20 requests
    a day), and busy models return 503. When a model is over its limit or
    unavailable it is set aside for a while and the next model in the chain
    is used for new questions.
    """

    name = "gemini"

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        fallback_models: list[str] | None = None,
        max_tokens: int = 4096,
        transport: Callable[[str, dict[str, str], dict[str, Any]], dict[str, Any]] | None = None,
        short_wait_limit: float = 20.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.api_key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise AgentUnavailable("GEMINI_API_KEY is not set.")
        if fallback_models is None:
            configured = os.getenv("BROWNSEA_AGENT_FALLBACK_MODELS")
            fallback_models = GEMINI_FALLBACK_MODELS if configured is None else [m.strip() for m in configured.split(",")]
        chain = [model or GEMINI_DEFAULT_MODEL, *fallback_models]
        self.models = [name for name in dict.fromkeys(chain) if name]
        self.max_tokens = max_tokens
        self.transport = transport or _http_post
        self.short_wait_limit = short_wait_limit
        self._sleep = sleep
        self._clock = clock
        self._set_aside_until: dict[str, float] = {}
        self.last_problem = ""
        self.last_model = ""

    @property
    def model(self) -> str:
        """The model a new question would use right now."""
        return self._pick_model() or self.models[0]

    def _pick_model(self) -> str | None:
        now = self._clock()
        return next((name for name in self.models if self._set_aside_until.get(name, 0) <= now), None)

    def has_available_model(self) -> bool:
        return self._pick_model() is not None

    def start(self, history: list[dict[str, str]], question: str) -> list[dict[str, Any]]:
        model = self._pick_model()
        if model is None:
            raise LLMError(f"Every configured Gemini model is over its limit or unavailable. {self.last_problem}", status=429)
        messages = _Conversation(
            {"role": "model" if turn["role"] == "assistant" else "user", "parts": [{"text": turn["content"]}]}
            for turn in history
        )
        messages.append({"role": "user", "parts": [{"text": question}]})
        messages.model = model
        return messages

    def _post(self, model: str, body: dict[str, Any]) -> dict[str, Any]:
        """One request, with one short retry. Sets the model aside when it cannot serve."""
        url = GEMINI_URL.format(model=model)
        headers = {"Content-Type": "application/json", "x-goog-api-key": self.api_key}
        for attempt in (1, 2):
            try:
                return self.transport(url, headers, body)
            except LLMError as exc:
                if exc.status not in (404, 429, 503, 504):
                    raise
                wait = _retry_seconds(str(exc))
                if exc.status in (503, 504) and wait is None:
                    wait = 5.0  # busy model: one quick retry
                if exc.status != 404 and wait is not None and wait <= self.short_wait_limit and attempt == 1:
                    self._sleep(wait)  # per-minute limit or a brief spike
                    continue
                # Daily quota (long wait), a model that stays busy, or an unknown model name.
                pause = {404: 86400.0, 503: 120.0, 504: 120.0}.get(exc.status, wait if wait is not None else 60.0)
                self._set_aside_until[model] = self._clock() + min(pause, 86400.0)
                self.last_problem = f"{model}: {str(exc)[:200]}"
                raise
        raise AssertionError("unreachable")

    def send(self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMReply:
        body = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": list(messages),
            "tools": gemini_tools(tools),
            "generationConfig": {"maxOutputTokens": self.max_tokens},
        }
        model = getattr(messages, "model", "") or self.models[0]
        data = self._post(model, body)

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
        return LLMReply(text="\n".join(texts).strip(), tool_requests=requests, raw={"role": "model", "parts": parts}, model=model)

    def complete_json(self, system: str, prompt: str, schema: dict[str, Any]) -> dict[str, Any]:
        """One question, no tools, answered as JSON that follows `schema`.

        If a model is busy or over its limit, the next model in the chain is tried.
        The model that answered is kept in `last_model`.
        """
        body = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                # Room for the model's own reasoning as well as the JSON it returns.
                "maxOutputTokens": max(self.max_tokens, 8192),
                "temperature": 0,
                "responseMimeType": "application/json",
                "responseSchema": schema,
            },
        }
        while True:
            model = self._pick_model()
            if model is None:
                raise LLMError(f"Every configured Gemini model is over its limit or unavailable. {self.last_problem}", status=429)
            try:
                data = self._post(model, body)
                break
            except LLMError as exc:
                if exc.status not in (404, 429, 503, 504):
                    raise
        self.last_model = model
        parts = ((data.get("candidates") or [{}])[0].get("content") or {}).get("parts") or []
        text = "".join(part.get("text", "") for part in parts if not part.get("thought"))
        return _parse_json(text)

    def add_tool_results(self, messages: list[dict[str, Any]], reply: LLMReply, results: list[tuple[dict[str, Any], dict[str, Any]]]) -> None:
        messages.append(reply.raw)
        parts = []
        for request, output in results:
            response: dict[str, Any] = {"name": request["name"], "response": json.loads(_dump_safe(output))}
            if request.get("has_id"):
                response["id"] = request["id"]
            parts.append({"functionResponse": response})
        messages.append({"role": "user", "parts": parts})


def _parse_json(text: str) -> dict[str, Any]:
    """Read a JSON object from a model reply, allowing for a ```json fence around it."""
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", (text or "").strip())
    try:
        value = json.loads(cleaned)
    except ValueError as exc:
        raise LLMError(f"The model did not return valid JSON: {cleaned[:200]}") from exc
    if not isinstance(value, dict):
        raise LLMError("The model returned JSON that is not an object.")
    return value


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
        return LLMReply(text="\n".join(part for part in texts if part).strip(), tool_requests=requests, raw=content, model=self.model)

    def complete_json(self, system: str, prompt: str, schema: dict[str, Any]) -> dict[str, Any]:
        """One question, no tools, answered as JSON that follows `schema`."""
        instruction = f"{system}\n\nReply with a single JSON object that matches this schema, and nothing else:\n{json.dumps(schema)}"
        response = self.client.messages.create(
            model=self.model, max_tokens=self.max_tokens, system=instruction,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(_block_value(block, "text") or "" for block in _block_value(response, "content") or []
                       if _block_value(block, "type") == "text")
        return _parse_json(text)

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
