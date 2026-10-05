# -*- coding: utf-8 -*-
"""Ask the equity agent a question from the command line.

    python -m src.agent "Which districts need urgent action?"
    python -m src.agent --show-tools "How many districts are in each priority zone?"

By default it reads outputs/releases/latest, falling back to the published
copy in docs/.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.agent.agent import EquityAgent
from src.agent.llm import AgentUnavailable, LLMError
from src.agent.tools import DISTRICT_TABLE, ArtifactStore


def _default_dirs() -> tuple[Path, Path]:
    for base in (Path("outputs") / "releases" / "latest", Path("docs")):
        if (base / "artifacts" / DISTRICT_TABLE).exists():
            return base / "artifacts", base / "reports"
    return Path("docs") / "artifacts", Path("docs") / "reports"


def main(argv: list[str] | None = None) -> int:
    default_artifacts, default_reports = _default_dirs()
    parser = argparse.ArgumentParser(description="Ask the Brownsea equity agent a question")
    parser.add_argument("question")
    parser.add_argument("--artifacts", default=str(default_artifacts))
    parser.add_argument("--reports", default=str(default_reports))
    parser.add_argument("--model", default=None)
    parser.add_argument("--show-tools", action="store_true", help="Print the tool calls the agent made")
    args = parser.parse_args(argv)

    store = ArtifactStore.from_paths(args.artifacts, args.reports)
    try:
        agent = EquityAgent(store, model=args.model)
    except AgentUnavailable as exc:
        print(f"Agent unavailable: {exc}", file=sys.stderr)
        return 1

    try:
        result = agent.ask(args.question)
    except LLMError as exc:
        print(f"The LLM request failed: {exc}", file=sys.stderr)
        return 1
    print(result.answer)
    if args.show_tools:
        print("\nTool calls:")
        for call in result.tool_calls:
            print(f"  {call['name']} {json.dumps(call['input'], ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
