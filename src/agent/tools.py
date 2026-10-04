# -*- coding: utf-8 -*-
"""Read-only tools the equity agent can call.

Every tool reads artifacts the pipeline has already produced (postcode lookup,
district analysis table, model performance, framework definitions). Nothing
here writes, retrains or reaches the private visitor records.

The module uses only the standard library so the lightweight Flask image does
not need pandas.
"""
from __future__ import annotations

import csv
import json
import re
import statistics
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Iterable

DISTRICT_TABLE = "three_way_intersection_analysis_v2.csv"
MODEL_PERFORMANCE_CSV = "model_performance.csv"
MODEL_PERFORMANCE_SUMMARY = "model_performance_summary.json"
DEFINITION_TABLES = {
    "priority_zones": "priority_action_matrix_categories.html",
    "intervention_types": "intervention_strategy_framework.html",
    "need_tiers": "need_tier_definitions.html",
}

BROWNSEA_DESTINATION_POSTCODES = {"BH137EE"}
HIDDEN_DISTRICT_COLUMNS = {"shap_error", "visits_gap_raw"}
DEFAULT_DISTRICT_COLUMNS = [
    "District",
    "Authority_Name",
    "priority_zone",
    "intervention_type",
    "need_tier",
    "visits_per_1000",
    "predicted_visit_rate",
    "performance_gap",
    "total_journey_min",
    "Population",
]
MAX_ROWS = 50
OPERATORS = ("eq", "ne", "lt", "le", "gt", "ge", "in", "contains")
AGGREGATIONS = ("count", "sum", "mean", "median", "min", "max")

_BROWNSEA_COMPETITOR = re.compile(r";?\s*Brownsea Island:\s*[^;\]]*", flags=re.IGNORECASE)


class ToolError(ValueError):
    """A problem the model can fix by calling the tool differently."""


def normalise_postcode(value: Any) -> str:
    return str(value or "").replace(" ", "").upper().strip()


def _coerce(value: Any) -> Any:
    """Turn a CSV cell into None, bool, float or str."""
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return None
    if text in ("True", "False"):
        return text == "True"
    try:
        return float(text)
    except ValueError:
        return text


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [{key: _coerce(val) for key, val in row.items()} for row in csv.DictReader(handle)]


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag == "tr":
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._row is not None and self._cell is not None:
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None


def _read_html_table(path: Path) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(path.read_text(encoding="utf-8"))
    if len(parser.rows) < 2:
        return []
    header, *body = parser.rows
    return [dict(zip(header, row)) for row in body if len(row) == len(header)]


def _round(value: Any) -> Any:
    return round(value, 4) if isinstance(value, float) else value


class ArtifactStore:
    """In-memory, read-only view of one release's artifacts."""

    def __init__(
        self,
        lookup_index: dict[str, dict[str, Any]] | None = None,
        districts: list[dict[str, Any]] | None = None,
        model_performance: dict[str, Any] | None = None,
        definitions: dict[str, list[dict[str, str]]] | None = None,
    ) -> None:
        self.lookup_index = lookup_index or {}
        self.districts = districts or []
        self.model_performance = model_performance or {}
        self.definitions = definitions or {}
        self._columns = {name.lower(): name for name in self.district_columns()}

    # ------------------------------------------------------------------ loading
    @classmethod
    def from_paths(
        cls,
        artifacts_dir: str | Path,
        reports_dir: str | Path | None = None,
        lookup_index: dict[str, dict[str, Any]] | None = None,
        narrative_cleaner: Callable[[Any], str] | None = None,
    ) -> "ArtifactStore":
        artifacts = Path(artifacts_dir)

        if lookup_index is None:
            lookup_index = cls._load_lookup(artifacts)

        districts: list[dict[str, Any]] = []
        table = artifacts / DISTRICT_TABLE
        if table.exists():
            for row in _read_csv(table):
                for hidden in HIDDEN_DISTRICT_COLUMNS:
                    row.pop(hidden, None)
                context = row.get("competitor_context")
                if isinstance(context, str):
                    row["competitor_context"] = _BROWNSEA_COMPETITOR.sub("", context).replace("[;", "[").strip()
                if narrative_cleaner and row.get("shap_narrative"):
                    row["shap_narrative"] = narrative_cleaner(row["shap_narrative"])
                districts.append(row)

        performance: dict[str, Any] = {}
        summary = artifacts / MODEL_PERFORMANCE_SUMMARY
        if summary.exists():
            performance["summary"] = json.loads(summary.read_text(encoding="utf-8"))
        perf_csv = artifacts / MODEL_PERFORMANCE_CSV
        if perf_csv.exists():
            performance["models"] = _read_csv(perf_csv)

        definitions: dict[str, list[dict[str, str]]] = {}
        if reports_dir is not None:
            tables = Path(reports_dir) / "tables"
            for topic, filename in DEFINITION_TABLES.items():
                path = tables / filename
                if path.exists():
                    rows = _read_html_table(path)
                    if rows:
                        definitions[topic] = rows

        return cls(lookup_index, districts, performance, definitions)

    @staticmethod
    def _load_lookup(artifacts: Path) -> dict[str, dict[str, Any]]:
        json_path = artifacts / "postcode_lookup.json"
        csv_path = artifacts / "postcode_lookup.csv"
        if json_path.exists():
            rows = json.loads(json_path.read_text(encoding="utf-8"))
        elif csv_path.exists():
            rows = _read_csv(csv_path)
        else:
            return {}
        index: dict[str, dict[str, Any]] = {}
        for row in rows:
            if "brownsea" in str(row.get("nearest_nt_site_name") or "").lower():
                row["nearest_nt_site_name"] = "No competing NT site identified"
                row["nearest_nt_site_drive_min"] = None
                row["brownsea_vs_nearest_nt_gap_min"] = None
            key = normalise_postcode(row.get("postcode_clean") or row.get("postcode"))
            if key:
                index[key] = row
        return index

    # ----------------------------------------------------------------- metadata
    @property
    def has_districts(self) -> bool:
        return bool(self.districts)

    def district_columns(self) -> list[str]:
        return list(self.districts[0].keys()) if self.districts else []

    def column_catalogue(self, max_categories: int = 12) -> list[dict[str, Any]]:
        """Describe each district column so the model knows what it can filter on."""
        catalogue: list[dict[str, Any]] = []
        for name in self.district_columns():
            values = [row[name] for row in self.districts if row.get(name) is not None]
            if values and all(isinstance(v, bool) for v in values):
                catalogue.append({"column": name, "type": "boolean"})
            elif values and all(isinstance(v, float) for v in values):
                catalogue.append({"column": name, "type": "number", "min": _round(min(values)), "max": _round(max(values))})
            else:
                distinct = sorted({str(v) for v in values})
                entry: dict[str, Any] = {"column": name, "type": "text"}
                if len(distinct) <= max_categories:
                    entry["values"] = distinct
                catalogue.append(entry)
        return catalogue

    # ------------------------------------------------------------------ helpers
    def _column(self, name: Any) -> str:
        key = str(name or "").strip().lower()
        if key not in self._columns:
            raise ToolError(f"Unknown column '{name}'. Valid columns: {', '.join(self.district_columns())}")
        return self._columns[key]

    def _apply_filters(self, filters: Iterable[dict[str, Any]] | None) -> list[dict[str, Any]]:
        rows = self.districts
        for item in filters or []:
            if not isinstance(item, dict):
                raise ToolError("Each filter must be an object with column, op and value.")
            column = self._column(item.get("column"))
            op = str(item.get("op", "eq")).lower()
            if op not in OPERATORS:
                raise ToolError(f"Unknown op '{op}'. Use one of: {', '.join(OPERATORS)}")
            rows = [row for row in rows if _matches(row.get(column), op, item.get("value"))]
        return rows

    # -------------------------------------------------------------------- tools
    def lookup_postcode(self, postcode: str) -> dict[str, Any]:
        query = normalise_postcode(postcode)
        if not query:
            raise ToolError("postcode is required")
        if query in BROWNSEA_DESTINATION_POSTCODES:
            return {
                "match_type": "destination",
                "message": "BH13 7EE is Brownsea Island itself. Ask for a mainland visitor origin postcode.",
            }
        exact = self.lookup_index.get(query)
        if exact:
            return {"match_type": "exact", "result": exact}
        prefix = next((row for key, row in self.lookup_index.items() if key.startswith(query)), None)
        if prefix:
            return {
                "match_type": "prefix",
                "note": "No exact match; this is the first postcode starting with the query.",
                "result": prefix,
            }
        return {"match_type": "none", "message": f"No BH, DT or SP postcode matches '{postcode}'."}

    def get_district(self, district: str) -> dict[str, Any]:
        key = normalise_postcode(district)
        for row in self.districts:
            if normalise_postcode(row.get("District")) == key:
                return {"district": row}
        known = ", ".join(str(row.get("District")) for row in self.districts)
        raise ToolError(f"Unknown district '{district}'. Known districts: {known}")

    def query_districts(
        self,
        filters: list[dict[str, Any]] | None = None,
        sort_by: str | None = None,
        descending: bool = True,
        limit: int = 10,
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        rows = self._apply_filters(filters)
        if sort_by:
            sort_column = self._column(sort_by)
            present = [row for row in rows if row.get(sort_column) is not None]
            missing = [row for row in rows if row.get(sort_column) is None]
            present.sort(key=lambda row: row[sort_column], reverse=bool(descending))
            rows = present + missing
        else:
            sort_column = None

        selected = [self._column(name) for name in columns] if columns else list(DEFAULT_DISTRICT_COLUMNS)
        extras = [self._column(item["column"]) for item in filters or []]
        if sort_column:
            extras.append(sort_column)
        available = set(self.district_columns())
        selected = [name for name in dict.fromkeys(selected + extras) if name in available]

        limit = max(1, min(int(limit or 10), MAX_ROWS))
        return {
            "matched": len(rows),
            "returned": min(len(rows), limit),
            "total_districts": len(self.districts),
            "rows": [{name: _round(row.get(name)) for name in selected} for row in rows[:limit]],
        }

    def aggregate_districts(
        self,
        metrics: list[dict[str, Any]] | None = None,
        group_by: str | None = None,
        filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        rows = self._apply_filters(filters)
        group_column = self._column(group_by) if group_by else None

        specs: list[tuple[str, str]] = []
        for item in metrics or []:
            agg = str(item.get("agg", "")).lower()
            if agg not in AGGREGATIONS:
                raise ToolError(f"Unknown agg '{agg}'. Use one of: {', '.join(AGGREGATIONS)}")
            specs.append((self._column(item.get("column")), agg))

        groups: dict[Any, list[dict[str, Any]]] = {}
        for row in rows:
            groups.setdefault(row.get(group_column) if group_column else "all", []).append(row)

        results = []
        for key, members in groups.items():
            entry: dict[str, Any] = {(group_column or "group"): key, "districts": len(members)}
            for column, agg in specs:
                entry[f"{agg}_{column}"] = _aggregate([row.get(column) for row in members], agg, column)
            results.append(entry)
        results.sort(key=lambda entry: entry["districts"], reverse=True)
        return {"matched": len(rows), "total_districts": len(self.districts), "groups": results}

    def get_model_performance(self) -> dict[str, Any]:
        if not self.model_performance:
            return {"message": "Model performance artifacts are not available in this release."}
        return {
            **self.model_performance,
            "note": "Cross-validated metrics as reported by the pipeline (lower MAE and higher R2 are better). Predicted visit rates are model estimates, not observations.",
        }

    def get_definitions(self, topic: str | None = None) -> dict[str, Any]:
        if not self.definitions:
            return {"message": "Framework definition tables are not available in this release."}
        if topic:
            if topic not in self.definitions:
                raise ToolError(f"Unknown topic '{topic}'. Use one of: {', '.join(self.definitions)}")
            return {topic: self.definitions[topic]}
        return dict(self.definitions)

    # ----------------------------------------------------------------- dispatch
    def run_tool(self, name: str, arguments: dict[str, Any] | None) -> dict[str, Any]:
        """Run a tool by name. Bad input comes back as {'error': ...} so the model can retry."""
        handlers: dict[str, Callable[..., dict[str, Any]]] = {
            "lookup_postcode": self.lookup_postcode,
            "get_district": self.get_district,
            "query_districts": self.query_districts,
            "aggregate_districts": self.aggregate_districts,
            "get_model_performance": self.get_model_performance,
            "get_definitions": self.get_definitions,
        }
        handler = handlers.get(name)
        if handler is None:
            return {"error": f"Unknown tool '{name}'."}
        try:
            return handler(**(arguments or {}))
        except ToolError as exc:
            return {"error": str(exc)}
        except TypeError as exc:
            return {"error": f"Bad arguments for {name}: {exc}"}


def _matches(cell: Any, op: str, target: Any) -> bool:
    if op == "in":
        options = target if isinstance(target, list) else [target]
        return any(_matches(cell, "eq", option) for option in options)
    if cell is None:
        return False
    if op == "contains":
        return str(target).lower() in str(cell).lower()
    if isinstance(cell, bool):
        wanted = target if isinstance(target, bool) else str(target).strip().lower() in ("true", "1", "yes")
        return (cell == wanted) if op == "eq" else (cell != wanted) if op == "ne" else False
    if isinstance(cell, float):
        try:
            number = float(target)
        except (TypeError, ValueError):
            raise ToolError(f"'{target}' is not a number.") from None
        return {
            "eq": cell == number, "ne": cell != number, "lt": cell < number,
            "le": cell <= number, "gt": cell > number, "ge": cell >= number,
        }[op]
    left, right = str(cell).strip().lower(), str(target).strip().lower()
    if op == "eq":
        return left == right
    if op == "ne":
        return left != right
    raise ToolError(f"Operator '{op}' only works on number columns; use eq, ne, in or contains for text.")


def _aggregate(values: list[Any], agg: str, column: str) -> Any:
    present = [value for value in values if value is not None]
    if agg == "count":
        return len(present)
    numbers = [float(value) for value in present if isinstance(value, (int, float))]
    if len(numbers) != len(present):
        raise ToolError(f"Cannot compute {agg} of text column '{column}'. Use count, or group_by it.")
    if not numbers:
        return None
    result = {
        "sum": sum, "mean": statistics.fmean, "median": statistics.median, "min": min, "max": max,
    }[agg](numbers)
    return _round(float(result))


_FILTERS_SCHEMA = {
    "type": "array",
    "description": "Conditions that must all hold. Column names come from the district column list.",
    "items": {
        "type": "object",
        "properties": {
            "column": {"type": "string"},
            "op": {"type": "string", "enum": list(OPERATORS)},
            "value": {"description": "Number, text, boolean, or a list when op is 'in'."},
        },
        "required": ["column", "op", "value"],
    },
}

TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "name": "lookup_postcode",
        "description": "Get the full record for one BH, DT or SP postcode: journey to Brownsea, nearest competing National Trust site, deprivation context and the district-level assessment.",
        "input_schema": {
            "type": "object",
            "properties": {"postcode": {"type": "string", "description": "e.g. 'BH15 1AA'"}},
            "required": ["postcode"],
        },
    },
    {
        "name": "get_district",
        "description": "Get every available field for one postcode district (e.g. 'BH15'), including the plain-language narrative, safe-zone status, risk flags and sensitivity results.",
        "input_schema": {
            "type": "object",
            "properties": {"district": {"type": "string"}},
            "required": ["district"],
        },
    },
    {
        "name": "query_districts",
        "description": "Filter, sort and list postcode districts. Returns 'matched' (the exact count) plus up to 'limit' rows. Use this for 'which districts...' and ranking questions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "filters": _FILTERS_SCHEMA,
                "sort_by": {"type": "string"},
                "descending": {"type": "boolean", "description": "Default true."},
                "limit": {"type": "integer", "description": f"Default 10, maximum {MAX_ROWS}."},
                "columns": {"type": "array", "items": {"type": "string"}, "description": "Extra columns to return."},
            },
        },
    },
    {
        "name": "aggregate_districts",
        "description": "Compute counts, sums, means, medians, minimums or maximums over districts, optionally grouped by a column. Always use this instead of doing arithmetic yourself.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metrics": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "column": {"type": "string"},
                            "agg": {"type": "string", "enum": list(AGGREGATIONS)},
                        },
                        "required": ["column", "agg"],
                    },
                },
                "group_by": {"type": "string"},
                "filters": _FILTERS_SCHEMA,
            },
        },
    },
    {
        "name": "get_model_performance",
        "description": "Get the accuracy metrics (MAE, R2) for the visit-rate models, so you can say how reliable the expected visit rates are.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_definitions",
        "description": "Get the official definitions for priority zones, intervention types and need tiers.",
        "input_schema": {
            "type": "object",
            "properties": {"topic": {"type": "string", "enum": list(DEFINITION_TABLES)}},
        },
    },
]
