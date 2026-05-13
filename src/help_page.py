"""Help page content for the Brownsea Visitor Access Tool."""

from __future__ import annotations

from html import escape
from pathlib import Path


def _rows(rows: list[tuple[str, str]]) -> str:
    return "\n".join(
        f"<tr><td><strong>{escape(term)}</strong></td><td>{escape(meaning)}</td></tr>"
        for term, meaning in rows
    )


def _table(title: str, rows: list[tuple[str, str]], *, class_name: str = "") -> str:
    css_class = f' class="{class_name}"' if class_name else ""
    return f"""
<section class="card table-card">
  <h2>{escape(title)}</h2>
  <table{css_class}>
    <tbody>
      {_rows(rows)}
    </tbody>
  </table>
</section>"""


def _priority_rows() -> str:
    rows = [
        ("Urgent Action", "Less than 4 visits per 1,000", "High-need areas with critically low engagement."),
        ("High Priority", "Less than 4 visits per 1,000", "Medium-need areas with low engagement."),
        ("Monitor", "4.0 to 6.9 visits per 1,000", "Medium-need areas with moderate engagement."),
        ("Growth Opportunity", "Less than 4 visits per 1,000", "Lower-need areas with growth potential."),
        ("Maintain", "7 or more visits per 1,000", "Areas with good engagement meeting expectations."),
    ]
    return "\n".join(
        f"<tr><td><strong>{escape(zone)}</strong></td><td>{escape(rate)}</td><td>{escape(desc)}</td></tr>"
        for zone, rate, desc in rows
    )


def _intervention_rows() -> str:
    rows = [
        ("Crisis Intervention", "Highest"),
        ("Targeted Support", "High"),
        ("Growth & Awareness", "Medium"),
        ("Model District", "Benchmark"),
        ("Sustain & Optimise", "Ongoing"),
    ]
    return _rows(rows)


def build_help_html(*, home_href: str = "/", downloads_href: str = "/downloads", reports_href: str = "/reports/index.html") -> str:
    """Return the Help & Definitions page as standalone HTML."""
    access_rows = [
        ("Departure terminal", "The selected mainland departure point used for the Brownsea journey estimate."),
        ("Journey to departure terminal", "Estimated travel time from the postcode area to the selected departure terminal."),
        ("Ferry crossing time", "Fixed Brownsea passenger ferry crossing allowance used in the estimate."),
        ("Total Brownsea journey", "Journey to departure terminal plus Brownsea ferry crossing time."),
        ("Accessibility score", "A 0 to 100 planning score based on estimated Brownsea journey time. Higher is better."),
        ("Nearest competing NT site", "The nearest comparison National Trust site by estimated travel time. Brownsea is excluded from this comparison."),
        ("Brownsea journey minus nearest NT drive", "Difference between estimated Brownsea journey time and the nearest comparison NT site drive time. Positive values mean Brownsea takes longer."),
    ]
    score_rows = [
        ("80 to 100", "Strong access"),
        ("60 to 79", "Good access"),
        ("40 to 59", "Moderate access"),
        ("20 to 39", "Weak access"),
        ("0 to 19", "Very weak access"),
    ]
    engagement_rows = [
        ("Observed visits per 1,000", "Current observed Brownsea visit rate for the postcode district, shown per 1,000 residents."),
        ("Model expected visits per 1,000", "The visit rate the model expects for a district with similar characteristics."),
        ("Performance against expectation", "Difference between observed and expected visits, shown as visits per 1,000."),
    ]
    need_rows = [
        ("High Need", "Areas with severe socioeconomic challenges."),
        ("Medium Need", "Areas with moderate socioeconomic challenges."),
        ("Low Need", "Areas with relatively lower socioeconomic challenges."),
    ]
    deprivation_rows = [
        ("Most Deprived", "Districts with higher deprivation levels. These areas may face stronger access and affordability barriers."),
        ("Moderately Deprived", "Districts with mixed or moderate deprivation levels. These areas may need monitoring or targeted support depending on engagement."),
        ("Least Deprived", "Districts with lower deprivation levels. These areas generally face fewer socioeconomic barriers, though access barriers may still exist."),
    ]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Brownsea Help & Definitions</title>
<style>
:root {{ --bg:#f7fafc; --card:#fff; --ink:#1f2937; --muted:#6b7280; --line:#e5e7eb; --brand:#0f4c5c; --brand2:#2c7a7b; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:Arial,sans-serif; color:var(--ink); background:var(--bg); }}
header {{ background:linear-gradient(135deg,var(--brand),var(--brand2)); color:#fff; padding:28px 24px; }}
main {{ max-width:1160px; margin:0 auto; padding:24px; }}
a {{ color:#0f4c5c; }} header a {{ color:#fff; }}
.links {{ display:flex; gap:16px; flex-wrap:wrap; margin-top:14px; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:18px; margin-top:18px; box-shadow:0 4px 12px rgba(0,0,0,.04); }}
h1 {{ margin:0; }} h2 {{ margin:0 0 12px; font-size:18px; color:#374151; }}
p {{ line-height:1.5; }}
.table-grid {{ display:grid; grid-template-columns:repeat(2,minmax(280px,1fr)); gap:18px; align-items:start; }}
.table-card {{ margin-top:0; }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }}
th {{ text-align:left; color:#374151; background:#f3f4f6; }}
th, td {{ padding:10px 8px; border-bottom:1px solid var(--line); vertical-align:top; }}
td:first-child {{ width:34%; }}
.priority td:first-child {{ width:24%; }}
.priority td:nth-child(2) {{ width:27%; }}
.full-width {{ margin-top:18px; }}
@media (max-width:820px) {{ .table-grid {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<header>
  <h1>Help & Definitions</h1>
  <p style="max-width:820px; margin:8px 0 0;">This tool estimates Brownsea Island access and engagement context from mainland visitor-origin postcodes. It is intended for planning and decision support, not live journey planning.</p>
  <div class="links"><a href="{escape(home_href)}">Postcode lookup</a><a href="{escape(downloads_href)}">Reports & Downloads</a><a href="{escape(reports_href)}">Reports index</a></div>
</header>
<main>
  <div class="table-grid">
    {_table("Access definitions", access_rows)}
    {_table("Accessibility score guide", score_rows)}
  </div>

  <div class="table-grid full-width">
    {_table("Engagement and model terms", engagement_rows)}
    {_table("Need tier definitions", need_rows)}
  </div>

  <div class="table-grid full-width">
    {_table("Deprivation categories", deprivation_rows)}
    <section class="card table-card">
      <h2>Intervention strategy framework</h2>
      <table><tbody>{_intervention_rows()}</tbody></table>
    </section>
  </div>

  <section class="card full-width">
    <h2>Priority action matrix</h2>
    <table class="priority">
      <thead><tr><th>Priority zone</th><th>Visit rate range</th><th>Description</th></tr></thead>
      <tbody>{_priority_rows()}</tbody>
    </table>
  </section>
</main>
</body>
</html>"""


def write_help_html(output_path: Path, *, home_href: str = "/", downloads_href: str = "/downloads", reports_href: str = "/reports/index.html") -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        build_help_html(home_href=home_href, downloads_href=downloads_href, reports_href=reports_href),
        encoding="utf-8",
    )
    return output_path
