"""Rendering for fleet-scope answers.

The same lesson as `telemetry.py`: the API knows the values, the units and the
shape, so it emits the finished markdown and the caller's only job is to paste
it. That is what let a 7B local model produce the same output as a cloud model
— formatting reliability stopped depending on the model re-reading a contract
every turn.

It also means the reports view and the chat agent render from one place. A
standing report is a fleet query with fixed parameters; if each surface built
its own table they would drift, and two surfaces disagreeing about a count is
worse than either being ugly.

Every renderer here states its own coverage. A fleet rollup silently missing
the devices it could not resolve is the failure mode that matters, because the
number still looks plausible.
"""
from typing import Any, Dict, List, Optional, Tuple

# Past this many sites a pivot table stops fitting on a screen, and the rollup
# is rendered as totals with the per-site breakdown left to the JSON.
MAX_PIVOT_COLUMNS = 10

# Shown where a dimension value or a site is absent, so an empty cell always
# means zero rather than "unknown".
NOT_RECORDED = "(not recorded)"
UNASSIGNED = "(unassigned)"

DIMENSION_TITLES = {
    "model": "Device model",
    "manufacturer": "Manufacturer",
    "type": "Device type",
    "function": "ANSI function",
    "firmware": "Firmware version",
    "site": "Site",
}


def _fmt(value: Any) -> str:
    """Table-safe cell text — pipes would otherwise break the row."""
    if value is None:
        return "—"
    return str(value).replace("|", "\\|")


def _describe_filters(filters: Dict[str, Any]) -> str:
    """One clause naming what was filtered on, for the heading."""
    parts = [f"{k}={v}" for k, v in sorted(filters.items()) if v]
    return ", ".join(parts)


def build_caveats(
    coverage: Optional[Dict[str, Any]],
    scope_note: Optional[str] = None,
) -> List[str]:
    """Statements a fleet answer must carry to be honest about itself.

    Returned as a list rather than folded into prose so a caller can render
    them, log them, or surface them as warnings in a report header.
    """
    caveats: List[str] = []
    if scope_note:
        caveats.append(scope_note)
    if not coverage:
        return caveats

    resolution = coverage.get("siteResolution") or {}
    unresolved = resolution.get("unresolved") or 0
    if unresolved:
        caveats.append(
            f"{unresolved} device(s) could not be tied to a site and are grouped "
            f"under '{UNASSIGNED}'. They are counted in totals but not attributed "
            "to any substation."
        )

    attrs = coverage.get("attributeCoverage") or {}
    total = coverage.get("totalDevices") or 0
    for key, label in (("model", "a device model"),
                       ("manufacturer", "a manufacturer"),
                       ("firmware", "a firmware version")):
        have = attrs.get(key) or 0
        if total and have < total:
            caveats.append(
                f"{total - have} of {total} devices have no {label} recorded; "
                f"they appear as '{NOT_RECORDED}'."
            )

    for entry in coverage.get("duplication") or []:
        if entry.get("duplicated"):
            caveats.append(
                f"The {entry['entity']} catalog holds {entry['nodes']} nodes for "
                f"{entry['distinctValues']} distinct values — counts here collapse "
                "the duplicates, but the underlying data should be de-duplicated."
            )
    return caveats


def render_inventory(
    rows: List[Dict[str, Any]],
    dimension: str,
    filters: Optional[Dict[str, Any]] = None,
    caveats: Optional[List[str]] = None,
) -> Tuple[str, List[str], Dict[str, Any]]:
    """Pivot a fleet rollup into value × site, and return the finished markdown.

    Returns (markdown, sites, totals) so a caller that wants to build its own
    view still gets the pivot rather than having to redo it.
    """
    title = DIMENSION_TITLES.get(dimension, dimension.title())
    sites = sorted({r["site"] for r in rows})
    pivot: Dict[str, Dict[str, int]] = {}
    for row in rows:
        pivot.setdefault(row["value"], {})[row["site"]] = row["devices"]

    row_totals = {v: sum(cells.values()) for v, cells in pivot.items()}
    col_totals = {s: sum(pivot[v].get(s, 0) for v in pivot) for s in sites}
    grand_total = sum(row_totals.values())

    # Real values first, then the placeholder buckets — a reader wants the
    # inventory, with the gaps acknowledged underneath it rather than on top.
    def sort_key(value: str):
        placeholder = value in (NOT_RECORDED, UNASSIGNED)
        return (placeholder, -row_totals[value], value)

    ordered = sorted(pivot, key=sort_key)

    scope = _describe_filters(filters or {})
    heading = f"**{title} across the fleet**"
    if scope:
        heading += f" — filtered by {scope}"
    heading += f" — {grand_total} device(s) across {len(sites)} site(s)"

    parts: List[str] = [heading, ""]

    if len(sites) <= MAX_PIVOT_COLUMNS:
        header = f"| {title} | " + " | ".join(_fmt(s) for s in sites) + " | Total |"
        divider = "| :--- | " + " | ".join("---:" for _ in sites) + " | ---: |"
        parts += [header, divider]
        for value in ordered:
            cells = " | ".join(
                str(pivot[value].get(s, "")) or "–" for s in sites
            )
            parts.append(f"| {_fmt(value)} | {cells} | **{row_totals[value]}** |")
        totals = " | ".join(f"**{col_totals[s]}**" for s in sites)
        parts.append(f"| **Total** | {totals} | **{grand_total}** |")
    else:
        parts += [f"| {title} | Devices |", "| :--- | ---: |"]
        for value in ordered:
            parts.append(f"| {_fmt(value)} | {row_totals[value]} |")
        parts.append(f"| **Total** | **{grand_total}** |")
        parts.append("")
        parts.append(f"_{len(sites)} sites — per-site breakdown omitted from the "
                     "table; see `rows` for the full pivot._")

    for caveat in caveats or []:
        parts.append("")
        parts.append(f"> {caveat}")

    return "\n".join(parts), sites, {"byValue": row_totals,
                                     "bySite": col_totals,
                                     "total": grand_total}


def render_devices(
    rows: List[Dict[str, Any]],
    filters: Optional[Dict[str, Any]] = None,
    caveats: Optional[List[str]] = None,
    truncated: bool = False,
) -> str:
    """Finished markdown for a fleet device list, grouped by site."""
    scope = _describe_filters(filters or {})
    by_site: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        by_site.setdefault(row.get("site") or UNASSIGNED, []).append(row)

    heading = "**Fleet device list**"
    if scope:
        heading += f" — {scope}"
    heading += f" — {len(rows)} device(s) across {len(by_site)} site(s)"

    parts: List[str] = [heading, ""]
    show_firmware = any(r.get("firmware") for r in rows)
    show_functions = any(r.get("ansiFunctions") for r in rows)

    cols = ["Site", "Device", "Type", "Model", "Manufacturer"]
    if show_firmware:
        cols.append("Firmware")
    if show_functions:
        cols.append("ANSI")
    parts.append("| " + " | ".join(cols) + " |")
    parts.append("| " + " | ".join([":---"] * len(cols)) + " |")

    for site in sorted(by_site):
        for row in by_site[site]:
            cells = [site, _fmt(row.get("name")), _fmt(row.get("type")),
                     _fmt(row.get("model")), _fmt(row.get("manufacturer"))]
            if show_firmware:
                cells.append(_fmt(row.get("firmware")))
            if show_functions:
                fns = row.get("ansiFunctions") or []
                cells.append(", ".join(fns) if fns else "—")
            parts.append("| " + " | ".join(cells) + " |")

    if truncated:
        parts.append("")
        parts.append("> Result truncated at the row limit — narrow the filter "
                     "or raise `limit` to see the rest.")
    for caveat in caveats or []:
        parts.append("")
        parts.append(f"> {caveat}")
    return "\n".join(parts)


def render_models(rows: List[Dict[str, Any]]) -> str:
    """Finished markdown for the device model catalog."""
    in_use = [r for r in rows if (r.get("deviceCount") or 0) > 0]
    unused = len(rows) - len(in_use)

    parts = [
        f"**Device model catalog** — {len(in_use)} model(s) in use, "
        f"{unused} in the catalog with no device linked",
        "",
        "| Model | Manufacturer | Devices | Sites | ANSI functions |",
        "| :--- | :--- | ---: | :--- | :--- |",
    ]
    for row in in_use:
        sites = ", ".join(row.get("sites") or []) or "—"
        fns = ", ".join(row.get("ansiFunctions") or []) or "—"
        parts.append(
            f"| {_fmt(row.get('model'))} | {_fmt(row.get('manufacturer'))} | "
            f"{row.get('deviceCount')} | {_fmt(sites)} | {_fmt(fns)} |"
        )
    if unused:
        parts.append("")
        parts.append(f"> {unused} catalog model(s) are not linked to any device. "
                     "That is expected for a reference catalog, but it also means "
                     "a device whose model was only recorded as text will not "
                     "appear against its model here.")
    return "\n".join(parts)


def render_coverage(coverage: Dict[str, Any]) -> str:
    """Finished markdown for the data-quality report.

    Deliberately blunt: this exists so a fleet number can be trusted, and the
    parts that cannot be trusted are the ones worth reading.
    """
    resolution = coverage.get("siteResolution") or {}
    attrs = coverage.get("attributeCoverage") or {}
    total = coverage.get("totalDevices") or 0
    unresolved = coverage.get("unresolvedDevices") or []

    parts = [
        f"**Fleet data coverage** — {total} device(s) across "
        f"{coverage.get('sites')} site(s)",
        "",
        "**How each device was tied to a site**",
        "",
        "| Path | Devices |",
        "| :--- | ---: |",
        f"| Denormalized `substationName` | {resolution.get('denormalized', 0)} |",
        f"| CIM containment | {resolution.get('containment', 0)} |",
        f"| Protected switch (relays) | {resolution.get('protectedSwitch', 0)} |",
        f"| **Unresolved** | **{resolution.get('unresolved', 0)}** |",
        "",
        "**Attribute coverage**",
        "",
        "| Attribute | Devices | Of total |",
        "| :--- | ---: | ---: |",
    ]
    for key, label in (("model", "Model (node or text)"),
                       ("modelNode", "Model linked to a catalog node"),
                       ("manufacturer", "Manufacturer"),
                       ("firmware", "Firmware version")):
        have = attrs.get(key) or 0
        pct = f"{100 * have // total}%" if total else "—"
        parts.append(f"| {label} | {have} | {pct} |")

    if unresolved:
        parts += ["", "**Devices not tied to any site**", "",
                  "| Device | Type |", "| :--- | :--- |"]
        for device in unresolved[:25]:
            parts.append(f"| {_fmt(device.get('name'))} | {_fmt(device.get('type'))} |")
        if len(unresolved) > 25:
            parts.append(f"| _…and {len(unresolved) - 25} more_ | |")

    duplication = [d for d in (coverage.get("duplication") or []) if d.get("duplicated")]
    if duplication:
        parts += ["", "**Catalog duplication**", "",
                  "| Entity | Distinct values | Stored nodes |",
                  "| :--- | ---: | ---: |"]
        for entry in duplication:
            parts.append(f"| {entry['entity']} | {entry['distinctValues']} | "
                         f"{entry['nodes']} |")
        parts.append("")
        parts.append("> Fleet counts collapse these duplicates, so the numbers above "
                     "are correct. The duplication is still worth fixing at ingest: "
                     "any query written by hand against these nodes will double-count.")
    return "\n".join(parts)
