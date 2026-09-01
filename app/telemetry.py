"""Standard shape for SCADA telemetry returned by the API.

Every endpoint that returns a device returns its telemetry in the same shape, so
a caller never has to know which points a given device model happens to carry.

The important design point: the API hands back *units, labels and a suggested
chart series* alongside the values. A caller reading this should never need to
infer that `W` is megawatts or guess which points are worth plotting — and an
LLM caller should never fall back on describing what a device model "typically"
reports, which is how you get a confident answer that is not from the graph.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

SCADA_PREFIX = "scada__"

# point name -> (human label, unit, kind)
#   analog  — a measured value, chartable
#   status  — a discrete state, not chartable
#   counter — a monotonic count, chartable but rarely interesting live
POINT_CATALOG: Dict[str, tuple] = {
    "IA":          ("Phase A current",           "A",    "analog"),
    "IB":          ("Phase B current",           "A",    "analog"),
    "IC":          ("Phase C current",           "A",    "analog"),
    "IN":          ("Neutral current",           "A",    "analog"),
    "V":           ("Voltage",                   "V",    "analog"),
    "W":           ("Real power",                "MW",   "analog"),
    "VAR":         ("Reactive power",            "MVAr", "analog"),
    "PF":          ("Power factor",              "",     "analog"),
    "FREQ":        ("Frequency",                 "Hz",   "analog"),
    "OPERATIONS":  ("Operation count",           "",     "counter"),
    "TRIP":        ("Trip command",              "",     "status"),
    "CLOSE":       ("Close command",             "",     "status"),
    "52A":         ("Breaker aux contact (52a)", "",     "status"),
    "TARGET":      ("Relay target",              "",     "status"),
    "COMM_STATUS": ("Communications status",     "",     "status"),
}

# Carried as context, not presented as telemetry points.
METADATA_POINTS = {"source", "poll_cycle", "updated_at", "RELAY_MODEL"}

# First group whose points are all present becomes the suggested chart series.
CHART_PREFERENCE: List[List[str]] = [
    ["V", "IA", "IB", "IC"],
    ["IA", "IB", "IC"],
    ["W", "VAR"],
    ["V"],
    ["FREQ"],
]

# Past this, a reading is called out as stale rather than presented as live.
STALE_AFTER_SECONDS = 300


def _coerce_epoch(value: Any) -> Optional[datetime]:
    """Interpret an updated_at value that may be epoch seconds, millis, or ISO."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = value / 1000.0 if value > 1e11 else float(value)
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def extract_telemetry(props: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Build the standard telemetry block from a node's raw properties.

    Returns None when the device carries no telemetry at all — which callers
    should report as "no telemetry bound to this device", never paper over.
    """
    if not props:
        return None

    scada = {
        k[len(SCADA_PREFIX):]: v
        for k, v in props.items()
        if k.startswith(SCADA_PREFIX)
    }
    if not scada:
        return None

    analog: List[Dict[str, Any]] = []
    status: List[Dict[str, Any]] = []
    for point, value in sorted(scada.items()):
        if point in METADATA_POINTS or value is None:
            continue
        label, unit, kind = POINT_CATALOG.get(point, (point, "", "status"))
        entry: Dict[str, Any] = {"point": point, "label": label, "value": value}
        if unit:
            entry["unit"] = unit
        (analog if kind in ("analog", "counter") else status).append(entry)

    if not analog and not status:
        return None

    observed = _coerce_epoch(scada.get("updated_at"))
    age_seconds = None
    if observed is not None:
        age_seconds = max(0, int((datetime.now(timezone.utc) - observed).total_seconds()))

    present = {e["point"] for e in analog}
    chart_fields: List[str] = []
    for group in CHART_PREFERENCE:
        if all(p in present for p in group):
            chart_fields = list(group)
            break

    block: Dict[str, Any] = {
        "source": scada.get("source"),
        "updated_at": observed.isoformat() if observed else scada.get("updated_at"),
        "age_seconds": age_seconds,
        "stale": age_seconds is not None and age_seconds > STALE_AFTER_SECONDS,
        "point_count": len(analog) + len(status),
        "analog": analog,
        "status": status,
        # The series worth plotting. A caller rendering a chart should use
        # exactly these rather than choosing its own.
        "chart_fields": chart_fields,
    }
    if scada.get("poll_cycle") is not None:
        block["poll_cycle"] = scada["poll_cycle"]
    return block


def attach_telemetry(row: Dict[str, Any], props_key: str = "scadaProps") -> Dict[str, Any]:
    """Replace a row's raw scada property map with the standard telemetry block."""
    props = row.pop(props_key, None)
    row["telemetry"] = extract_telemetry(props)
    return row
