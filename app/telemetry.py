"""Standard shape for SCADA telemetry returned by the API.

Every endpoint that returns a device returns its telemetry in the same shape, so
a caller never has to know which points a given device model happens to carry.

Two design points matter here:

  * The API hands back *units, labels and a suggested chart series* alongside the
    values. A caller should never need to infer that `W` is megawatts or guess
    which points are worth plotting — and an LLM caller should never fall back on
    describing what a device model "typically" reports, which is how you get a
    confident answer that did not come from the graph.

  * Points are separated by kind. A relay's `51P_PICKUP` is a configured
    protection *setting*, not a live measurement, and presenting it in the same
    table as `IA` misrepresents both. Measurements, states, settings and
    counters are returned as four separate lists.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

SCADA_PREFIX = "scada__"

# point name -> (human label, unit, kind)
#   analog   — a live measured value; chartable
#   status   — a discrete state; not chartable
#   setting  — configured, changes only on reconfiguration; never "live"
#   counter  — a cumulative count
POINT_CATALOG: Dict[str, tuple] = {
    # ── Relay analogue measurements ─────────────────────────────────────
    "IA":               ("Phase A current",            "A",    "analog"),
    "IB":               ("Phase B current",            "A",    "analog"),
    "IC":               ("Phase C current",            "A",    "analog"),
    "IN":               ("Neutral current",            "A",    "analog"),
    "V":                ("Voltage",                    "V",    "analog"),
    "W":                ("Real power",                 "MW",   "analog"),
    "VAR":              ("Reactive power",             "MVAr", "analog"),
    "PF":               ("Power factor",               "",     "analog"),
    "FREQ":             ("Frequency",                  "Hz",   "analog"),
    # ── Transformer differential (87T) ──────────────────────────────────
    "87T_DIFF":         ("Differential current",       "A",    "analog"),
    "87T_RESTRAIN":     ("Restraint current",          "A",    "analog"),
    "87T_OPERATE":      ("Differential element",       "",     "status"),
    # ── Plant / transformer measurements ────────────────────────────────
    "voltage_kv":       ("Voltage",                    "kV",   "analog"),
    "frequency_hz":     ("Frequency",                  "Hz",   "analog"),
    "mw":               ("Real power",                 "MW",   "analog"),
    "mvar":             ("Reactive power",             "MVAr", "analog"),
    "temperature_c":    ("Temperature",                "°C", "analog"),
    "tap_position":     ("Tap position",               "",     "analog"),
    "cooling_stage":    ("Cooling stage",              "",     "status"),
    # ── Discrete states ─────────────────────────────────────────────────
    "TRIP":             ("Trip command",               "",     "status"),
    "CLOSE":            ("Close command",              "",     "status"),
    "52A":              ("Breaker aux contact (52a)",  "",     "status"),
    "TARGET":           ("Relay target",               "",     "status"),
    "COMM_STATUS":      ("Communications status",      "",     "status"),
    "79_STATE":         ("Recloser state",             "",     "status"),
    "status":           ("Device status",              "",     "status"),
    "position":         ("Switch position",            "",     "status"),
    # ── Configured protection settings — NOT measurements ───────────────
    "50P_PICKUP":       ("50P instantaneous OC pickup", "A",   "setting"),
    "51P_PICKUP":       ("51P time OC pickup",          "A",   "setting"),
    "51G_PICKUP":       ("51G ground OC pickup",        "A",   "setting"),
    "79_CYCLE":         ("Reclose cycle count",         "",    "setting"),
    "POLL_INTERVAL_MS": ("Poll interval",               "ms",  "setting"),
    # ── Counters and comms health ───────────────────────────────────────
    "OPERATIONS":       ("Operation count",            "",     "counter"),
    "trip_count":       ("Trip count",                 "",     "counter"),
    "DNP3_RESTARTS":    ("DNP3 link restarts",         "",     "counter"),
    "POINTS_REPORTING": ("Points reporting",           "",     "counter"),
    "LAST_POLL_MS":     ("Last poll duration",         "ms",   "counter"),
}

# Carried as context, not presented as points.
METADATA_POINTS = {"source", "poll_cycle", "updated_at", "RELAY_MODEL", "DEVICE_TYPE"}

# A double-prefixed key exists in the data (scada__scada__V). Normalise rather
# than surfacing it as an unknown point; the ingest bug is tracked separately.
ALIASES = {"scada__V": "V"}

# First group whose points are all present becomes the suggested chart series.
CHART_PREFERENCE: List[List[str]] = [
    ["V", "IA", "IB", "IC"],
    ["IA", "IB", "IC"],
    ["87T_DIFF", "87T_RESTRAIN"],
    ["mw", "mvar"],
    ["W", "VAR"],
    ["voltage_kv"],
    ["V"],
    ["temperature_c"],
    ["FREQ"],
]

# Past this, a reading is called out as stale rather than presented as live.
STALE_AFTER_SECONDS = 300


def _coerce_epoch(value: Any) -> Optional[datetime]:
    """Interpret an updated_at that may be epoch seconds, millis, or ISO text."""
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

    scada: Dict[str, Any] = {}
    for key, value in props.items():
        if not key.startswith(SCADA_PREFIX):
            continue
        point = key[len(SCADA_PREFIX):]
        scada[ALIASES.get(point, point)] = value
    if not scada:
        return None

    buckets: Dict[str, List[Dict[str, Any]]] = {
        "analog": [], "status": [], "setting": [], "counter": [],
    }
    for point, value in sorted(scada.items()):
        if point in METADATA_POINTS or value is None:
            continue
        label, unit, kind = POINT_CATALOG.get(point, (point, "", "status"))
        entry: Dict[str, Any] = {"point": point, "label": label, "value": value}
        if unit:
            entry["unit"] = unit
        buckets[kind].append(entry)

    if not any(buckets.values()):
        return None

    observed = _coerce_epoch(scada.get("updated_at"))
    age_seconds = None
    if observed is not None:
        age_seconds = max(0, int((datetime.now(timezone.utc) - observed).total_seconds()))

    present = {e["point"] for e in buckets["analog"]}
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
        "point_count": sum(len(v) for v in buckets.values()),
        # Live measurements.
        "analog": buckets["analog"],
        # Discrete states.
        "status": buckets["status"],
        # Configured thresholds — these are not measurements and must not be
        # presented as live readings.
        "settings": buckets["setting"],
        # Cumulative counts.
        "counters": buckets["counter"],
        # The series worth plotting. A caller rendering a chart should use
        # exactly these rather than choosing its own.
        "chart_fields": chart_fields,
    }
    if scada.get("poll_cycle") is not None:
        block["poll_cycle"] = scada["poll_cycle"]
    if scada.get("DEVICE_TYPE") is not None:
        block["device_type"] = scada["DEVICE_TYPE"]
    return block


def attach_telemetry(row: Dict[str, Any], props_key: str = "scadaProps") -> Dict[str, Any]:
    """Replace a row's raw scada property map with the standard telemetry block."""
    props = row.pop(props_key, None)
    row["telemetry"] = extract_telemetry(props)
    return row
