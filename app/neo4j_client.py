"""
Neo4j Client — Shared connection and query helpers for Verance AI services.

This module provides a reusable Neo4j connection layer that mirrors the
Blazegraph/SPARQL patterns used elsewhere. All services (Graph API,
CIMgraph API, Drawing Link, etc.) use this as their Neo4j abstraction.

CIM Mapping (n10s handleVocabUris=SHORTEN):
  - Labels:  cim__Substation, cim__Feeder, cim__Breaker, ...
  - Props:   cim__IdentifiedObject.name, cim__IdentifiedObject.mRID, ...
  - Rels:    cim__Feeder.NormalEnergizingSubstation, cim__Equipment.EquipmentContainer, ...

Environment:
  NEO4J_URI      bolt://neo4j.railway.internal:7687
  NEO4J_USER     neo4j
  NEO4J_PASSWORD verance-ai-dev
"""

import os
import logging
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase, AsyncGraphDatabase

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j.railway.internal:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "verance-ai-dev")

# CIM namespace prefix as shortened by n10s
CIM = "cim__"  # n10s SHORTEN mode turns "http://iec.ch/TC57/CIM100#" → "cim__"
# Verance secondary-system namespace prefix (SCADA, network, protection devices)
VER = "ns1__"

# ── Synchronous driver (for Flask services) ───────────────────────────────

_driver = None


def get_driver():
    """Get or create the synchronous Neo4j driver (singleton)."""
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        logger.info(f"Neo4j driver created: {NEO4J_URI}")
    return _driver


def close_driver():
    """Close the synchronous driver."""
    global _driver
    if _driver:
        _driver.close()
        _driver = None


def execute_cypher(query: str, parameters: dict = None) -> List[Dict[str, Any]]:
    """Execute a Cypher query and return results as list of dicts.

    This is the Neo4j equivalent of execute_sparql_direct() from api.py.
    """
    driver = get_driver()
    with driver.session() as session:
        result = session.run(query, parameters or {})
        return [dict(record) for record in result]


def check_neo4j() -> bool:
    """Health check — can we reach Neo4j?"""
    try:
        driver = get_driver()
        driver.verify_connectivity()
        return True
    except Exception as e:
        logger.warning(f"Neo4j health check failed: {e}")
        return False


# ── Async driver (for FastAPI services) ───────────────────────────────────

_async_driver = None


def get_async_driver():
    """Get or create the async Neo4j driver (singleton)."""
    global _async_driver
    if _async_driver is None:
        _async_driver = AsyncGraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        logger.info(f"Async Neo4j driver created: {NEO4J_URI}")
    return _async_driver


async def close_async_driver():
    """Close the async driver."""
    global _async_driver
    if _async_driver:
        await _async_driver.close()
        _async_driver = None


async def execute_cypher_async(query: str, parameters: dict = None) -> List[Dict[str, Any]]:
    """Execute a Cypher query asynchronously and return results as list of dicts."""
    driver = get_async_driver()
    async with driver.session() as session:
        result = await session.run(query, parameters or {})
        records = await result.data()
        return records


async def check_neo4j_async() -> bool:
    """Async health check."""
    try:
        driver = get_async_driver()
        await driver.verify_connectivity()
        return True
    except Exception as e:
        logger.warning(f"Neo4j async health check failed: {e}")
        return False


# ── CIM-specific Cypher helpers ───────────────────────────────────────────

def cim_label(cim_class: str) -> str:
    """Convert CIM class name to Neo4j label.

    n10s with handleVocabUris=SHORTEN maps:
      http://iec.ch/TC57/CIM100#Substation → label "cim__Substation"

    Usage:
      cim_label("Substation") → "cim__Substation"
    """
    return f"{CIM}{cim_class}"


def cim_prop(prop_path: str) -> str:
    """Convert CIM property path to Neo4j property key.

    n10s maps:
      http://iec.ch/TC57/CIM100#IdentifiedObject.name → "cim__IdentifiedObject.name"

    Usage:
      cim_prop("IdentifiedObject.name") → "cim__IdentifiedObject.name"
    """
    return f"{CIM}{prop_path}"


# ── Common CIM queries (Cypher equivalents of SPARQL endpoints) ──────────


def cypher_list_models() -> str:
    """Cypher: List all feeders grouped by substation (mirrors GET /models)."""
    return f"""
MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s:{cim_label('Substation')})
OPTIONAL MATCH (s)-[:`{cim_prop('Substation.Region')}`]->(r)
OPTIONAL MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
WITH f, s, r, count(DISTINCT eq) AS equipmentCount
RETURN
  elementId(f)           AS feeder,
  f.`{cim_prop('IdentifiedObject.name')}`  AS feederName,
  elementId(s)           AS substation,
  s.`{cim_prop('IdentifiedObject.name')}`  AS substationName,
  r.`{cim_prop('IdentifiedObject.name')}`  AS regionName,
  equipmentCount
ORDER BY substationName, feederName
"""


def cypher_list_substations() -> str:
    """Cypher: List all substations (mirrors GET /substations)."""
    return f"""
MATCH (s:{cim_label('Substation')})
OPTIONAL MATCH (s)-[:`{cim_prop('Substation.Region')}`]->(r)
RETURN
  elementId(s)           AS substation,
  s.`{cim_prop('IdentifiedObject.name')}`  AS name,
  elementId(r)           AS region,
  r.`{cim_prop('IdentifiedObject.name')}`  AS regionName
ORDER BY name
"""


def _substation_equipment_cte() -> str:
    """
    Common subquery that finds equipment in a substation via ALL containment paths:
      - Direct: Equipment → Substation (some CGMES models)
      - Distribution: Equipment → Feeder → Substation
      - CGMES/Transmission: Equipment → Bay → VoltageLevel → Substation
      - CGMES direct VL: Equipment → VoltageLevel → Substation
      - Auxiliary: AuxiliaryEquipment via Terminal → ConductingEquipment in substation
      - Measurement: Measurement via Terminal → ConductingEquipment in substation
    Returns (eq, containerName) for each equipment node.
    Finds both cim__ (standard CIM) and ns1__ (Verance SCADA/network) labelled nodes.
    """
    # Match nodes with either CIM or Verance namespace labels
    lbl_filter = f"(lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}') AND lbl <> 'Resource'"
    return f"""
    CALL {{
        // Path 1: Equipment directly in Substation
        WITH s
        MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(s)
        WHERE any(lbl IN labels(eq) WHERE {lbl_filter})
        RETURN eq, s.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 2: Equipment in Feeders (distribution models)
        WITH s
        MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
        MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
        WHERE any(lbl IN labels(eq) WHERE {lbl_filter})
        RETURN eq, f.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 3: Equipment in Bays within VoltageLevels (CGMES models)
        WITH s
        MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
        MATCH (bay:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl)
        MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay)
        WHERE any(lbl IN labels(eq) WHERE {lbl_filter})
        RETURN eq, vl.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 4: Equipment directly in VoltageLevels (no Bay)
        WITH s
        MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
        MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(vl)
        WHERE any(lbl IN labels(eq) WHERE {lbl_filter})
          AND NOT eq:{cim_label('Bay')}
        RETURN eq, vl.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 5: AuxiliaryEquipment via Terminal → ConductingEquipment in substation
        WITH s
        MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
        MATCH (bay:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl)
        MATCH (ce)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay)
        MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(ce)
        MATCH (aux)-[:`{cim_prop('AuxiliaryEquipment.Terminal')}`]->(t)
        WHERE any(lbl IN labels(aux) WHERE {lbl_filter})
        RETURN aux AS eq, vl.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 6: AuxiliaryEquipment via Terminal → ConductingEquipment in Feeders
        WITH s
        MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
        MATCH (ce)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
        MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(ce)
        MATCH (aux)-[:`{cim_prop('AuxiliaryEquipment.Terminal')}`]->(t)
        WHERE any(lbl IN labels(aux) WHERE {lbl_filter})
        RETURN aux AS eq, f.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 7: Measurement nodes via Terminal → ConductingEquipment in substation
        WITH s
        MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
        MATCH (bay:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl)
        MATCH (ce)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay)
        MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(ce)
        MATCH (m)-[:`{cim_prop('Measurement.Terminal')}`]->(t)
        WHERE any(lbl IN labels(m) WHERE {lbl_filter})
        RETURN m AS eq, vl.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 8: Measurement nodes via Terminal → ConductingEquipment in Feeders
        WITH s
        MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
        MATCH (ce)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
        MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(ce)
        MATCH (m)-[:`{cim_prop('Measurement.Terminal')}`]->(t)
        WHERE any(lbl IN labels(m) WHERE {lbl_filter})
        RETURN m AS eq, f.`{cim_prop('IdentifiedObject.name')}` AS containerName
    }}
    """


def cypher_substation_equipment(substation_name: str) -> str:
    """Cypher: Equipment in a substation via feeders OR voltage-levels/bays."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_substation_equipment_cte()}
RETURN DISTINCT
  elementId(eq)          AS equipment,
  eq.`{cim_prop('IdentifiedObject.name')}`  AS name,
  [lbl IN labels(eq) WHERE (lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}') AND lbl <> 'Resource' | replace(replace(lbl, '{CIM}', ''), '{VER}', '')][0] AS type,
  containerName
ORDER BY type, name
"""


def cypher_substation_transformers(substation_name: str) -> str:
    """Cypher: Transformers with winding details via feeders OR voltage-levels/bays."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_substation_equipment_cte()}
WITH DISTINCT eq, containerName
WHERE eq:{cim_label('PowerTransformer')}
OPTIONAL MATCH (w:{cim_label('PowerTransformerEnd')})-[:`{cim_prop('PowerTransformerEnd.PowerTransformer')}`]->(eq)
OPTIONAL MATCH (w)-[:`{cim_prop('TransformerEnd.BaseVoltage')}`]->(bv:{cim_label('BaseVoltage')})
RETURN
  elementId(eq)           AS transformer,
  eq.`{cim_prop('IdentifiedObject.name')}`  AS name,
  containerName,
  w.`{cim_prop('IdentifiedObject.name')}`           AS windingName,
  w.`{cim_prop('PowerTransformerEnd.ratedU')}`       AS ratedU,
  w.`{cim_prop('PowerTransformerEnd.ratedS')}`       AS ratedS,
  w.`{cim_prop('PowerTransformerEnd.connectionKind')}` AS connectionKind,
  w.`{cim_prop('TransformerEnd.endNumber')}`         AS endNumber,
  bv.`{cim_prop('BaseVoltage.nominalVoltage')}`      AS baseVoltage
ORDER BY name, endNumber
"""


def cypher_substation_breakers(substation_name: str) -> str:
    """Cypher: Breakers and switching devices via feeders OR voltage-levels/bays."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_substation_equipment_cte()}
WITH DISTINCT eq, containerName
WHERE any(lbl IN labels(eq) WHERE lbl IN [
  '{cim_label("Breaker")}', '{cim_label("Disconnector")}',
  '{cim_label("LoadBreakSwitch")}', '{cim_label("Recloser")}', '{cim_label("Fuse")}'
])
RETURN
  elementId(eq)          AS switch,
  eq.`{cim_prop('IdentifiedObject.name')}`  AS name,
  [lbl IN labels(eq) WHERE (lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}') AND lbl <> 'Resource' | replace(replace(lbl, '{CIM}', ''), '{VER}', '')][0] AS type,
  containerName,
  eq.`{cim_prop('Switch.normalOpen')}`  AS normalOpen,
  eq.`{cim_prop('Switch.retained')}`    AS retained
ORDER BY type, name
"""


def cypher_substation_voltage_levels(substation_name: str) -> str:
    """Cypher: Voltage levels in a substation (CGMES VoltageLevel or base voltages from feeders)."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
CALL {{
    // CGMES: VoltageLevel objects directly under Substation
    WITH s
    MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
    OPTIONAL MATCH (vl)-[:`{cim_prop('VoltageLevel.BaseVoltage')}`]->(bv:{cim_label('BaseVoltage')})
    RETURN
      elementId(vl) AS voltageLevelId,
      vl.`{cim_prop('IdentifiedObject.name')}` AS voltageLevelName,
      elementId(bv) AS baseVoltageId,
      bv.`{cim_prop('BaseVoltage.nominalVoltage')}` AS nominalVoltage
  UNION
    // Distribution: BaseVoltage from equipment in feeders
    WITH s
    MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
    MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
    MATCH (eq)-[:`{cim_prop('ConductingEquipment.BaseVoltage')}`]->(bv:{cim_label('BaseVoltage')})
    RETURN
      null AS voltageLevelId,
      null AS voltageLevelName,
      elementId(bv) AS baseVoltageId,
      bv.`{cim_prop('BaseVoltage.nominalVoltage')}` AS nominalVoltage
}}
RETURN DISTINCT
  voltageLevelId,
  voltageLevelName,
  baseVoltageId,
  nominalVoltage
ORDER BY nominalVoltage DESC
"""


def cypher_substation_topology(substation_name: str) -> str:
    """Cypher: Connectivity topology via feeders OR voltage-levels/bays."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_substation_equipment_cte()}
WITH DISTINCT eq, containerName
MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq)
MATCH (t)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn:{cim_label('ConnectivityNode')})
RETURN
  eq.`{cim_prop('IdentifiedObject.name')}`   AS equipmentName,
  [lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS equipmentType,
  containerName,
  t.`{cim_prop('IdentifiedObject.name')}`    AS terminalName,
  elementId(cn)                              AS connectivityNode,
  cn.`{cim_prop('IdentifiedObject.name')}`   AS cnName,
  t.`{cim_prop('ACDCTerminal.sequenceNumber')}` AS sequenceNumber
ORDER BY equipmentName, sequenceNumber
"""


def cypher_substation_feeders(substation_name: str) -> str:
    """Cypher: Feeders AND voltage levels in a substation with equipment counts."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
CALL {{
    // Feeders (distribution models)
    WITH s
    MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
    OPTIONAL MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
    OPTIONAL MATCH (eq)-[:`{cim_prop('ConductingEquipment.BaseVoltage')}`]->(bv:{cim_label('BaseVoltage')})
    WITH f AS container, 'Feeder' AS containerType,
         count(DISTINCT eq) AS equipmentCount,
         collect(DISTINCT bv.`{cim_prop('BaseVoltage.nominalVoltage')}`) AS voltages
    RETURN elementId(container) AS containerId,
           container.`{cim_prop('IdentifiedObject.name')}` AS name,
           containerType, equipmentCount, voltages
  UNION
    // Voltage Levels (CGMES models)
    WITH s
    MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
    OPTIONAL MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(vl)
    OPTIONAL MATCH (bay:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl)
    OPTIONAL MATCH (bayEq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay)
    OPTIONAL MATCH (vl)-[:`{cim_prop('VoltageLevel.BaseVoltage')}`]->(bv:{cim_label('BaseVoltage')})
    WITH vl AS container, 'VoltageLevel' AS containerType,
         count(DISTINCT eq) + count(DISTINCT bayEq) AS equipmentCount,
         collect(DISTINCT bv.`{cim_prop('BaseVoltage.nominalVoltage')}`) AS voltages
    RETURN elementId(container) AS containerId,
           container.`{cim_prop('IdentifiedObject.name')}` AS name,
           containerType, equipmentCount, voltages
}}
RETURN containerId, name, containerType, equipmentCount, voltages
ORDER BY name
"""


def cypher_list_feeders() -> str:
    """Cypher: List all feeders (mirrors GET /feeders)."""
    return f"""
MATCH (f)
WHERE '{cim_label("Feeder")}' IN labels(f) OR '{cim_label("Line")}' IN labels(f)
OPTIONAL MATCH (f)-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
RETURN
  elementId(f)           AS feeder,
  f.`{cim_prop('IdentifiedObject.name')}`  AS name,
  s.`{cim_prop('IdentifiedObject.name')}`  AS substationName
ORDER BY name
"""


def cypher_graph_stats() -> str:
    """Cypher: Node/relationship counts by label (mirrors GET /triplestore/stats)."""
    return """
CALL {
  MATCH (n) RETURN count(n) AS totalNodes
}
CALL {
  MATCH ()-[r]->() RETURN count(r) AS totalRelationships
}
RETURN totalNodes, totalRelationships
"""


def cypher_class_counts() -> str:
    """Cypher: Count nodes by CIM/Verance label (mirrors class distribution in triplestore/stats)."""
    return f"""
MATCH (n)
WHERE any(lbl IN labels(n) WHERE lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}')
UNWIND [lbl IN labels(n) WHERE (lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}') AND lbl <> 'Resource'] AS nsLabel
WITH replace(replace(nsLabel, '{CIM}', ''), '{VER}', '') AS type, count(*) AS count
RETURN type, count
ORDER BY count DESC
LIMIT 50
"""


def cypher_equipment_connected(equipment_name: str) -> str:
    """Cypher: All equipment directly connected to a named piece of equipment via Terminal→CN→Terminal traversal."""
    return f"""
MATCH (eq)
WHERE eq.`{cim_prop('IdentifiedObject.name')}` =~ $equipment_name
  AND any(lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
WITH eq
MATCH (t1:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq)
MATCH (t1)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn:{cim_label('ConnectivityNode')})
MATCH (t2:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn)
MATCH (t2)-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(neighbor)
WHERE eq <> neighbor
RETURN DISTINCT
  eq.`{cim_prop('IdentifiedObject.name')}` AS equipment,
  [lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS equipmentType,
  cn.`{cim_prop('IdentifiedObject.name')}` AS via_connectivity_node,
  neighbor.`{cim_prop('IdentifiedObject.name')}` AS connected_equipment,
  [lbl IN labels(neighbor) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS connected_type
ORDER BY connected_equipment
"""


def cypher_isolation_boundary(equipment_name: str) -> str:
    """Cypher: BFS outward from equipment, collecting switchable devices as the isolation boundary.

    Traverses through non-switchable equipment (busbars, CTs, VTs, line segments)
    and stops at any switchable device (Breaker, Disconnector, LoadBreakSwitch, Fuse, Recloser).
    Works for any topology: radial, ring, breaker-and-a-half, etc.
    """
    switchable = ", ".join([
        f"'{cim_label('Breaker')}'",
        f"'{cim_label('Disconnector')}'",
        f"'{cim_label('LoadBreakSwitch')}'",
        f"'{cim_label('Fuse')}'",
        f"'{cim_label('Recloser')}'",
    ])
    return f"""
// Find the target equipment
MATCH (start)
WHERE start.`{cim_prop('IdentifiedObject.name')}` =~ $equipment_name
  AND any(lbl IN labels(start) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
WITH start

// BFS: traverse Terminal→CN→Terminal→Equipment, collecting switches
MATCH path = (start)<-[:`{cim_prop('Terminal.ConductingEquipment')}`]-
             (:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConnectivityNode')}`]->
             (:{cim_label('ConnectivityNode')})
             <-[:`{cim_prop('Terminal.ConnectivityNode')}`]-
             (:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->
             (hop1)
WHERE hop1 <> start
WITH start, hop1,
     [lbl IN labels(hop1) WHERE lbl IN [{switchable}]] AS switchLabels

// If hop1 is a switch → it's a boundary device
// If hop1 is non-switchable → traverse one more hop to find switch behind it
WITH start, hop1, switchLabels,
     CASE WHEN size(switchLabels) > 0 THEN true ELSE false END AS isBoundary

// Collect direct boundary switches
WITH start,
     CASE WHEN isBoundary THEN hop1 ELSE null END AS boundarySwitch,
     CASE WHEN NOT isBoundary THEN hop1 ELSE null END AS passthrough
WITH start, collect(DISTINCT boundarySwitch) AS directBoundary, collect(DISTINCT passthrough) AS passthroughs

// For passthrough equipment, look one more hop
UNWIND (CASE WHEN size(passthroughs) = 0 THEN [null] ELSE passthroughs END) AS pt
OPTIONAL MATCH (pt)<-[:`{cim_prop('Terminal.ConductingEquipment')}`]-
               (:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConnectivityNode')}`]->
               (:{cim_label('ConnectivityNode')})
               <-[:`{cim_prop('Terminal.ConnectivityNode')}`]-
               (:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->
               (hop2)
WHERE hop2 <> start AND hop2 <> pt
  AND any(lbl IN labels(hop2) WHERE lbl IN [{switchable}])
WITH start, directBoundary, collect(DISTINCT hop2) AS indirectBoundary

// Combine all boundary switches
WITH start, directBoundary + indirectBoundary AS allBoundary
UNWIND allBoundary AS sw
WITH DISTINCT start, sw
WHERE sw IS NOT NULL
RETURN
  start.`{cim_prop('IdentifiedObject.name')}` AS equipment,
  [lbl IN labels(start) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS equipmentType,
  sw.`{cim_prop('IdentifiedObject.name')}` AS boundary_switch,
  [lbl IN labels(sw) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS switch_type,
  sw.`{cim_prop('Switch.normalOpen')}` AS normally_open
ORDER BY boundary_switch
"""


def cypher_network_summary() -> str:
    """Cypher: Per-substation summary with equipment counts, voltage levels, and transformer capacity."""
    return f"""
MATCH (s:{cim_label('Substation')})
OPTIONAL MATCH (s)-[:`{cim_prop('Substation.Region')}`]->(r)

// Equipment counts per substation (all containment paths)
CALL {{
    WITH s
    // Direct
    OPTIONAL MATCH (eq1)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(s)
    WHERE any(lbl IN labels(eq1) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
    WITH s, collect(DISTINCT eq1) AS direct
    // Via VoltageLevel
    OPTIONAL MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
    OPTIONAL MATCH (eq2)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(vl)
    WHERE any(lbl IN labels(eq2) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
      AND NOT eq2:{cim_label('Bay')}
    WITH s, direct, collect(DISTINCT eq2) AS vlEquip
    // Via Bay
    OPTIONAL MATCH (vl2:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
    OPTIONAL MATCH (bay:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl2)
    OPTIONAL MATCH (eq3)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay)
    WHERE any(lbl IN labels(eq3) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
    WITH s, direct, vlEquip, collect(DISTINCT eq3) AS bayEquip
    // Via Feeder
    OPTIONAL MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
    OPTIONAL MATCH (eq4)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
    WHERE any(lbl IN labels(eq4) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
    // Collect the aggregation first, then combine. Neo4j 5.24+ rejects mixing an
    // aggregation with grouping keys inside one expression.
    WITH s, direct, vlEquip, bayEquip, collect(DISTINCT eq4) AS feederEquip
    WITH s, direct + vlEquip + bayEquip + feederEquip AS allEquip
    UNWIND allEquip AS eq
    WITH DISTINCT s, eq
    WITH s, count(eq) AS totalEquipment,
         count(CASE WHEN eq:{cim_label('Breaker')} THEN 1 END) AS breakers,
         count(CASE WHEN eq:{cim_label('Disconnector')} THEN 1 END) AS disconnectors,
         count(CASE WHEN eq:{cim_label('PowerTransformer')} THEN 1 END) AS transformers,
         count(CASE WHEN eq:{cim_label('BusbarSection')} THEN 1 END) AS busbars,
         count(CASE WHEN eq:{cim_label('ACLineSegment')} THEN 1 END) AS lineSegments,
         count(CASE WHEN eq:{cim_label('EnergyConsumer')} THEN 1 END) AS loads,
         count(CASE WHEN eq:{cim_label('ProtectiveRelay')} THEN 1 END) AS protectionRelays
    // Do NOT return `s` — it is imported from the outer scope, and returning it
    // shadows the outer binding. Neo4j 5.24+ rejects that outright.
    RETURN totalEquipment, breakers, disconnectors, transformers, busbars,
           lineSegments, loads, protectionRelays
}}
WITH s, r, totalEquipment, breakers, disconnectors, transformers, busbars,
     lineSegments, loads, protectionRelays

// Voltage levels
OPTIONAL MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
OPTIONAL MATCH (vl)-[:`{cim_prop('VoltageLevel.BaseVoltage')}`]->(bv:{cim_label('BaseVoltage')})
WITH s, r, totalEquipment, breakers, disconnectors, transformers, busbars,
     lineSegments, loads, protectionRelays,
     collect(DISTINCT bv.`{cim_prop('BaseVoltage.nominalVoltage')}`) AS voltages

// Transformer capacity (sum of highest-voltage winding ratedS per transformer)
OPTIONAL MATCH (pt:{cim_label('PowerTransformer')})-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(s)
OPTIONAL MATCH (pte:{cim_label('PowerTransformerEnd')})-[:`{cim_prop('PowerTransformerEnd.PowerTransformer')}`]->(pt)
WHERE pte.`{cim_prop('TransformerEnd.endNumber')}` = 1
WITH s, r, totalEquipment, breakers, disconnectors, transformers, busbars,
     lineSegments, loads, protectionRelays, voltages,
     sum(CASE WHEN pte IS NOT NULL THEN toFloat(pte.`{cim_prop('PowerTransformerEnd.ratedS')}`) ELSE 0 END) AS totalMVA_direct

// Also check transformers in VoltageLevel containers
OPTIONAL MATCH (vl2:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
OPTIONAL MATCH (pt2:{cim_label('PowerTransformer')})-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(vl2)
OPTIONAL MATCH (pte2:{cim_label('PowerTransformerEnd')})-[:`{cim_prop('PowerTransformerEnd.PowerTransformer')}`]->(pt2)
WHERE pte2.`{cim_prop('TransformerEnd.endNumber')}` = 1
// Aggregate first, add second — same 5.24 restriction as above.
WITH s, r, totalEquipment, breakers, disconnectors, transformers, busbars,
     lineSegments, loads, protectionRelays, voltages, totalMVA_direct,
     sum(CASE WHEN pte2 IS NOT NULL THEN toFloat(pte2.`{cim_prop('PowerTransformerEnd.ratedS')}`) ELSE 0 END) AS totalMVA_vl
WITH s, r, totalEquipment, breakers, disconnectors, transformers, busbars,
     lineSegments, loads, protectionRelays, voltages,
     totalMVA_direct + totalMVA_vl AS totalMVA

RETURN
  s.`{cim_prop('IdentifiedObject.name')}` AS substation,
  r.`{cim_prop('IdentifiedObject.name')}` AS region,
  totalEquipment,
  breakers,
  disconnectors,
  transformers,
  busbars,
  lineSegments,
  loads,
  protectionRelays,
  voltages,
  totalMVA
ORDER BY substation
"""


def cypher_enhanced_topology(substation_name: str) -> str:
    """Cypher: Enhanced topology with busbar arrangement classification and breaker roles."""
    switchable = ", ".join([
        f"'{cim_label('Breaker')}'",
        f"'{cim_label('Disconnector')}'",
        f"'{cim_label('LoadBreakSwitch')}'",
    ])
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s

// Get voltage levels
MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
OPTIONAL MATCH (vl)-[:`{cim_prop('VoltageLevel.BaseVoltage')}`]->(bv:{cim_label('BaseVoltage')})
WITH s, vl, bv.`{cim_prop('BaseVoltage.nominalVoltage')}` AS nominalKV

// Count busbars per voltage level
OPTIONAL MATCH (bb:{cim_label('BusbarSection')})-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(vl)
WITH s, vl, nominalKV, collect(DISTINCT bb) AS busbars, count(DISTINCT bb) AS busbarCount

// Classify busbar arrangement
WITH s, vl, nominalKV, busbars, busbarCount,
     CASE
       WHEN busbarCount = 0 THEN 'no_busbars'
       WHEN busbarCount = 1 THEN 'single_bus'
       WHEN busbarCount = 2 THEN 'double_bus'
       ELSE 'multi_bus'
     END AS busbarArrangement

// Also check busbars in bays
OPTIONAL MATCH (bay:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl)
OPTIONAL MATCH (bb2:{cim_label('BusbarSection')})-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay)
WITH s, vl, nominalKV, busbars + collect(DISTINCT bb2) AS allBusbars,
     busbarCount + count(DISTINCT bb2) AS totalBusbars,
     CASE
       WHEN busbarCount + count(DISTINCT bb2) = 0 THEN 'no_busbars'
       WHEN busbarCount + count(DISTINCT bb2) = 1 THEN 'single_bus'
       WHEN busbarCount + count(DISTINCT bb2) = 2 THEN 'double_bus'
       ELSE 'multi_bus'
     END AS arrangement

// Classify breakers: bus_section (connected to 2+ busbars) vs feeder (connected to 1 busbar)
OPTIONAL MATCH (brk:{cim_label('Breaker')})-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(vl)
OPTIONAL MATCH (t1:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(brk)
OPTIONAL MATCH (t1)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn:{cim_label('ConnectivityNode')})
OPTIONAL MATCH (t2:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn)
OPTIONAL MATCH (t2)-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(connBB:{cim_label('BusbarSection')})

WITH s, vl, nominalKV, totalBusbars, arrangement,
     [bb IN allBusbars | bb.`{cim_prop('IdentifiedObject.name')}`] AS busbarNames,
     brk, count(DISTINCT connBB) AS connectedBusbars
WITH s, vl, nominalKV, totalBusbars, arrangement, busbarNames,
     collect(CASE WHEN brk IS NOT NULL THEN {{
       name: brk.`{cim_prop('IdentifiedObject.name')}`,
       normalOpen: brk.`{cim_prop('Switch.normalOpen')}`,
       role: CASE
         WHEN connectedBusbars >= 2 THEN 'bus_section'
         WHEN connectedBusbars = 1 THEN 'feeder'
         ELSE 'unclassified'
       END,
       connectedBusbars: connectedBusbars
     }} END) AS breakerClassification

// Also check breakers in bays
OPTIONAL MATCH (bay2:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl)
OPTIONAL MATCH (brk2:{cim_label('Breaker')})-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay2)
OPTIONAL MATCH (t3:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(brk2)
OPTIONAL MATCH (t3)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn2:{cim_label('ConnectivityNode')})
OPTIONAL MATCH (t4:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn2)
OPTIONAL MATCH (t4)-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(connBB2:{cim_label('BusbarSection')})

WITH s, vl, nominalKV, totalBusbars, arrangement, busbarNames, breakerClassification,
     brk2, count(DISTINCT connBB2) AS connectedBusbars2
WITH s, vl, nominalKV, totalBusbars, arrangement, busbarNames,
     breakerClassification + collect(CASE WHEN brk2 IS NOT NULL THEN {{
       name: brk2.`{cim_prop('IdentifiedObject.name')}`,
       normalOpen: brk2.`{cim_prop('Switch.normalOpen')}`,
       role: CASE
         WHEN connectedBusbars2 >= 2 THEN 'bus_section'
         WHEN connectedBusbars2 = 1 THEN 'feeder'
         ELSE 'unclassified'
       END,
       connectedBusbars: connectedBusbars2
     }} END) AS allBreakers

RETURN
  vl.`{cim_prop('IdentifiedObject.name')}` AS voltageLevel,
  nominalKV,
  totalBusbars,
  arrangement,
  busbarNames,
  [b IN allBreakers WHERE b IS NOT NULL AND b.name IS NOT NULL] AS breakers,
  size([b IN allBreakers WHERE b IS NOT NULL AND b.role = 'bus_section']) AS busSectionBreakers,
  size([b IN allBreakers WHERE b IS NOT NULL AND b.role = 'feeder']) AS feederBreakers
ORDER BY nominalKV DESC
"""


def cypher_connected_equipment(substation_name: str) -> str:
    """Cypher: Equipment connected via connectivity nodes (both containment paths)."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_substation_equipment_cte()}
WITH DISTINCT eq AS eq1, containerName
MATCH (t1:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq1)
MATCH (t1)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn)
MATCH (t2:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn)
MATCH (t2)-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq2)
WHERE eq1 <> eq2
RETURN DISTINCT
  eq1.`{cim_prop('IdentifiedObject.name')}` AS eq1Name,
  [lbl IN labels(eq1) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS eq1Type,
  eq1.`{cim_prop('IdentifiedObject.mRID')}` AS eq1MRID,
  cn.`{cim_prop('IdentifiedObject.name')}`  AS cnName,
  eq2.`{cim_prop('IdentifiedObject.name')}` AS eq2Name,
  [lbl IN labels(eq2) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS eq2Type,
  eq2.`{cim_prop('IdentifiedObject.mRID')}` AS eq2MRID
ORDER BY eq1Name, eq2Name
"""


# ── Demo/Latency Shortcuts ──────────────────────────────────────────────
# These collapse question shapes that previously forced the chat agent to
# author multi-hop Cypher at runtime (measured 8-65s) into a single
# pre-built endpoint call (measured 2-4s). See ROADMAP "chat latency".
#
# Scoping note: secondary-system devices (relays, RTUs, network gear) carry a
# denormalised `ns1__substationName` property, so they can be scoped directly
# without walking the 8-path containment CTE. Where that property is absent we
# fall back to the containment CTE.


def _catalog_joins(dev: str = "d", model: str = "dm", mfr: str = "mfr") -> str:
    """Hops to a device's model and manufacturer, collapsed to one node each.

    The reference catalog holds duplicate nodes for every model, manufacturer
    and ANSI function, so a bare OPTIONAL MATCH returns a row per duplicate
    and a per-device query silently returns two rows per device. Ordering by
    elementId makes the choice deterministic rather than merely arbitrary, so
    repeated calls agree with each other.

    Pass `mfr=None` when only the model is needed.
    """
    parts = [f"""
CALL {{
    WITH {dev}
    OPTIONAL MATCH ({dev})-[:`{VER}HAS_DEVICE_MODEL`]->(_dm)
    RETURN _dm AS {model} ORDER BY elementId(_dm) LIMIT 1
}}"""]
    if mfr:
        parts.append(f"""
CALL {{
    WITH {model}
    OPTIONAL MATCH ({model})-[:`{VER}MADE_BY`]->(_mfr)
    RETURN _mfr AS {mfr} ORDER BY elementId(_mfr) LIMIT 1
}}""")
    return "\n".join(parts)


def _ver_devices_by_substation(labels: List[str]) -> str:
    """Subquery: find ns1__/cim__ secondary-system devices belonging to a substation.

    Matches on the denormalised `ns1__substationName` property first (fast path),
    then unions the containment CTE result for devices that lack it.
    """
    label_list = ", ".join(f"'{lbl}'" for lbl in labels)
    return f"""
    CALL {{
        // Fast path: denormalised substation name on the device itself
        WITH s
        MATCH (d)
        WHERE any(lbl IN labels(d) WHERE lbl IN [{label_list}])
          AND d.`{VER}substationName` = s.`{cim_prop('IdentifiedObject.name')}`
        RETURN d
      UNION
        // Fallback: devices reachable through CIM containment
        WITH s
        {_substation_equipment_cte()}
        WITH eq AS d
        WHERE any(lbl IN labels(d) WHERE lbl IN [{label_list}])
        RETURN d
    }}
    """


def cypher_substation_summary(substation_name: str) -> str:
    """Cypher: one-shot substation profile — counts by type across all layers.

    Answers "what is this facility / what's in it / how many X and Y" without
    the agent having to author a query or make several calls.
    """
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_substation_equipment_cte()}
WITH s, eq
WITH s,
     [lbl IN labels(eq) WHERE (lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}')
        AND lbl <> 'Resource' | replace(replace(lbl, '{CIM}', ''), '{VER}', '')][0] AS type,
     eq
WHERE type IS NOT NULL
WITH s, type, count(DISTINCT eq) AS n
ORDER BY n DESC, type
WITH s, collect({{type: type, count: n}}) AS byType, sum(n) AS totalEquipment
OPTIONAL MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
OPTIONAL MATCH (vl)-[:`{cim_prop('VoltageLevel.BaseVoltage')}`]->(bv)
WITH s, byType, totalEquipment,
     collect(DISTINCT bv.`{cim_prop('BaseVoltage.nominalVoltage')}`) AS kvRaw
OPTIONAL MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
RETURN
  s.`{cim_prop('IdentifiedObject.name')}` AS substation,
  s.`{cim_prop('IdentifiedObject.mRID')}` AS mrid,
  totalEquipment,
  byType,
  [v IN kvRaw WHERE v IS NOT NULL] AS voltageLevelsKV,
  count(DISTINCT f) AS feederCount
"""


def cypher_substation_protection(substation_name: str) -> str:
    """Cypher: the protection layer — relays, models, ANSI functions, live telemetry.

    Replaces the agent-authored protection queries that measured 8-65s.
    """
    relay_labels = [cim_label("ProtectiveRelay"), f"{VER}ProtectiveRelay"]
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_ver_devices_by_substation(relay_labels)}
WITH DISTINCT d AS r
{_catalog_joins('r')}
OPTIONAL MATCH (dm)-[:`{VER}SUPPORTS_FUNCTION`]->(fn)
WITH r, dm, mfr,
     collect(DISTINCT CASE WHEN fn IS NULL THEN null ELSE
        {{code: fn.`{VER}code`, name: fn.`{VER}name`}} END) AS fns
RETURN
  r.`{cim_prop('IdentifiedObject.name')}` AS name,
  r.`{cim_prop('IdentifiedObject.mRID')}` AS mrid,
  coalesce(dm.`{VER}model_number`, r.`{VER}relay_model`, r.`scada__RELAY_MODEL`) AS model,
  coalesce(mfr.`{VER}name`, mfr.`{cim_prop('IdentifiedObject.name')}`) AS manufacturer,
  dm.`{VER}description` AS modelDescription,
  [f IN fns WHERE f IS NOT NULL | f.code] AS ansiFunctions,
  r.`{VER}ansi_functions` AS ansiFunctionsRaw,
  [k IN keys(r) WHERE k STARTS WITH 'scada__' | [k, r[k]]] AS scadaPairs
ORDER BY model, name
"""


def cypher_substation_scada(substation_name: str) -> str:
    """Cypher: the SCADA/control layer — RTUs, gateways, HMIs, historians, controllers."""
    scada_labels = [
        f"{VER}RemoteUnit", f"{VER}ControlCenter", f"{VER}HMI", f"{VER}Historian",
        f"{VER}PPC", f"{VER}BESSController", f"{VER}PortServer",
        cim_label("RemoteUnit"), cim_label("PortServer"),
    ]
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_ver_devices_by_substation(scada_labels)}
WITH DISTINCT d
{_catalog_joins('d')}
RETURN
  coalesce(d.`{cim_prop('IdentifiedObject.name')}`, d.`{VER}name`) AS name,
  [lbl IN labels(d) WHERE (lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}')
     AND lbl <> 'Resource' | replace(replace(lbl, '{CIM}', ''), '{VER}', '')][0] AS deviceType,
  dm.`{VER}model_number` AS model,
  coalesce(mfr.`{VER}name`, mfr.`{cim_prop('IdentifiedObject.name')}`) AS manufacturer,
  dm.`{VER}description` AS modelDescription,
  coalesce(d.`{VER}ip_address`, d.`{VER}ipAddress`, d.`{VER}RemoteUnit.ipAddress`) AS ipAddress,
  d.`{VER}protocol` AS protocol,
  [k IN keys(d) WHERE k STARTS WITH 'scada__' | [k, d[k]]] AS scadaPairs
ORDER BY deviceType, name
"""


def cypher_substation_network(substation_name: str) -> str:
    """Cypher: the communications layer — switches, routers, port servers, addressing."""
    net_labels = [
        cim_label("NetworkSwitch"), cim_label("Router"), cim_label("PortServer"),
        cim_label("Firewall"), f"{VER}NetworkSwitch", f"{VER}Router",
        f"{VER}PortServer", f"{VER}Firewall",
    ]
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
{_ver_devices_by_substation(net_labels)}
WITH DISTINCT d
{_catalog_joins('d')}
RETURN
  coalesce(d.`{cim_prop('IdentifiedObject.name')}`, d.`{VER}name`) AS name,
  [lbl IN labels(d) WHERE (lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}')
     AND lbl <> 'Resource' | replace(replace(lbl, '{CIM}', ''), '{VER}', '')][0] AS deviceType,
  dm.`{VER}model_number` AS model,
  coalesce(mfr.`{VER}name`, mfr.`{cim_prop('IdentifiedObject.name')}`) AS manufacturer,
  coalesce(d.`{VER}ip_address`, d.`{VER}ipAddress`) AS ipAddress,
  d.`{VER}protocol` AS protocol
ORDER BY deviceType, name
"""


# ── Cross-Layer Traversal ───────────────────────────────────────────────
# The graph carries first-class edges between layers — a relay is joined to the
# switch it protects by `cim__ProtectionEquipment.ProtectedSwitch`, and devices
# are joined to their comms peers by the protocol-link edges. Multi-dimensional
# questions are therefore short traversals, not the 8-path containment walks the
# agent writes when left to author its own Cypher. These builders expose those
# edges directly so a cross-layer question stays a single parameterised call.

PROTOCOL_LINKS = [
    "cim__DNP3Link", "cim__SELProtocolLink", "cim__ICCPLink",
    "cim__ModbusLink", "cim__SerialLink", "cim__EthernetLink",
]

# Protocol edge type → the protocol name an engineer would actually say.
PROTOCOL_NAMES = {
    "cim__DNP3Link": "DNP3",
    "cim__SELProtocolLink": "SEL Mirrored Bits",
    "cim__ICCPLink": "ICCP",
    "cim__ModbusLink": "Modbus",
    "cim__SerialLink": "Serial",
    "cim__EthernetLink": "Ethernet",
}


def _protocol_list() -> str:
    return ", ".join(f"'{t}'" for t in PROTOCOL_LINKS)


def _type_expr(var: str) -> str:
    """Cypher fragment: strip the namespace prefix off a node's primary label."""
    return (
        f"[lbl IN labels({var}) WHERE (lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}') "
        f"AND lbl <> 'Resource' | replace(replace(lbl, '{CIM}', ''), '{VER}', '')][0]"
    )


# Nodes that borrow an equipment name into their own label text — limit sets,
# diagram furniture, location points. A name lookup must never resolve to one.
_NAME_MATCH_DENY = [
    "OperationalLimitSet", "OperationalLimit", "CurrentLimit", "VoltageLimit",
    "ApparentPowerLimit", "DiagramObject", "DiagramObjectPoint", "Location",
    "PositionPoint", "CoordinateSystem", "TopologicalNode", "TopologicalIsland",
    "BaseVoltage", "PSRType", "OperationalLimitType",
    # Sub-components that carry their parent's name — "T3" means the
    # transformer, not its tap changer or one of its winding ends. Each is
    # still reachable through the transformer endpoints.
    "RatioTapChanger", "PhaseTapChanger", "TapChanger", "TapChangerControl",
    "TransformerEnd", "PowerTransformerEnd", "TransformerMeshImpedance",
    "TransformerCoreAdmittance", "RegulatingControl",
]


def _name_match_where(var: str) -> str:
    """Cypher predicate: `var` is a real device whose name matches $name-style param."""
    deny = " AND ".join(f"NOT {var}:`{CIM}{lbl}`" for lbl in _NAME_MATCH_DENY)
    return f"""any(lbl IN labels({var}) WHERE (lbl STARTS WITH '{CIM}' OR lbl STARTS WITH '{VER}') AND lbl <> 'Resource')
  AND {deny}"""


def _name_match_rank(var: str, param: str) -> str:
    """Cypher expression: 0 for an exact name, 1 for a prefix, 2 for anything else."""
    name = f"{var}.`{cim_prop('IdentifiedObject.name')}`"
    return f"""CASE
       WHEN toLower({name}) = toLower({param}) THEN 0
       WHEN toLower({name}) STARTS WITH toLower({param}) THEN 1
       ELSE 2
     END"""


def _name_match_tier(var: str) -> str:
    """Cypher expression: 0 if `var` is real contained equipment, else 1.

    Breaks ties between same-named nodes — the device that sits in an equipment
    container is the one an engineer means.
    """
    return f"""CASE WHEN EXISTS {{
       MATCH ({var})-[:`{cim_prop('Equipment.EquipmentContainer')}`]->()
     }} THEN 0 ELSE 1 END"""


def cypher_cross_layer(equipment_name: str) -> str:
    """Cypher: every layer touching one device, in a single query.

    Electrical neighbours, the relays protecting it, the SCADA devices that poll
    it, the comms gear carrying that traffic, and its live telemetry — each from
    the edge that actually models the relationship.
    """
    return f"""
MATCH (e)
WHERE e.`{cim_prop('IdentifiedObject.name')}` =~ $equipment_name
  AND {_name_match_where('e')}
WITH e, {_name_match_rank('e', '$equipment_raw')} AS rank, {_name_match_tier('e')} AS tier
ORDER BY rank, tier, e.`{cim_prop('IdentifiedObject.name')}`
WITH e LIMIT 1

// ── Electrical: direct connectivity ───────────────────────────────────
CALL {{
    WITH e
    OPTIONAL MATCH (e)-[:CONNECTED_TO]-(n)
    WHERE n <> e
    RETURN collect(DISTINCT CASE WHEN n IS NULL THEN null ELSE {{
        name: n.`{cim_prop('IdentifiedObject.name')}`,
        type: {_type_expr('n')}
    }} END) AS electricalRaw
}}

// ── Protection: relays bound to this device (edge points either way) ───
CALL {{
    WITH e
    OPTIONAL MATCH (r)-[:`{cim_prop('ProtectionEquipment.ProtectedSwitch')}`]-(e)
    OPTIONAL MATCH (r)-[:`{VER}HAS_DEVICE_MODEL`]->(rdm)
    RETURN collect(DISTINCT CASE WHEN r IS NULL THEN null ELSE {{
        name: r.`{cim_prop('IdentifiedObject.name')}`,
        type: {_type_expr('r')},
        model: coalesce(rdm.`{VER}model_number`, r.`{VER}relay_model`, r.`scada__RELAY_MODEL`),
        ansiFunctions: r.`{VER}ansi_functions`,
        mrid: r.`{cim_prop('IdentifiedObject.mRID')}`,
        scadaPairs: [k IN keys(r) WHERE k STARTS WITH 'scada__' | [k, r[k]]]
    }} END) AS protectionRaw
}}

// ── Protection, indirectly: relays on the adjacent switchgear ─────────
// CIM binds a relay to the switch it trips, not to the transformer or busbar
// behind it. "What protects T3" therefore means "what protects the breakers
// either side of T3", which is one more hop.
CALL {{
    WITH e
    OPTIONAL MATCH (e)-[:CONNECTED_TO]-(sw)
    WHERE any(lbl IN labels(sw) WHERE lbl IN [
        '{cim_label('Breaker')}', '{cim_label('Disconnector')}',
        '{cim_label('LoadBreakSwitch')}', '{cim_label('Recloser')}'
    ])
    OPTIONAL MATCH (r2)-[:`{cim_prop('ProtectionEquipment.ProtectedSwitch')}`]-(sw)
    OPTIONAL MATCH (r2)-[:`{VER}HAS_DEVICE_MODEL`]->(r2dm)
    RETURN collect(DISTINCT CASE WHEN r2 IS NULL THEN null ELSE {{
        relay: r2.`{cim_prop('IdentifiedObject.name')}`,
        model: coalesce(r2dm.`{VER}model_number`, r2.`{VER}relay_model`, r2.`scada__RELAY_MODEL`),
        ansiFunctions: r2.`{VER}ansi_functions`,
        viaSwitch: sw.`{cim_prop('IdentifiedObject.name')}`,
        mrid: r2.`{cim_prop('IdentifiedObject.mRID')}`,
        scadaPairs: [k IN keys(r2) WHERE k STARTS WITH 'scada__' | [k, r2[k]]]
    }} END) AS indirectRaw
}}

// ── Containing substation, when the device does not carry the name ─────
CALL {{
    WITH e
    OPTIONAL MATCH (e)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(c)
    OPTIONAL MATCH (c)-[:`{cim_prop('VoltageLevel.Substation')}`|`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(sub)
    RETURN coalesce(
        sub.`{cim_prop('IdentifiedObject.name')}`,
        c.`{cim_prop('IdentifiedObject.name')}`
    ) AS containerName
}}

// ── Comms: peers one protocol hop away, labelled with the protocol ─────
CALL {{
    WITH e
    OPTIONAL MATCH (e)-[l]-(peer)
    WHERE type(l) IN [{_protocol_list()}] AND peer <> e
    RETURN collect(DISTINCT CASE WHEN peer IS NULL THEN null ELSE {{
        name: coalesce(peer.`{cim_prop('IdentifiedObject.name')}`, peer.`{VER}name`),
        type: {_type_expr('peer')},
        link: type(l),
        ipAddress: coalesce(peer.`{VER}ip_address`, peer.`{VER}ipAddress`)
    }} END) AS commsRaw
}}

RETURN
  e.`{cim_prop('IdentifiedObject.name')}` AS equipment,
  {_type_expr('e')} AS equipmentType,
  e.`{cim_prop('IdentifiedObject.mRID')}` AS mrid,
  coalesce(e.`{VER}substationName`, containerName) AS substation,
  [x IN electricalRaw WHERE x IS NOT NULL] AS electrical,
  [x IN protectionRaw WHERE x IS NOT NULL] AS protection,
  [x IN indirectRaw WHERE x IS NOT NULL] AS protectionViaSwitchgear,
  [x IN commsRaw WHERE x IS NOT NULL] AS comms,
  [k IN keys(e) WHERE k STARTS WITH 'scada__' | [k, e[k]]] AS scadaPairs
"""


def cypher_protection_map(substation_name: str) -> str:
    """Cypher: which relay protects which device, across a whole substation.

    Answers "what protects what here" in one call, following the CIM
    ProtectedSwitch edge rather than inferring from names.
    """
    return f"""
MATCH (r)-[:`{cim_prop('ProtectionEquipment.ProtectedSwitch')}`]-(sw)
WHERE any(lbl IN labels(r) WHERE lbl CONTAINS 'ProtectiveRelay' OR lbl CONTAINS 'ProtectionEquipment')
  AND (
    r.`{VER}substationName` =~ $substation_name
    OR sw.`{VER}substationName` =~ $substation_name
    OR EXISTS {{
        MATCH (sw)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(c)
        WHERE c.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
    }}
  )
{_catalog_joins('r', mfr=None)}
RETURN
  r.`{cim_prop('IdentifiedObject.name')}` AS relay,
  coalesce(dm.`{VER}model_number`, r.`{VER}relay_model`, r.`scada__RELAY_MODEL`) AS relayModel,
  r.`{VER}ansi_functions` AS ansiFunctions,
  sw.`{cim_prop('IdentifiedObject.name')}` AS protects,
  {_type_expr('sw')} AS protectsType,
  r.`scada__COMM_STATUS` AS commStatus
ORDER BY relay, protects
"""


def cypher_comms_path(device_name: str, max_hops: int = 4) -> str:
    """Cypher: follow protocol links outward from a device to trace its comms path.

    "How does this relay report back to the control centre" — the answer is a
    path over DNP3/SEL/ICCP/Ethernet/Serial edges, which the graph models
    explicitly.

    Uses shortestPath rather than a variable-length walk: an unbounded walk over
    a densely linked comms segment returns hundreds of near-duplicate paths (one
    per ordering), which was both slow and unreadable. shortestPath yields one
    path per reachable endpoint, which is the answer anyone actually wants.
    """
    hops = max(1, min(int(max_hops), 6))
    alternation = "|".join(f"`{t}`" for t in PROTOCOL_LINKS)
    return f"""
MATCH (start)
WHERE start.`{cim_prop('IdentifiedObject.name')}` =~ $device_name
  AND {_name_match_where('start')}
WITH start, {_name_match_rank('start', '$device_raw')} AS rank, {_name_match_tier('start')} AS tier
ORDER BY rank, tier, start.`{cim_prop('IdentifiedObject.name')}`
WITH start LIMIT 1

MATCH path = shortestPath((start)-[:{alternation}*1..{hops}]-(endpoint))
WHERE endpoint <> start
WITH path, endpoint,
     [n IN nodes(path) | coalesce(n.`{cim_prop('IdentifiedObject.name')}`, n.`{VER}name`)] AS hopNames,
     [n IN nodes(path) | {_type_expr('n')}] AS hopTypes,
     [r IN relationships(path) | type(r)] AS linkTypes,
     length(path) AS hopCount,
     {_type_expr('endpoint')} AS endpointType

// Render the path as one readable line rather than three parallel arrays —
// far fewer tokens for a caller to read, and already in the shape of an answer.
WITH hopCount, endpointType, linkTypes,
     reduce(acc = '', i IN range(0, size(hopNames) - 1) |
       acc
       + CASE WHEN i = 0 THEN '' ELSE ' --[' + linkTypes[i - 1] + ']--> ' END
       + coalesce(hopNames[i], '?') + ' (' + coalesce(hopTypes[i], '?') + ')'
     ) AS route

// A "how does this report back" question means the control centre, the
// historian or the HMI — surface those destinations before intermediate hops.
WITH route, hopCount, endpointType, linkTypes,
     CASE WHEN endpointType IN ['ControlCenter', 'Historian', 'HMI'] THEN 0 ELSE 1 END AS destRank
RETURN route, hopCount, endpointType, linkTypes AS protocols
ORDER BY destRank, hopCount, endpointType
LIMIT 12
"""


# ── Fleet Scope ──────────────────────────────────────────────────────────
#
# Every endpoint above scopes to one substation and then describes it. Fleet
# queries invert that: start from an attribute, then find out which sites it
# lands in. The cost scales with the number of *matches* rather than the size
# of the fleet, because the starting set comes from a lookup and the site comes
# from following relationships rather than scanning every container.
#
# Where a dimension is already a node — device model, manufacturer, ANSI
# function — the query starts at that single node and walks its incoming
# edges. "Which sites run this relay model" is therefore cheaper than the
# per-substation endpoints it complements, not more expensive.
#
# Duplication note: the reference catalog in the current graph is double
# loaded — every manufacturer, model and ANSI function exists as two nodes.
# Joining through them naively multiplies rows, which silently inflates counts
# (99 breakers reported against 96 that exist). Every join below therefore
# collapses to one node deterministically, and the duplication is reported by
# /api/fleet/coverage rather than hidden. Fixing the data is an ingest
# concern; being wrong about a count is not something a report can afford to
# wait on.

# Node types worth reporting on across the fleet. An allowlist rather than a
# deny-list: terminals, diagram furniture, limit sets and position points
# outnumber real devices roughly four to one and would swamp every rollup.
FLEET_LABELS: List[str] = [
    # Primary electrical plant
    f"{CIM}Breaker", f"{CIM}Disconnector", f"{CIM}LoadBreakSwitch",
    f"{CIM}Recloser", f"{CIM}GroundDisconnector", f"{CIM}Jumper",
    f"{CIM}Switch", f"{CIM}PowerTransformer", f"{CIM}BusbarSection",
    f"{CIM}EnergyConsumer", f"{CIM}ACLineSegment",
    f"{CIM}PowerElectronicsConnection", f"{CIM}BatteryUnit",
    f"{CIM}PhotovoltaicUnit", f"{CIM}VsConverter",
    # Protection
    f"{CIM}ProtectiveRelay", f"{CIM}CurrentRelay", f"{VER}ProtectiveRelay",
    # SCADA, control and network
    f"{CIM}RemoteUnit", f"{CIM}PortServer",
    f"{VER}RemoteUnit", f"{VER}ControlCenter", f"{VER}HMI", f"{VER}Historian",
    f"{VER}PPC", f"{VER}BESSController", f"{VER}PortServer",
    f"{VER}NetworkSwitch", f"{VER}Router", f"{VER}Firewall",
]

# Dimensions a fleet rollup can pivot on. Each maps to a Cypher expression
# yielding a *list*, so a device with several ANSI functions counts once per
# function while a device with one model counts once.
FLEET_DIMENSIONS = ("model", "manufacturer", "type", "function", "firmware", "site")


def _fleet_label_filter(var: str = "d") -> str:
    labels = ", ".join(f"'{lbl}'" for lbl in FLEET_LABELS)
    return f"any(lbl IN labels({var}) WHERE lbl IN [{labels}])"


def _fleet_model_expr(dev: str = "d", model: str = "dm") -> str:
    """Model number, whether it came from a linked node or a raw string.

    Most of the catalog is normalised into `ns1__DeviceModel` nodes, but some
    devices carry the model only as text from SCADA or the editor. A rollup
    that read only the node would silently omit those.
    """
    return (f"coalesce({model}.`{VER}model_number`, {dev}.`{VER}relay_model`, "
            f"{dev}.`scada__RELAY_MODEL`)")


def _fleet_firmware_expr(dev: str = "d", fw: str = "fw") -> str:
    """Firmware version, from a linked node if present, else a property.

    Firmware is shared across many devices and is what advisories and upgrade
    campaigns pivot on, so it belongs on a node the way model does. The
    property fallbacks let manual entry and relay-config ingest work before
    that normalisation exists.
    """
    return (f"coalesce({fw}.`{VER}version`, {dev}.`{VER}firmware_version`, "
            f"{dev}.`{VER}firmware`, {dev}.`scada__FIRMWARE_VERSION`)")


def _fleet_mfr_expr(mfr: str = "mfr") -> str:
    return f"coalesce({mfr}.`{VER}name`, {mfr}.`{cim_prop('IdentifiedObject.name')}`)"


def _fleet_attribute_joins() -> str:
    """Hops to the nodes carrying a device's catalog attributes, one each.

    Each is a subquery ending in LIMIT 1 rather than a bare OPTIONAL MATCH.
    The catalog currently holds two nodes for every manufacturer, model and
    function, so a plain OPTIONAL MATCH returns a row per duplicate and every
    downstream count doubles. Collapsing here keeps one row per device no
    matter how many duplicate catalog nodes a device happens to reach.
    """
    return f"""
{_catalog_joins('d')}
CALL {{
    WITH d
    OPTIONAL MATCH (d)-[:`{VER}RUNS_FIRMWARE`]->(v)
    RETURN v AS fw ORDER BY v.`{VER}version` DESC, elementId(v) LIMIT 1
}}
"""


def _fleet_site_resolution(carry: str = "") -> str:
    """Cypher: resolve `d` to its substation, recording which path found it.

    Three paths, cheapest first:
      1. the denormalised `ns1__substationName` written at ingest
      2. the CIM containment chain (Bay / VoltageLevel / Feeder → Substation)
      3. for a relay, the switch it protects, then that switch's container

    `via` is carried out rather than discarded so an answer can state its own
    coverage. A device that resolves to nothing is a real finding — usually an
    editor artifact that was never bound to a site — and dropping it silently
    would understate every rollup built on top of this.

    `carry` is a comma-prefixed list of variables to keep in scope across the
    subqueries, e.g. ", dm, mfr, fw".
    """
    def up(var: str) -> str:
        return (f"-[:`{cim_prop('Bay.VoltageLevel')}`"
                f"|`{cim_prop('VoltageLevel.Substation')}`"
                f"|`{cim_prop('Feeder.NormalEnergizingSubstation')}`*0..2]->"
                f"({var}:{cim_label('Substation')})")

    name = cim_prop("IdentifiedObject.name")
    return f"""
CALL {{
    WITH d
    OPTIONAL MATCH (d)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(){up('subA')}
    RETURN subA.`{name}` AS viaContainer ORDER BY viaContainer LIMIT 1
}}
CALL {{
    WITH d
    OPTIONAL MATCH (d)-[:`{cim_prop('ProtectionEquipment.ProtectedSwitch')}`]-(psw)
                     -[:`{cim_prop('Equipment.EquipmentContainer')}`]->(){up('subB')}
    RETURN subB.`{name}` AS viaSwitch ORDER BY viaSwitch LIMIT 1
}}
WITH d{carry},
     coalesce(d.`{VER}substationName`, viaContainer, viaSwitch) AS site,
     CASE
       WHEN d.`{VER}substationName` IS NOT NULL THEN 'denormalized'
       WHEN viaContainer IS NOT NULL             THEN 'containment'
       WHEN viaSwitch    IS NOT NULL             THEN 'protected-switch'
       ELSE 'unresolved'
     END AS via
"""


def _fleet_filters() -> str:
    """WHERE clause: every filter optional, ANDed, case-insensitive substring.

    A null parameter means "do not filter on this", so one query serves
    "all SEL relays", "everything at Annfield" and "the whole fleet".
    """
    return f"""
WHERE ($model IS NULL OR toLower(coalesce({_fleet_model_expr()}, '')) CONTAINS toLower($model))
  AND ($manufacturer IS NULL OR toLower(coalesce({_fleet_mfr_expr()}, '')) CONTAINS toLower($manufacturer))
  AND ($firmware IS NULL OR toLower(coalesce({_fleet_firmware_expr()}, '')) CONTAINS toLower($firmware))
  AND ($type IS NULL OR toLower(coalesce({_type_expr('d')}, '')) CONTAINS toLower($type))
  AND ($function IS NULL OR EXISTS {{
        MATCH (dm)-[:`{VER}SUPPORTS_FUNCTION`]->(qf)
        WHERE toLower(coalesce(qf.`{VER}code`, '')) CONTAINS toLower($function)
           OR toLower(coalesce(qf.`{VER}name`, '')) CONTAINS toLower($function)
      }})
"""


def _fleet_functions_subquery() -> str:
    """ANSI codes for a device's model, deduplicated across duplicate nodes."""
    return f"""
CALL {{
    WITH dm
    OPTIONAL MATCH (dm)-[:`{VER}SUPPORTS_FUNCTION`]->(fn)
    RETURN [c IN collect(DISTINCT fn.`{VER}code`) WHERE c IS NOT NULL] AS ansiFunctions
}}
"""


def cypher_fleet_devices() -> str:
    """Cypher: devices across every site matching an attribute filter.

    The inversion the per-substation endpoints cannot express — "which sites
    have this" rather than "what is at this site". Telemetry is deliberately
    not returned: a fleet answer is an inventory, and pulling every device's
    SCADA points would make it an order of magnitude heavier for something no
    rollup uses.
    """
    name = cim_prop("IdentifiedObject.name")
    return f"""
MATCH (d)
WHERE {_fleet_label_filter()}
{_fleet_attribute_joins()}
WITH d, dm, mfr, fw
{_fleet_filters()}
{_fleet_site_resolution(", dm, mfr, fw")}
WITH d, dm, mfr, fw, site, via
WHERE ($site IS NULL OR toLower(coalesce(site, '')) CONTAINS toLower($site))
{_fleet_functions_subquery()}
RETURN
  d.`{name}`                              AS name,
  d.`{cim_prop('IdentifiedObject.mRID')}` AS mrid,
  {_type_expr('d')}                       AS type,
  site,
  via,
  {_fleet_model_expr()}                   AS model,
  {_fleet_mfr_expr()}                     AS manufacturer,
  {_fleet_firmware_expr()}                AS firmware,
  dm.`{VER}description`                   AS modelDescription,
  ansiFunctions
ORDER BY site, type, name
LIMIT $limit
"""


def cypher_fleet_inventory(dimension: str) -> str:
    """Cypher: fleet-wide counts for one dimension, broken out by site.

    The backbone of a standing report — how many of what, and where. Each
    dimension yields a list so a device with several ANSI functions is counted
    under each of them; every other dimension yields a single value.
    """
    if dimension not in FLEET_DIMENSIONS:
        raise ValueError(f"unknown dimension '{dimension}'")

    exprs = {
        "model":        f"[{_fleet_model_expr()}]",
        "manufacturer": f"[{_fleet_mfr_expr()}]",
        "type":         f"[{_type_expr('d')}]",
        "firmware":     f"[{_fleet_firmware_expr()}]",
        "site":         "[site]",
        "function":     "ansiFunctions",
    }
    return f"""
MATCH (d)
WHERE {_fleet_label_filter()}
{_fleet_attribute_joins()}
WITH d, dm, mfr, fw
{_fleet_filters()}
{_fleet_site_resolution(", dm, mfr, fw")}
WITH d, dm, mfr, fw, site, via
WHERE ($site IS NULL OR toLower(coalesce(site, '')) CONTAINS toLower($site))
{_fleet_functions_subquery()}
WITH d, site, {exprs[dimension]} AS vals
UNWIND (CASE WHEN vals IS NULL OR size(vals) = 0 THEN [null] ELSE vals END) AS value
WITH coalesce(value, '(not recorded)') AS value,
     coalesce(site, '(unassigned)')    AS site,
     count(DISTINCT d)                 AS devices
RETURN value, site, devices
ORDER BY value, site
"""


def cypher_fleet_models() -> str:
    """Cypher: the device model catalog, with where each model is actually used.

    Starts from the model node and walks its incoming edges — the cheap
    direction. Grouped by model *number* rather than by node, because the
    catalog holds duplicates; `catalogNodes` reports how many nodes collapsed
    into each row so the duplication stays visible.

    Models with no devices are returned too. An unused catalog entry is not an
    error, but a catalog that is mostly unused says the linking step of ingest
    has not kept up with it, which is worth seeing.
    """
    return f"""
MATCH (dm:`{VER}DeviceModel`)
CALL {{
    WITH dm
    OPTIONAL MATCH (dm)-[:`{VER}MADE_BY`]->(k)
    RETURN {_fleet_mfr_expr('k')} AS mfrName ORDER BY elementId(k) LIMIT 1
}}
CALL {{
    WITH dm
    OPTIONAL MATCH (dm)-[:`{VER}SUPPORTS_FUNCTION`]->(fn)
    RETURN [c IN collect(DISTINCT fn.`{VER}code`) WHERE c IS NOT NULL] AS fns
}}
OPTIONAL MATCH (dm)<-[:`{VER}HAS_DEVICE_MODEL`]-(d)
{_fleet_site_resolution(", dm, mfrName, fns")}
WITH
  dm.`{VER}model_number`                            AS model,
  head(collect(DISTINCT dm.`{VER}name`))            AS name,
  head(collect(DISTINCT dm.`{VER}description`))     AS description,
  head(collect(DISTINCT dm.`{VER}device_type`))     AS deviceType,
  head([n IN collect(mfrName) WHERE n IS NOT NULL]) AS manufacturer,
  coalesce(head([f IN collect(fns) WHERE size(f) > 0]), []) AS ansiFunctions,
  count(DISTINCT d)                                 AS deviceCount,
  count(DISTINCT dm)                                AS catalogNodes,
  [s IN collect(DISTINCT site) WHERE s IS NOT NULL] AS sites
RETURN model, name, description, deviceType, manufacturer,
       ansiFunctions, deviceCount, catalogNodes, sites
ORDER BY deviceCount DESC, model
"""


def cypher_fleet_search() -> str:
    """Cypher: open-ended lookup across device names, models and descriptions.

    The fallback for "find whatever" when no typed filter fits — the same role
    Custom_Cypher plays for the agent, but bounded to real devices and to
    fields worth searching. This is a scan, which is instant at a few thousand
    nodes; past roughly a hundred thousand the upgrade is a Neo4j full-text
    index over the same fields, which changes this query and nothing above it.
    """
    name = cim_prop("IdentifiedObject.name")
    haystack = (f"coalesce(d.`{name}`, '') + ' ' + coalesce({_fleet_model_expr()}, '') "
                f"+ ' ' + coalesce(dm.`{VER}description`, '') + ' ' "
                f"+ coalesce({_fleet_mfr_expr()}, '') + ' ' + coalesce({_type_expr('d')}, '')")
    return f"""
MATCH (d)
WHERE {_fleet_label_filter()}
{_fleet_attribute_joins()}
WITH d, dm, mfr, fw, {haystack} AS haystack
WHERE toLower(haystack) CONTAINS toLower($q)
{_fleet_site_resolution(", dm, mfr, fw")}
RETURN
  d.`{name}`                              AS name,
  d.`{cim_prop('IdentifiedObject.mRID')}` AS mrid,
  {_type_expr('d')}                       AS type,
  site,
  {_fleet_model_expr()}                   AS model,
  {_fleet_mfr_expr()}                     AS manufacturer,
  {_fleet_firmware_expr()}                AS firmware,
  CASE
    WHEN toLower(coalesce(d.`{name}`, '')) = toLower($q) THEN 0
    WHEN toLower(coalesce(d.`{name}`, '')) STARTS WITH toLower($q) THEN 1
    WHEN toLower(coalesce({_fleet_model_expr()}, '')) CONTAINS toLower($q) THEN 2
    ELSE 3
  END AS rank
ORDER BY rank, site, name
LIMIT $limit
"""


def cypher_fleet_coverage() -> str:
    """Cypher: how complete the fleet data actually is.

    A fleet answer is only as trustworthy as its weakest join, and the failure
    mode that matters is the silent one — a device that never appears in a
    rollup because nothing ties it to a site. This reports the resolution path
    taken for every device, names the ones that resolve to nothing, and counts
    how many carry each catalog attribute, so a report can state what it does
    not know rather than quietly rounding it off.
    """
    name = cim_prop("IdentifiedObject.name")
    return f"""
MATCH (d)
WHERE {_fleet_label_filter()}
{_fleet_attribute_joins()}
WITH d, dm, mfr, fw
{_fleet_site_resolution(", dm, mfr, fw")}
WITH d, dm, mfr, fw, site, via
WITH
  count(d)                                                   AS totalDevices,
  count(DISTINCT site)                                       AS sites,
  collect(CASE WHEN via = 'unresolved' THEN {{
      name: d.`{name}`, type: {_type_expr('d')},
      mrid: d.`{cim_prop('IdentifiedObject.mRID')}`
  }} END)                                                    AS unresolvedRaw,
  sum(CASE WHEN via = 'denormalized'     THEN 1 ELSE 0 END)  AS byDenormalized,
  sum(CASE WHEN via = 'containment'      THEN 1 ELSE 0 END)  AS byContainment,
  sum(CASE WHEN via = 'protected-switch' THEN 1 ELSE 0 END)  AS byProtectedSwitch,
  sum(CASE WHEN via = 'unresolved'       THEN 1 ELSE 0 END)  AS unresolvedCount,
  sum(CASE WHEN {_fleet_model_expr()}    IS NOT NULL THEN 1 ELSE 0 END) AS withModel,
  sum(CASE WHEN dm                       IS NOT NULL THEN 1 ELSE 0 END) AS withModelNode,
  sum(CASE WHEN mfr                      IS NOT NULL THEN 1 ELSE 0 END) AS withManufacturer,
  sum(CASE WHEN {_fleet_firmware_expr()} IS NOT NULL THEN 1 ELSE 0 END) AS withFirmware
RETURN
  totalDevices, sites,
  {{denormalized: byDenormalized, containment: byContainment,
    protectedSwitch: byProtectedSwitch, unresolved: unresolvedCount}} AS siteResolution,
  {{model: withModel, modelNode: withModelNode,
    manufacturer: withManufacturer, firmware: withFirmware}}          AS attributeCoverage,
  [u IN unresolvedRaw WHERE u IS NOT NULL]                            AS unresolvedDevices
"""


def cypher_fleet_catalog_health() -> str:
    """Cypher: duplication and usage across the reference catalog.

    Every join through the catalog is a place a count can double, so the
    difference between distinct values and stored nodes is worth reporting
    alongside any fleet number derived from them.
    """
    return f"""
CALL {{
    MATCH (dm:`{VER}DeviceModel`)
    OPTIONAL MATCH (dm)<-[:`{VER}HAS_DEVICE_MODEL`]-(d)
    WITH dm.`{VER}model_number` AS model, count(DISTINCT d) AS uses
    RETURN
      count(*)                                  AS catalogModels,
      sum(CASE WHEN uses > 0 THEN 1 ELSE 0 END) AS modelsInUse,
      sum(CASE WHEN uses = 0 THEN 1 ELSE 0 END) AS modelsUnlinked,
      sum(uses)                                 AS devicesLinked
}}
CALL {{
    MATCH (n:`{VER}DeviceModel`)
    WITH n.`{VER}model_number` AS v, count(*) AS c
    RETURN {{entity: 'DeviceModel', distinctValues: count(*), nodes: sum(c),
             duplicated: sum(CASE WHEN c > 1 THEN 1 ELSE 0 END)}} AS dupModels
}}
CALL {{
    MATCH (n:`{VER}Manufacturer`)
    WITH coalesce(n.`{VER}name`, n.`{cim_prop('IdentifiedObject.name')}`) AS v, count(*) AS c
    RETURN {{entity: 'Manufacturer', distinctValues: count(*), nodes: sum(c),
             duplicated: sum(CASE WHEN c > 1 THEN 1 ELSE 0 END)}} AS dupMfrs
}}
CALL {{
    MATCH (n:`{VER}ANSIFunction`)
    WITH n.`{VER}code` AS v, count(*) AS c
    RETURN {{entity: 'ANSIFunction', distinctValues: count(*), nodes: sum(c),
             duplicated: sum(CASE WHEN c > 1 THEN 1 ELSE 0 END)}} AS dupFns
}}
RETURN
  catalogModels, modelsInUse, modelsUnlinked, devicesLinked,
  [dupModels, dupMfrs, dupFns] AS duplication
"""
