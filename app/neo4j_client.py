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
    """
    return f"""
    CALL {{
        // Path 1: Equipment directly in Substation
        WITH s
        MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(s)
        WHERE any(lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
        RETURN eq, s.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 2: Equipment in Feeders (distribution models)
        WITH s
        MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
        MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
        WHERE any(lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
        RETURN eq, f.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 3: Equipment in Bays within VoltageLevels (CGMES models)
        WITH s
        MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
        MATCH (bay:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl)
        MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay)
        WHERE any(lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
        RETURN eq, vl.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 4: Equipment directly in VoltageLevels (no Bay)
        WITH s
        MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
        MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(vl)
        WHERE any(lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
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
        WHERE any(lbl IN labels(aux) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
        RETURN aux AS eq, vl.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 6: AuxiliaryEquipment via Terminal → ConductingEquipment in Feeders
        WITH s
        MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
        MATCH (ce)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
        MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(ce)
        MATCH (aux)-[:`{cim_prop('AuxiliaryEquipment.Terminal')}`]->(t)
        WHERE any(lbl IN labels(aux) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
        RETURN aux AS eq, f.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 7: Measurement nodes via Terminal → ConductingEquipment in substation
        WITH s
        MATCH (vl:{cim_label('VoltageLevel')})-[:`{cim_prop('VoltageLevel.Substation')}`]->(s)
        MATCH (bay:{cim_label('Bay')})-[:`{cim_prop('Bay.VoltageLevel')}`]->(vl)
        MATCH (ce)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(bay)
        MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(ce)
        MATCH (m)-[:`{cim_prop('Measurement.Terminal')}`]->(t)
        WHERE any(lbl IN labels(m) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
        RETURN m AS eq, vl.`{cim_prop('IdentifiedObject.name')}` AS containerName
      UNION
        // Path 8: Measurement nodes via Terminal → ConductingEquipment in Feeders
        WITH s
        MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
        MATCH (ce)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
        MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(ce)
        MATCH (m)-[:`{cim_prop('Measurement.Terminal')}`]->(t)
        WHERE any(lbl IN labels(m) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource')
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
  [lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS type,
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
  [lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS type,
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
    """Cypher: Count nodes by CIM label (mirrors class distribution in triplestore/stats)."""
    return f"""
MATCH (n)
WHERE any(lbl IN labels(n) WHERE lbl STARTS WITH '{CIM}')
UNWIND [lbl IN labels(n) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource'] AS cimLabel
WITH replace(cimLabel, '{CIM}', '') AS type, count(*) AS count
RETURN type, count
ORDER BY count DESC
LIMIT 30
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
    WITH s, direct + vlEquip + bayEquip + collect(DISTINCT eq4) AS allEquip
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
    RETURN s, totalEquipment, breakers, disconnectors, transformers, busbars,
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
WITH s, r, totalEquipment, breakers, disconnectors, transformers, busbars,
     lineSegments, loads, protectionRelays, voltages,
     totalMVA_direct + sum(CASE WHEN pte2 IS NOT NULL THEN toFloat(pte2.`{cim_prop('PowerTransformerEnd.ratedS')}`) ELSE 0 END) AS totalMVA

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
