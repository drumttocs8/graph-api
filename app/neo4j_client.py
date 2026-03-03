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


def cypher_substation_equipment(substation_name: str) -> str:
    """Cypher: Equipment in a substation via its feeders (mirrors GET /substations/:name/equipment)."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
WITH f, f.`{cim_prop('IdentifiedObject.name')}` AS feederName
MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
WHERE any(lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}')
RETURN
  elementId(eq)          AS equipment,
  eq.`{cim_prop('IdentifiedObject.name')}`  AS name,
  [lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS type,
  feederName
ORDER BY type, name
"""


def cypher_substation_transformers(substation_name: str) -> str:
    """Cypher: Transformers with winding details (mirrors GET /substations/:name/transformers)."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
WITH f, f.`{cim_prop('IdentifiedObject.name')}` AS feederName
MATCH (t:{cim_label('PowerTransformer')})-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
OPTIONAL MATCH (w:{cim_label('PowerTransformerEnd')})-[:`{cim_prop('PowerTransformerEnd.PowerTransformer')}`]->(t)
RETURN
  elementId(t)           AS transformer,
  t.`{cim_prop('IdentifiedObject.name')}`  AS name,
  feederName,
  w.`{cim_prop('IdentifiedObject.name')}`           AS windingName,
  w.`{cim_prop('PowerTransformerEnd.ratedU')}`       AS ratedU,
  w.`{cim_prop('PowerTransformerEnd.ratedS')}`       AS ratedS,
  w.`{cim_prop('PowerTransformerEnd.connectionKind')}` AS connectionKind,
  w.`{cim_prop('TransformerEnd.endNumber')}`         AS endNumber
ORDER BY name, endNumber
"""


def cypher_substation_breakers(substation_name: str) -> str:
    """Cypher: Breakers and switching devices (mirrors GET /substations/:name/breakers)."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
WITH f, f.`{cim_prop('IdentifiedObject.name')}` AS feederName
MATCH (sw)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
WHERE any(lbl IN labels(sw) WHERE lbl IN [
  '{cim_label("Breaker")}', '{cim_label("Disconnector")}',
  '{cim_label("LoadBreakSwitch")}', '{cim_label("Recloser")}', '{cim_label("Fuse")}'
])
RETURN
  elementId(sw)          AS switch,
  sw.`{cim_prop('IdentifiedObject.name')}`  AS name,
  [lbl IN labels(sw) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS type,
  feederName,
  sw.`{cim_prop('Switch.normalOpen')}`  AS normalOpen,
  sw.`{cim_prop('Switch.retained')}`    AS retained
ORDER BY type, name
"""


def cypher_substation_voltage_levels(substation_name: str) -> str:
    """Cypher: Base voltages in a substation's feeders (mirrors GET /substations/:name/voltage-levels)."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
MATCH (eq)-[:`{cim_prop('ConductingEquipment.BaseVoltage')}`]->(bv:{cim_label('BaseVoltage')})
RETURN DISTINCT
  elementId(bv)          AS baseVoltage,
  bv.`{cim_prop('BaseVoltage.nominalVoltage')}`  AS nominalVoltage
ORDER BY nominalVoltage DESC
"""


def cypher_substation_topology(substation_name: str) -> str:
    """Cypher: Connectivity topology (mirrors GET /substations/:name/topology)."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
WITH f, f.`{cim_prop('IdentifiedObject.name')}` AS feederName
MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq)
MATCH (t)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn:{cim_label('ConnectivityNode')})
RETURN
  eq.`{cim_prop('IdentifiedObject.name')}`   AS equipmentName,
  [lbl IN labels(eq) WHERE lbl STARTS WITH '{CIM}' AND lbl <> 'Resource' | replace(lbl, '{CIM}', '')][0] AS equipmentType,
  feederName,
  t.`{cim_prop('IdentifiedObject.name')}`    AS terminalName,
  elementId(cn)                              AS connectivityNode,
  cn.`{cim_prop('IdentifiedObject.name')}`   AS cnName,
  t.`{cim_prop('ACDCTerminal.sequenceNumber')}` AS sequenceNumber
ORDER BY equipmentName, sequenceNumber
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


def cypher_connected_equipment(substation_name: str) -> str:
    """Cypher: Equipment connected via connectivity nodes (mirrors /connected-equipment)."""
    return f"""
MATCH (s:{cim_label('Substation')})
WHERE s.`{cim_prop('IdentifiedObject.name')}` =~ $substation_name
WITH s
MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
MATCH (eq1)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
MATCH (t1:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq1)
MATCH (t1)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn)
MATCH (t2:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn)
MATCH (t2)-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq2)
WHERE eq1 <> eq2
MATCH (eq2)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
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
