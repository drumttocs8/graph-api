"""
Graph API Service — Neo4j REST API for CIM Queries

A standalone FastAPI service for querying CIM models in Neo4j (via n10s).
This is the Neo4j counterpart to CIMgraph API (which targets Blazegraph).

Features:
  - List all models / substations / feeders from Neo4j
  - Import CIM RDF/XML into Neo4j via n10s
  - Delete individual models or all data
  - D3.js / Mermaid visualization
  - Execute raw Cypher queries
  - Stats & health monitoring
"""
import os
import json
import re
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from neo4j_client import (
    get_async_driver, close_async_driver, execute_cypher_async, check_neo4j_async,
    cim_label, cim_prop, CIM,
    cypher_list_models, cypher_list_substations,
    cypher_substation_equipment, cypher_substation_transformers,
    cypher_substation_breakers, cypher_substation_voltage_levels,
    cypher_substation_topology, cypher_list_feeders,
    cypher_graph_stats, cypher_class_counts,
    cypher_connected_equipment,
    NEO4J_URI,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SERVICE_NAME = "Graph API Service"
VERSION = "1.0.0"

# Optional: Blazegraph URL for n10s cross-import
BLAZEGRAPH_URL = os.environ.get("BLAZEGRAPH_URL", "")


# ── Pydantic Models ──────────────────────────────────────────────────────

class CypherRequest(BaseModel):
    query: str
    parameters: Dict[str, Any] = {}


class ImportFromBGRequest(BaseModel):
    """Import CIM data from Blazegraph via n10s.rdf.import.fetch."""
    blazegraph_url: str = ""
    format: str = "Turtle"  # Turtle or RDF/XML


class ModelInfo(BaseModel):
    id: str
    name: str
    type: str
    node_count: int = 0


# ── Lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {SERVICE_NAME} v{VERSION}")
    logger.info(f"Neo4j URI: {NEO4J_URI}")
    # Warm up driver
    get_async_driver()
    yield
    await close_async_driver()
    logger.info("Shutting down Graph API Service")


# ── App ──────────────────────────────────────────────────────────────────

app = FastAPI(
    title=SERVICE_NAME,
    description="Model management and visualization for CIM data in Neo4j",
    version=VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files for admin UI
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ── Health & Status ──────────────────────────────────────────────────────

@app.get("/health")
async def health():
    neo4j_ok = await check_neo4j_async()
    return {
        "status": "healthy",
        "service": SERVICE_NAME,
        "version": VERSION,
        "neo4j_uri": NEO4J_URI,
        "neo4j_connected": neo4j_ok,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/")
async def root():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Graph API", "docs": "/docs"}


@app.get("/api/neo4j/status")
async def neo4j_status():
    ok = await check_neo4j_async()
    if ok:
        stats = await execute_cypher_async(cypher_graph_stats())
        return {
            "connected": True,
            "uri": NEO4J_URI,
            "totalNodes": stats[0]["totalNodes"] if stats else 0,
            "totalRelationships": stats[0]["totalRelationships"] if stats else 0,
        }
    return {"connected": False, "uri": NEO4J_URI}


# ── Models ───────────────────────────────────────────────────────────────

@app.get("/api/models")
async def list_models():
    """List all CIM feeder models in Neo4j, grouped by substation."""
    try:
        results = await execute_cypher_async(cypher_list_models())
        return {"success": True, "result_count": len(results), "models": results}
    except Exception as e:
        logger.error(f"Error listing models: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@app.get("/api/substations")
async def list_substations_endpoint():
    """List all substations."""
    try:
        results = await execute_cypher_async(cypher_list_substations())
        return {"success": True, "result_count": len(results), "substations": results}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/substations/{substation_name}/equipment")
async def get_equipment(substation_name: str):
    """All equipment in a substation's feeders."""
    try:
        pattern = f"(?i){re.escape(substation_name)}"
        results = await execute_cypher_async(
            cypher_substation_equipment(substation_name),
            {"substation_name": f"(?i).*{re.escape(substation_name)}.*"},
        )
        return {
            "success": True,
            "substation": substation_name,
            "result_count": len(results),
            "equipment": results,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/substations/{substation_name}/transformers")
async def get_transformers(substation_name: str):
    try:
        results = await execute_cypher_async(
            cypher_substation_transformers(substation_name),
            {"substation_name": f"(?i).*{re.escape(substation_name)}.*"},
        )
        return {
            "success": True,
            "substation": substation_name,
            "result_count": len(results),
            "transformers": results,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/substations/{substation_name}/breakers")
async def get_breakers(substation_name: str):
    try:
        results = await execute_cypher_async(
            cypher_substation_breakers(substation_name),
            {"substation_name": f"(?i).*{re.escape(substation_name)}.*"},
        )
        return {
            "success": True,
            "substation": substation_name,
            "result_count": len(results),
            "switches": results,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/substations/{substation_name}/voltage-levels")
async def get_voltage_levels(substation_name: str):
    try:
        results = await execute_cypher_async(
            cypher_substation_voltage_levels(substation_name),
            {"substation_name": f"(?i).*{re.escape(substation_name)}.*"},
        )
        return {
            "success": True,
            "substation": substation_name,
            "result_count": len(results),
            "voltage_levels": results,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/substations/{substation_name}/topology")
async def get_topology(substation_name: str):
    try:
        results = await execute_cypher_async(
            cypher_substation_topology(substation_name),
            {"substation_name": f"(?i).*{re.escape(substation_name)}.*"},
        )
        return {
            "success": True,
            "substation": substation_name,
            "result_count": len(results),
            "topology": results,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/substations/{substation_name}/connected-equipment")
async def get_connected_equipment(substation_name: str):
    try:
        results = await execute_cypher_async(
            cypher_connected_equipment(substation_name),
            {"substation_name": f"(?i).*{re.escape(substation_name)}.*"},
        )
        return {
            "success": True,
            "substation": substation_name,
            "result_count": len(results),
            "connections": results,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/feeders")
async def list_feeders():
    try:
        results = await execute_cypher_async(cypher_list_feeders())
        return {"success": True, "result_count": len(results), "feeders": results}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/stats")
async def graph_stats():
    """Node/relationship counts — equivalent of GET /triplestore/stats."""
    try:
        overview = await execute_cypher_async(cypher_graph_stats())
        classes = await execute_cypher_async(cypher_class_counts())
        row = overview[0] if overview else {}
        return {
            "success": True,
            "total_nodes": row.get("totalNodes", 0),
            "total_relationships": row.get("totalRelationships", 0),
            "top_classes": classes,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Import ───────────────────────────────────────────────────────────────

@app.post("/api/import/rdf")
async def import_rdf_file(file: UploadFile = File(...)):
    """Import an RDF/XML file into Neo4j via n10s.

    The file is written to Neo4j's import directory (or passed via inline)
    and loaded with n10s.rdf.import.inline or n10s.rdf.import.fetch.
    """
    try:
        content = await file.read()
        # Use n10s inline import (no filesystem mount needed)
        query = """
        CALL n10s.rdf.import.inline($payload, "RDF/XML", {verifyUriSyntax: false})
        YIELD terminationStatus, triplesLoaded, triplesParsed, namespaces, extraInfo
        RETURN terminationStatus, triplesLoaded, triplesParsed, namespaces, extraInfo
        """
        results = await execute_cypher_async(query, {"payload": content.decode("utf-8")})
        row = results[0] if results else {}
        return {
            "success": True,
            "filename": file.filename,
            "triples_loaded": row.get("triplesLoaded", 0),
            "triples_parsed": row.get("triplesParsed", 0),
            "status": row.get("terminationStatus", "unknown"),
        }
    except Exception as e:
        logger.error(f"RDF import failed: {e}", exc_info=True)
        raise HTTPException(500, f"Import failed: {e}")


@app.post("/api/import/from-blazegraph")
async def import_from_blazegraph(req: ImportFromBGRequest):
    """Pull CIM data from Blazegraph into Neo4j via n10s.rdf.import.fetch."""
    bg_url = req.blazegraph_url or BLAZEGRAPH_URL
    if not bg_url:
        raise HTTPException(400, "No Blazegraph URL provided or configured")

    sparql_endpoint = f"{bg_url}/namespace/kb/sparql"
    try:
        query = """
        CALL n10s.rdf.import.fetch(
            $endpoint,
            $format,
            {headerParams: {Accept: "text/turtle"}, verifyUriSyntax: false}
        )
        YIELD terminationStatus, triplesLoaded, triplesParsed, namespaces
        RETURN terminationStatus, triplesLoaded, triplesParsed, namespaces
        """
        results = await execute_cypher_async(
            query, {"endpoint": sparql_endpoint, "format": req.format}
        )
        row = results[0] if results else {}
        return {
            "success": True,
            "source": sparql_endpoint,
            "triples_loaded": row.get("triplesLoaded", 0),
            "triples_parsed": row.get("triplesParsed", 0),
            "status": row.get("terminationStatus", "unknown"),
        }
    except Exception as e:
        logger.error(f"Blazegraph import failed: {e}", exc_info=True)
        raise HTTPException(500, f"Import failed: {e}")


# ── Cypher passthrough ───────────────────────────────────────────────────

@app.post("/api/cypher")
async def execute_raw_cypher(req: CypherRequest):
    """Execute a raw Cypher query (escape hatch, like /custom-sparql)."""
    try:
        results = await execute_cypher_async(req.query, req.parameters)
        return {
            "success": True,
            "result_count": len(results),
            "results": results[:100],
        }
    except Exception as e:
        logger.error(f"Cypher execution failed: {e}", exc_info=True)
        raise HTTPException(500, f"Query failed: {e}")


# ── Delete / Clear ───────────────────────────────────────────────────────

@app.delete("/api/clear-all")
async def clear_all():
    """Delete all nodes and relationships (full reset)."""
    try:
        await execute_cypher_async("MATCH (n) DETACH DELETE n")
        # Re-init n10s config
        try:
            await execute_cypher_async("""
                CALL n10s.graphconfig.init({
                    handleVocabUris: "SHORTEN",
                    handleMultival: "ARRAY",
                    keepLangTag: true,
                    keepCustomDataTypes: true,
                    typesToLabels: true
                })
            """)
        except Exception:
            pass  # may already exist
        return {"success": True, "message": "All data cleared, n10s re-initialized"}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── D3.js Visualization ─────────────────────────────────────────────────

@app.get("/api/models/{model_id}/visualize/d3")
async def visualize_d3(model_id: str, max_nodes: int = Query(200, ge=1, le=5000)):
    """Return D3.js-compatible graph data for a model (substation or feeder)."""
    try:
        # Find the model node
        find_q = f"""
        MATCH (m)
        WHERE ('{cim_label("Feeder")}' IN labels(m) OR '{cim_label("Substation")}' IN labels(m))
          AND (elementId(m) = $model_id
               OR m.`{cim_prop('IdentifiedObject.mRID')}` = $model_id
               OR m.`{cim_prop('IdentifiedObject.name')}` = $model_id
               OR m.uri = $model_id)
        RETURN m, labels(m) AS labels
        LIMIT 1
        """
        found = await execute_cypher_async(find_q, {"model_id": model_id})
        if not found:
            raise HTTPException(404, f"Model not found: {model_id}")

        is_substation = cim_label("Substation") in found[0]["labels"]

        if is_substation:
            # Get equipment in substation → feeders
            graph_q = f"""
            MATCH (s:{cim_label('Substation')})
            WHERE elementId(s) = $model_id
               OR s.`{cim_prop('IdentifiedObject.mRID')}` = $model_id
               OR s.`{cim_prop('IdentifiedObject.name')}` = $model_id
               OR s.uri = $model_id
            WITH s
            MATCH (f:{cim_label('Feeder')})-[:`{cim_prop('Feeder.NormalEnergizingSubstation')}`]->(s)
            OPTIONAL MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
            WITH s, f, collect(DISTINCT eq)[0..$max_nodes] AS eqs
            UNWIND eqs AS eq
            OPTIONAL MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq)
            OPTIONAL MATCH (t)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn)
            RETURN s, f, eq, t, cn
            LIMIT $max_nodes
            """
        else:
            graph_q = f"""
            MATCH (f:{cim_label('Feeder')})
            WHERE elementId(f) = $model_id
               OR f.`{cim_prop('IdentifiedObject.mRID')}` = $model_id
               OR f.`{cim_prop('IdentifiedObject.name')}` = $model_id
               OR f.uri = $model_id
            WITH f
            OPTIONAL MATCH (eq)-[:`{cim_prop('Equipment.EquipmentContainer')}`]->(f)
            WITH f, collect(DISTINCT eq)[0..$max_nodes] AS eqs
            UNWIND eqs AS eq
            OPTIONAL MATCH (t:{cim_label('Terminal')})-[:`{cim_prop('Terminal.ConductingEquipment')}`]->(eq)
            OPTIONAL MATCH (t)-[:`{cim_prop('Terminal.ConnectivityNode')}`]->(cn)
            RETURN f, eq, t, cn
            LIMIT $max_nodes
            """

        records = await execute_cypher_async(graph_q, {"model_id": model_id, "max_nodes": max_nodes})

        # Build D3 node/edge sets
        nodes_map = {}
        edges = []

        def add_node(record_val, node_type: str):
            if record_val is None:
                return None
            nid = str(record_val.element_id) if hasattr(record_val, "element_id") else str(id(record_val))
            if nid not in nodes_map:
                name = ""
                if isinstance(record_val, dict):
                    name = record_val.get(cim_prop("IdentifiedObject.name"), nid)
                nodes_map[nid] = {"id": nid, "label": name, "type": node_type}
            return nid

        # Simplified: return the query results as flat structures
        d3_nodes = []
        d3_edges = []
        seen_ids = set()

        for rec in records:
            for key in rec:
                val = rec[key]
                if val and isinstance(val, dict) and "uri" in val:
                    nid = val.get("uri", str(id(val)))
                    if nid not in seen_ids:
                        seen_ids.add(nid)
                        name = val.get(cim_prop("IdentifiedObject.name"), "")
                        d3_nodes.append({"id": nid, "label": name})

        return {
            "success": True,
            "model_id": model_id,
            "nodes": d3_nodes[:max_nodes],
            "edges": d3_edges,
            "metadata": {"node_count": len(d3_nodes), "edge_count": len(d3_edges)},
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"D3 visualization failed: {e}", exc_info=True)
        raise HTTPException(500, str(e))
