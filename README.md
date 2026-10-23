# Graph API — Neo4j REST API for CIM Queries

A standalone FastAPI service for querying CIM/graph models in **Neo4j** (via n10s).
The Neo4j counterpart to CIMgraph API (which targets Blazegraph/SPARQL).

## Why Graph API?

CIMgraph API targets Blazegraph (SPARQL/RDF triplestore). Graph API targets Neo4j
(Cypher/labeled property graph). Both expose the same REST endpoints:

- List all models/substations/feeders
- Import CIM files (RDF/XML via n10s)
- Delete individual models or all data
- D3.js/Mermaid visualization
- Gephi export

## Architecture

```
┌──────────────────────┐
│  Graph API (this)    │  ← FastAPI + Neo4j Bolt driver
│  Port 8083           │
└──────────┬───────────┘
           │ Bolt (7687) / HTTP (7474)
           ▼
┌──────────────────────┐
│  Neo4j + n10s        │
│  CIM property graph  │
└──────────────────────┘
```

## Quick Start (local)

```bash
# Requires Neo4j running (see ../neo4j/)
pip install -r requirements.txt
cd app
uvicorn main:app --reload --port 8083
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | `bolt://neo4j.railway.internal:7687` | Neo4j Bolt endpoint |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `verance-ai-dev` | Neo4j password |
| `BLAZEGRAPH_URL` | (optional) | For CIM import from Blazegraph via n10s |
| `PORT` | `8083` | HTTP port |

## Railway Deployment

Internal URL: `http://graph-api.railway.internal:8080`

### Environment Variables (defaults work on Railway)
```
NEO4J_URI=bolt://neo4j.railway.internal:7687  (default)
NEO4J_USER=neo4j                               (default)
NEO4J_PASSWORD=verance-ai-dev                  (default)
PORT=8080                                       (Railway-injected)
```

## API Endpoints

### Health & Status
- `GET /health` — Service health + Neo4j connectivity
- `GET /api/neo4j/status` — Neo4j connection details

### Discovery
- `GET /api/models` — List all feeders/substations in Neo4j
- `GET /api/substations` — List all substations
- `GET /api/feeders` — List all feeders
- `GET /api/stats` — Node/relationship count by label

### Substation Detail (by name, case-insensitive)
- `GET /api/substations/{name}/equipment` — All equipment by type
- `GET /api/substations/{name}/transformers` — Power transformers + windings
- `GET /api/substations/{name}/breakers` — Breakers, disconnectors, switches
- `GET /api/substations/{name}/voltage-levels` — Voltage levels with kV ratings
- `GET /api/substations/{name}/topology` — Electrical connectivity
- `GET /api/substations/{name}/connected-equipment` — Equipment connection pairs

### Visualization
- `GET /api/models/{id}/visualize/d3` — D3.js graph data

### Import
- `POST /api/import/rdf` — Import RDF/XML file via n10s
- `POST /api/import/from-blazegraph` — Pull CIM from Blazegraph via n10s (migration)

### Management
- `DELETE /api/clear-all` — Clear entire graph + re-init n10s
- `POST /api/cypher` — Execute raw Cypher (escape hatch)
