# EKG ETL Pipeline

Enterprise Knowledge Graph ETL pipeline — incremental, multi-target, multi-cloud.

Extracts business data from relational source databases, transforms it into a
graph model driven entirely by metadata, and loads it into a supported graph
database. Designed for Scalefree's Data Vault-based EKG architecture.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Layout](#file-layout)
4. [Prerequisites](#prerequisites)
5. [Installation](#installation)
6. [Database Setup](#database-setup)
   - [Meta Database](#meta-database)
   - [Run Database](#run-database)
7. [Configuration](#configuration)
   - [Tenant Identity](#tenant-identity)
   - [Credential Resolution](#credential-resolution)
   - [INI File Credentials](#ini-file-credentials)
   - [Graph Target](#graph-target)
   - [Logging Targets](#logging-targets)
8. [Metadata Model](#metadata-model)
   - [data_source](#data_source)
   - [source_table](#source_table)
   - [graph_entity](#graph_entity)
   - [entity_mapping](#entity_mapping)
   - [attribute_mapping](#attribute_mapping)
   - [concept_tag](#concept_tag)
   - [entity_tag](#entity_tag)
   - [control_sdts](#control_sdts)
9. [Run Modes](#run-modes)
   - [Full Load](#full-load)
   - [Incremental Load](#incremental-load)
   - [Change Detection Modes](#change-detection-modes)
10. [Concept Tags and Classification](#concept-tags-and-classification)
    - [Node-Level Tags](#node-level-tags)
    - [Attribute-Level Classification](#attribute-level-classification)
11. [Graph Targets](#graph-targets)
    - [AWS Neptune](#aws-neptune)
    - [Neo4j](#neo4j)
    - [Azure Cosmos DB](#azure-cosmos-db)
    - [Google Spanner Graph](#google-spanner-graph)
12. [Secret Providers](#secret-providers)
13. [Logging Targets](#logging-targets-reference)
14. [SQL Registry (queries.ini)](#sql-registry-queriesini)
15. [Environment Variable Reference](#environment-variable-reference)
16. [Adding a New Graph Target](#adding-a-new-graph-target)
17. [Adding a New Secret Provider](#adding-a-new-secret-provider)
18. [Security Considerations](#security-considerations)
19. [Troubleshooting](#troubleshooting)

---

## Overview

The EKG ETL pipeline is a Python-based, metadata-driven loader that:

- Reads mapping configuration from a **read-only meta PostgreSQL database**
- Reads and writes operational state (watermarks, row hashes, audit logs) to a **separate run PostgreSQL database**
- Extracts source data from any **DB-API 2.0 compatible database** (PostgreSQL, SQL Server, Oracle, etc.)
- Writes nodes, edges, concept tags, and TAGGED_AS relationships to a **graph database**
- Supports **full loads** (initial or rebuild) and **incremental loads** (change detection)
- Is fully **multi-tenant** via `gsr_client_id` / `gsr_inst_id` scoping
- Is scoped to a **snapshot timestamp** (`gsr_sdts`) from the `control_sdts` table

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ekg_etl.py                               │
│                   EKGETLPipeline                                │
│                                                                 │
│  MetaRepository ──► meta DB (read-only PostgreSQL)              │
│  RunRepository  ──► run  DB (read-write PostgreSQL)             │
│  SourceConnectionFactory ──► business DBs (DB-API 2.0)          │
│  GraphTarget    ──► graph DB (Neptune/Neo4j/Cosmos/Spanner)     │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
   secrets/             logging_/              graph/
  ┌──────────┐        ┌──────────────┐      ┌──────────────┐
  │ aws      │        │ cloudwatch   │      │ neptune      │
  │ azure    │        │ azure_monitor│      │ neo4j        │
  │ google   │        │ google_log   │      │ cosmos       │
  │ vault    │        │ console      │      │ spanner      │
  │ ini_file │        └──────────────┘      └──────────────┘
  │ env      │
  └──────────┘
```

**Meta database** is read-only for the pipeline. All mapping definitions,
entity types, attribute mappings, and concept tags live here. It is shared
across tenants; tenant isolation is enforced by `gsr_client_id` and
`gsr_inst_id` filters baked into every query via CTE fragments.

**Run database** is the pipeline's writable state store. It tracks load
runs, per-mapping statistics, watermarks (for timestamp-mode incremental
loads), and row hashes (for hash-mode change detection).

**queries.ini** is the SQL registry. Every SQL statement the pipeline
executes is defined there. CTE fragments for meta tables are assembled
into full queries at runtime by `SqlRegistry` — no SQL is hardcoded in
Python.

---

## File Layout

```
ekg_etl/
├── ekg_etl.py                  # Pipeline orchestrator — the only file you run
├── queries.ini                 # All SQL statements (CTE fragments + queries)
├── requirements.txt            # Python dependencies
├── credentials.ini.example     # Template for INI-file credential store
├── meta_schema.sql             # DDL for the meta (read-only) database
├── run_schema.sql              # DDL for the run (read-write) database
│
├── graph/                      # Graph target abstraction layer
│   ├── __init__.py             # Factory: create_graph_target(name, config)
│   ├── base.py                 # GraphTarget ABC + BulkRecord + GraphTargetConfig
│   ├── neptune.py              # AWS Neptune (Gremlin HTTP + S3 Bulk Loader)
│   ├── neo4j.py                # Neo4j (Bolt driver)
│   ├── cosmos.py               # Azure Cosmos DB for Apache Gremlin
│   └── spanner.py              # Google Spanner Graph
│
├── secrets/                    # Credential resolution abstraction layer
│   ├── __init__.py             # Exports build_secrets_chain()
│   ├── base.py                 # SecretsProvider ABC + ChainedSecretsProvider
│   ├── aws.py                  # AWS Secrets Manager
│   ├── azure.py                # Azure Key Vault
│   ├── google.py               # Google Secret Manager
│   ├── vault.py                # HashiCorp Vault (KV v2)
│   ├── ini_file.py             # INI file provider
│   └── env.py                  # Environment variable provider (fallback)
│
└── logging_/                   # Log handler abstraction layer
    ├── __init__.py             # Exports configure_logging()
    ├── builder.py              # LoggingBuilder — assembles handlers from config
    ├── cloudwatch.py           # AWS CloudWatch Logs
    ├── azure_monitor.py        # Azure Monitor / Application Insights
    └── google_logging.py       # Google Cloud Logging
```

---

## Prerequisites

- Python 3.9 or later
- Two PostgreSQL databases: one for **meta** (read-only), one for **run** (read-write)
- A supported graph database (Neptune, Neo4j, Cosmos DB, or Spanner)
- Network access from the pipeline host to all databases

---

## Installation

```bash
# Clone or copy the ekg_etl/ directory to your host

cd ekg_etl

# Install all dependencies
pip install -r requirements.txt

# Or install only what you need (see requirements.txt comments)
pip install psycopg2-binary requests neo4j   # minimal: Neo4j + console logging
```

---

## Database Setup

### Meta Database

Apply the DDL to your meta PostgreSQL database:

```bash
psql -h <meta-host> -U <admin-user> -d <meta-db> -f meta_schema.sql
```

This creates the `meta` schema and all reference tables. The pipeline
user needs only `SELECT` on all tables in this schema:

```sql
GRANT USAGE  ON SCHEMA meta TO ekg_meta_user;
GRANT SELECT ON ALL TABLES IN SCHEMA meta TO ekg_meta_user;
```

### Run Database

Apply the DDL to your run PostgreSQL database:

```bash
psql -h <run-host> -U <admin-user> -d <run-db> -f run_schema.sql
```

The pipeline user needs full DML access:

```sql
GRANT USAGE  ON SCHEMA run TO ekg_run_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA run TO ekg_run_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA run TO ekg_run_user;
```

---

## Configuration

All configuration is passed via environment variables. No configuration
file is required beyond `credentials.ini` for credentials (optional).

### Tenant Identity

Every pipeline run is scoped to a single tenant, identified by two UUIDs.
These are **mandatory** — the pipeline will not start without them.

```bash
export GSR_CLIENT_ID="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
export GSR_INST_ID="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
```

`gsr_client_id` identifies the client organisation.
`gsr_inst_id` identifies the specific instance or environment (e.g. production vs. staging).

Both values are injected into every meta query as CTE-level filters,
ensuring complete tenant isolation even in a shared meta database.

### Credential Resolution

The pipeline resolves credentials through an ordered **secrets chain**.
Each provider is tried in sequence; the first to return a result wins.
Environment variables are always the final fallback.

Configure the chain via:

```bash
export EKG_SECRET_PROVIDERS="aws,vault,ini,env"   # recommended for production
export EKG_SECRET_PROVIDERS="ini,env"             # local development
export EKG_SECRET_PROVIDERS="env"                 # CI / container environments
```

| Provider | Name  | Description |
|----------|-------|-------------|
| AWS Secrets Manager | `aws` | Secret name or ARN as ref |
| Azure Key Vault | `azure` | Secret name within configured vault |
| Google Secret Manager | `google` | Short name or full resource path |
| HashiCorp Vault | `vault` | KV v2 path |
| INI file | `ini` | Section name in `credentials.ini` |
| Environment variables | `env` | `<PREFIX>_USERNAME` / `<PREFIX>_PASSWORD` |

`env` is always appended at the end if not listed explicitly.

Each secret reference (`secret_ref`) is a string stored in
`meta.data_source.secret_ref` and in the pipeline's `DBConfig`. The
same string is used as the lookup key across all providers.

### INI File Credentials

Copy and edit the example file:

```bash
cp credentials.ini.example credentials.ini
chmod 600 credentials.ini   # mandatory — file contains passwords
```

The file uses section names that match secret refs:

```ini
[prod/meta-db]
username = meta_user
password = s3cr3t
host     = meta-db.internal
port     = 5432
dbname   = meta

[prod/run-db]
username = run_user
password = s3cr3t
host     = run-db.internal
port     = 5432
dbname   = run

[prod/ekg-target]
username = neo4j
password = bolt_password
endpoint = bolt://neo4j.internal:7687
```

File path resolution (first existing file wins):

1. `EKG_CREDENTIALS_FILE` environment variable
2. `./credentials.ini` (current working directory)
3. `~/.ekg/credentials.ini` (user home directory)

The pipeline logs a `WARNING` if the file is group- or world-readable
and prints the exact `chmod 600` command to fix it.

### Graph Target

```bash
export EKG_GRAPH_TARGET="neo4j"            # neptune | neo4j | cosmos | spanner
export EKG_TARGET_SECRET_REF="prod/ekg-target"
export EKG_TARGET_ENDPOINT="bolt://neo4j.internal:7687"
```

Target-specific options are documented in each `graph/*.py` file and
the [Graph Targets](#graph-targets) section below.

### Logging Targets

```bash
export EKG_LOG_TARGETS="console,cloudwatch"    # comma-separated
export EKG_LOG_GROUP="/ekg-etl"
export EKG_LOG_STREAM="run-20240101"
```

Console is always added when no cloud handler installs successfully.

---

## Metadata Model

All mapping configuration lives in the **meta database**. Every table
is tenant-scoped via `gsr_client_id` + `gsr_inst_id` (except
`data_source`, `source_table`, and `graph_entity` which are shared
infrastructure).

### data_source

One row per business database the pipeline reads from.

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | UUID PK | Auto-generated |
| `source_name` | VARCHAR | Unique human name |
| `driver_module` | VARCHAR | Python DB-API 2.0 module (e.g. `psycopg2`, `pyodbc`) |
| `host` | VARCHAR | Database host |
| `port` | INTEGER | Database port |
| `database_name` | VARCHAR | Database / catalog name |
| `secret_ref` | VARCHAR | Credential reference for the secrets chain |
| `extra_params` | JSONB | Additional `driver.connect()` kwargs |
| `is_active` | BOOLEAN | Soft-disable without deleting |

### source_table

A table, view, or SQL query within a data source.

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | UUID PK | Auto-generated |
| `source_id` | UUID FK | Parent data source |
| `schema_name` | VARCHAR | Source schema (optional) |
| `table_name` | VARCHAR | Source table or view name |
| `override_query` | TEXT | Full SQL replacing `SELECT * FROM table` |
| `partition_column` | VARCHAR | Numeric/date column for range partitioning |
| `partition_size` | INTEGER | Rows per partition chunk (default 100,000) |

### graph_entity

Declares a Neptune/graph node label or edge label.

| Column | Type | Description |
|--------|------|-------------|
| `entity_id` | UUID PK | Auto-generated |
| `entity_name` | VARCHAR | Graph label (e.g. `Customer`, `PLACED_ORDER`) |
| `entity_type` | VARCHAR | `node` or `edge` |
| `from_node_label` | VARCHAR | Source node label (edges only) |
| `to_node_label` | VARCHAR | Target node label (edges only) |

### entity_mapping

Links a source table to a graph entity. One row = one mapping.

| Column | Type | Description |
|--------|------|-------------|
| `mapping_id` | UUID PK | Auto-generated |
| `gsr_client_id` | UUID | Tenant client — all queries filter on this |
| `gsr_inst_id` | UUID | Tenant instance — all queries filter on this |
| `table_id` | UUID FK | Source table |
| `entity_id` | UUID FK | Target graph entity |
| `id_column_expr` | VARCHAR | Python eval expression producing `~id` from source row. Column names are local variables. Example: `"'Cust_' + str(customer_id)"` |
| `from_id_column_expr` | VARCHAR | Expression for edge `~from` node ID (edges only) |
| `to_id_column_expr` | VARCHAR | Expression for edge `~to` node ID (edges only) |
| `row_filter` | TEXT | Optional SQL `WHERE` fragment appended to base query |
| `change_mode` | VARCHAR | `timestamp`, `hash`, or `full` — see [Change Detection](#change-detection-modes) |
| `watermark_column` | VARCHAR | Source timestamp column (timestamp mode only) |
| `delete_flag_column` | VARCHAR | Column/expression truthy for logically deleted rows (timestamp mode) |
| `hash_columns` | TEXT[] | Columns included in SHA-256 hash; NULL = all attribute columns |
| `load_order` | INTEGER | Processing order; nodes must have lower values than their edges |

### attribute_mapping

Maps one source column to one graph property. Multiple rows per mapping.

| Column | Type | Description |
|--------|------|-------------|
| `attr_id` | UUID PK | Auto-generated |
| `gsr_client_id` | UUID | Tenant client |
| `gsr_inst_id` | UUID | Tenant instance |
| `mapping_id` | UUID FK | Parent entity mapping |
| `source_column` | VARCHAR | Source column name or SQL expression |
| `target_property` | VARCHAR | Graph property key name |
| `data_type` | VARCHAR | Neptune type: `String`, `Int`, `Long`, `Float`, `Double`, `Bool`, `Date`, `DateTime`, `Byte`, `Short` |
| `nullable` | BOOLEAN | Whether NULL source values are allowed |
| `transform_expr` | TEXT | Optional Python eval expression applied to raw value. Variable `value` holds the input. Example: `"value.strip().upper()"` |
| `privacy_class` | VARCHAR | Attribute-level privacy classification (e.g. `PII`, `QUASI_PII`, `SENSITIVE`, `NON_PII`). Emitted as `<property>__privacy_class` on the graph node. |
| `security_class` | VARCHAR | Attribute-level security classification (e.g. `PUBLIC`, `INTERNAL`, `CONFIDENTIAL`, `SECRET`). Emitted as `<property>__security_class` on the graph node. |

### concept_tag

Controlled vocabulary of node-level tags, grouped by category.

| Column | Type | Description |
|--------|------|-------------|
| `tag_id` | UUID PK | Auto-generated; used as graph node `~id` |
| `gsr_client_id` | UUID | Tenant client |
| `gsr_inst_id` | UUID | Tenant instance |
| `tag_name` | VARCHAR | Short machine-friendly name (e.g. `PII`, `Finance`, `Person`) |
| `tag_category` | VARCHAR | Classification axis (e.g. `business_domain`, `ontology_class`, `data_classification`) |
| `display_name` | VARCHAR | Human-readable label for UIs; defaults to `tag_name` |

### entity_tag

Assigns concept tags to entity mappings.

| Column | Type | Description |
|--------|------|-------------|
| `entity_tag_id` | UUID PK | Auto-generated |
| `gsr_client_id` | UUID | Tenant client |
| `gsr_inst_id` | UUID | Tenant instance |
| `mapping_id` | UUID FK | Entity mapping whose nodes receive this tag |
| `tag_id` | UUID FK | The concept tag to attach |

All nodes produced by a tagged mapping receive a `TAGGED_AS` edge
pointing to the corresponding `:ConceptTag` node in the graph.

### control_sdts

Snapshot control table. Exactly one active row per tenant determines
the current snapshot timestamp used to scope all source data reads.

| Column | Type | Description |
|--------|------|-------------|
| `gsr_client_id` | UUID | Tenant client |
| `gsr_inst_id` | UUID | Tenant instance |
| `gsr_sdts` | TIMESTAMP | Current snapshot datetime (UTC) |
| `is_active` | BOOLEAN | Exactly one row per tenant should be TRUE |

The pipeline fetches `gsr_sdts` once at startup and fails immediately
if no active row exists for the current tenant.

---

## Run Modes

### Full Load

Reads all source rows, writes them to the graph using each target's
bulk-load mechanism, then resets all watermarks and row hashes so the
next incremental run starts from a clean baseline.

```bash
python ekg_etl.py --mode full
```

Load order:
1. `:ConceptTag` nodes (always first)
2. Node entity mappings (in `load_order` sequence)
3. Edge entity mappings (in `load_order` sequence)

### Incremental Load

Detects changed rows since the last run and applies only the delta
to the graph. This is the normal scheduled operation.

```bash
python ekg_etl.py --mode incremental
```

Nodes are always processed before edges (enforced by `load_order`).
Concept tag nodes are upserted at startup before any data nodes.

### Change Detection Modes

Set per mapping via `entity_mapping.change_mode`.

#### `timestamp`

Extracts rows where `watermark_column > last_watermark`. The watermark
is stored in `run.table_watermark` and committed only after successful
graph writes (two-phase commit prevents data loss on interruption).

Deleted rows: detected only when `delete_flag_column` is set. Rows where
the flag expression is truthy are hard-deleted from the graph.

Best for: tables with a reliable `updated_at` column.

#### `hash`

Performs a full scan, computes a SHA-256 fingerprint of the mapped
columns for each row, and compares against the stored fingerprint in
`run.row_hash`.

- **Inserts / updates**: rows with a changed or absent hash → upserted
- **Deletes**: IDs present in the hash store but absent from the current
  scan → hard-deleted from the graph and removed from the hash store

Best for: tables without a watermark column, or where delete detection
without a flag column is required.

#### `full`

Always re-upserts every row. Detects deletes by id-set difference
(same as `hash` mode). Does not maintain a hash store — every row is
written to the graph on every run.

Best for: small reference tables, or as a simple correctness guarantee
when performance is not a concern.

---

## Concept Tags and Classification

### Node-Level Tags

Concept tags group nodes into categories (business domain, ontology
class, data classification, security class, etc.) and are represented
as first-class graph nodes connected by `TAGGED_AS` edges:

```
(:Customer {name: 'Jane Doe'})
    -[:TAGGED_AS]->
        (:ConceptTag {name: 'PII', category: 'data_classification'})

(:Customer {name: 'Jane Doe'})
    -[:TAGGED_AS]->
        (:ConceptTag {name: 'Finance', category: 'business_domain'})
```

Tags are defined in `meta.concept_tag` and assigned to mappings in
`meta.entity_tag`. All nodes produced by a tagged mapping automatically
receive `TAGGED_AS` edges — no per-row configuration needed.

### Attribute-Level Classification

Privacy and security classifications are stored per column in
`meta.attribute_mapping` and emitted as companion properties directly
on the graph node. No extra nodes or edges are created:

```
# source attribute: email, privacy_class: PII, security_class: CONFIDENTIAL
# → graph node properties:
{
  "email":                   "jane@example.com",
  "email__privacy_class":    "PII",
  "email__security_class":   "CONFIDENTIAL"
}
```

This approach keeps attribute-level classification queryable
(`g.V().has('email__privacy_class', 'PII')`) without exploding the
graph with millions of `:Attribute` nodes.

Recommended values:

| Field | Recommended values |
|-------|--------------------|
| `privacy_class` | `PII`, `QUASI_PII`, `SENSITIVE`, `NON_PII` |
| `security_class` | `PUBLIC`, `INTERNAL`, `CONFIDENTIAL`, `SECRET` |

---

## Graph Targets

### AWS Neptune

```bash
export EKG_GRAPH_TARGET="neptune"
export EKG_TARGET_ENDPOINT="https://my-cluster.cluster-xxxx.eu-central-1.neptune.amazonaws.com:8182"
export EKG_TARGET_S3_STAGING="s3://my-bucket/ekg-bulk"
export EKG_TARGET_IAM_ROLE="arn:aws:iam::123456789012:role/NeptuneLoadFromS3"
export EKG_TARGET_CONCURRENCY="2"           # 1=LOW 2=MEDIUM 4=HIGH 8=OVERSUBSCRIBE
export EKG_TARGET_FAIL_ON_ERROR="false"
```

- **Incremental**: Gremlin HTTP (`/gremlin`) with `coalesce(unfold, addV/addE)` upsert pattern
- **Bulk load**: S3 CSV upload + Neptune Bulk Loader REST API; returns loader job ID stored in `run.load_run.bulk_load_job_id`

Dependencies: `pip install boto3 requests`

### Neo4j

```bash
export EKG_GRAPH_TARGET="neo4j"
export EKG_TARGET_ENDPOINT="bolt://neo4j.internal:7687"
export EKG_TARGET_DATABASE="neo4j"
export EKG_TARGET_BULK_BATCH="500"
```

- **Incremental**: Bolt driver with `MERGE … SET` Cypher
- **Bulk load**: Batched `UNWIND $rows MERGE` transactions over Bolt
- Nodes carry a `_ekg_id` property used in every `MERGE` predicate (Neo4j has no user-settable element ID)

Dependencies: `pip install neo4j`

### Azure Cosmos DB

```bash
export EKG_GRAPH_TARGET="cosmos"
export EKG_TARGET_ENDPOINT="wss://my-account.gremlin.cosmos.azure.com:443/"
# username = /dbs/<database>/colls/<graph>  — set in credentials.ini or secrets
export EKG_TARGET_BULK_BATCH="100"
```

- **Incremental**: Gremlin WebSocket via `gremlinpython`
- **Bulk load**: Batched Gremlin upserts (Cosmos DB has no native bulk import API for the Gremlin surface)
- The partition key property must be included in `attribute_mapping` for partitioned graphs

Dependencies: `pip install gremlinpython`

### Google Spanner Graph

```bash
export EKG_GRAPH_TARGET="spanner"
export GCP_PROJECT="my-gcp-project"
export EKG_TARGET_INSTANCE="my-spanner-instance"
export EKG_TARGET_DATABASE="my-spanner-database"
export EKG_TARGET_NODE_TABLE="EKGNode"      # default
export EKG_TARGET_EDGE_TABLE="EKGEdge"      # default
export EKG_TARGET_BULK_BATCH="1000"
```

Spanner tables must exist with at minimum:

```sql
-- Node table
CREATE TABLE EKGNode (
    id         STRING(MAX) NOT NULL,
    label      STRING(256) NOT NULL,
    properties JSON,
) PRIMARY KEY (id);

-- Edge table
CREATE TABLE EKGEdge (
    id         STRING(MAX) NOT NULL,
    from_id    STRING(MAX) NOT NULL,
    to_id      STRING(MAX) NOT NULL,
    label      STRING(256) NOT NULL,
    properties JSON,
) PRIMARY KEY (id);
```

- **Incremental**: `INSERT_OR_UPDATE` mutations via `google-cloud-spanner`
- **Bulk load**: Batched `INSERT_OR_UPDATE` mutations grouped into transactions
- Properties are serialised to a single JSON column

Dependencies: `pip install google-cloud-spanner`

---

## Secret Providers

| Provider | Trigger name | Required dependency | Auth mechanism |
|----------|-------------|-------------------|----------------|
| AWS Secrets Manager | `aws` | `boto3` | IAM role / env vars |
| Azure Key Vault | `azure` | `azure-keyvault-secrets`, `azure-identity` | `DefaultAzureCredential` |
| Google Secret Manager | `google` | `google-cloud-secret-manager` | Application Default Credentials |
| HashiCorp Vault | `vault` | `hvac` | Token or AppRole |
| INI file | `ini` | stdlib only | File permissions |
| Environment variables | `env` | stdlib only | OS environment |

Providers are tried in the order listed in `EKG_SECRET_PROVIDERS`.
`env` is always appended at the end if not explicitly listed.

Each provider expects credentials as a JSON dict with at minimum
`username` and `password` keys. Optional keys: `host`, `port`,
`dbname`, `endpoint`, `region`.

---

## Logging Targets Reference

| Target | Trigger name | Required dependency |
|--------|-------------|-------------------|
| Console (stdout) | `console` | stdlib only |
| AWS CloudWatch Logs | `cloudwatch` | `watchtower`, `boto3` |
| Azure Monitor / App Insights | `azure` | `opencensus-ext-azure` |
| Google Cloud Logging | `google` | `google-cloud-logging` |

```bash
export EKG_LOG_TARGETS="console,cloudwatch"
export EKG_LOG_GROUP="/ekg-etl"
export EKG_LOG_STREAM="run-20240101T120000"
export AWS_REGION="eu-central-1"

# Azure Monitor
export AZURE_MONITOR_CONNECTION_STRING="InstrumentationKey=...;IngestionEndpoint=..."

# Google Cloud Logging
export GCP_PROJECT="my-gcp-project"
```

Console is always added when no cloud handler installs successfully.

---

## SQL Registry (queries.ini)

All SQL is externalised in `queries.ini` so it can be modified without
touching Python code. The file uses Python `configparser` INI syntax.

### CTE Fragments

Meta queries are assembled from named CTE fragments. Each
`[meta.cte.<name>]` section defines one fragment (the inner `SELECT`,
named with its alias):

```ini
[meta.cte.entity_mapping]
sql =
    entity_mapping AS (
        SELECT mapping_id, ...
        FROM {schema}.entity_mapping
        WHERE gsr_client_id = '{gsr_client}'::uuid
          AND gsr_inst_id   = '{gsr_inst}'::uuid
          AND is_active     = TRUE
    )
```

### Read Queries

Queries declare which CTEs they need via a `ctes =` line:

```ini
[meta.load_mappings]
ctes = entity_mapping, graph_entity, source_table, data_source
sql =
    SELECT em.mapping_id::text, ...
    FROM entity_mapping em
    JOIN graph_entity   ge ON ...
```

`SqlRegistry` prepends `WITH <fragments>` automatically. The assembled
query is cached so repeated calls within a run are free.

### Structural Placeholders

| Placeholder | Substituted with |
|-------------|-----------------|
| `{schema}` | Meta/run schema name |
| `{gsr_client}` | Tenant client UUID |
| `{gsr_inst}` | Tenant instance UUID |

These are applied to both CTE fragments and query bodies. They are
validated (UUIDs against a regex) before substitution. Data values
(IDs, timestamps, row data) always use psycopg2 `%s` bind parameters.

---

## Environment Variable Reference

### Mandatory

| Variable | Description |
|----------|-------------|
| `GSR_CLIENT_ID` | Tenant client UUID |
| `GSR_INST_ID` | Tenant instance UUID |

### Pipeline Behaviour

| Variable | Default | Description |
|----------|---------|-------------|
| `EKG_GRAPH_TARGET` | `neptune` | Graph target: `neptune`, `neo4j`, `cosmos`, `spanner` |
| `EKG_SECRET_PROVIDERS` | `aws,env` | Ordered comma-separated list of secret providers |
| `EKG_LOG_TARGETS` | `console` | Ordered comma-separated list of log targets |
| `EKG_BATCH_SIZE` | `50` | Graph write batch size (incremental mode) |
| `EKG_CREDENTIALS_FILE` | _(search path)_ | Explicit path to `credentials.ini` |

### Meta Database

| Variable | Default | Description |
|----------|---------|-------------|
| `META_DB_HOST` | `localhost` | Meta DB host |
| `META_DB_PORT` | `5432` | Meta DB port |
| `META_DB_NAME` | `meta` | Meta DB database name |
| `META_DB_SCHEMA` | `meta` | Meta schema name |
| `META_SECRET_REF` | `prod/meta-db` | Credential ref for meta DB |

### Run Database

| Variable | Default | Description |
|----------|---------|-------------|
| `RUN_DB_HOST` | `run-db-host` | Run DB host |
| `RUN_DB_PORT` | `5432` | Run DB port |
| `RUN_DB_NAME` | `run` | Run DB database name |
| `RUN_DB_SCHEMA` | `run` | Run schema name |
| `RUN_SECRET_REF` | `prod/run-db` | Credential ref for run DB |

### Graph Target (common)

| Variable | Description |
|----------|-------------|
| `EKG_TARGET_ENDPOINT` | Graph DB endpoint URL or Bolt URI |
| `EKG_TARGET_SECRET_REF` | Credential ref for graph target |
| `EKG_TARGET_DATABASE` | Graph database / keyspace name |
| `EKG_TARGET_BULK_BATCH` | Rows per bulk transaction batch |

### Graph Target (Neptune-specific)

| Variable | Description |
|----------|-------------|
| `EKG_TARGET_S3_STAGING` | S3 URI prefix for CSV staging |
| `EKG_TARGET_IAM_ROLE` | IAM role ARN for Neptune S3 access |
| `EKG_TARGET_CONCURRENCY` | Loader parallelism 1–8 (default 2) |
| `EKG_TARGET_FAIL_ON_ERROR` | Neptune failOnError flag |

### Graph Target (Spanner-specific)

| Variable | Description |
|----------|-------------|
| `EKG_TARGET_INSTANCE` | Spanner instance ID |
| `EKG_TARGET_NODE_TABLE` | Spanner node table name (default `EKGNode`) |
| `EKG_TARGET_EDGE_TABLE` | Spanner edge table name (default `EKGEdge`) |

### Secret Providers

| Variable | Description |
|----------|-------------|
| `AWS_REGION` | AWS region (Secrets Manager + CloudWatch) |
| `AZURE_VAULT_URL` | Azure Key Vault URL |
| `GCP_PROJECT` | GCP project ID (Secret Manager + Cloud Logging) |
| `VAULT_ADDR` | HashiCorp Vault address |
| `VAULT_TOKEN` | Vault static token (takes precedence over AppRole) |
| `VAULT_ROLE_ID` | Vault AppRole role ID |
| `VAULT_SECRET_ID` | Vault AppRole secret ID |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `EKG_LOG_GROUP` | `/ekg-etl` | CloudWatch log group name |
| `EKG_LOG_STREAM` | `ekg-etl` | CloudWatch log stream name |
| `AZURE_MONITOR_CONNECTION_STRING` | | App Insights connection string |

---

## Adding a New Graph Target

1. Create `graph/my_target.py` subclassing `GraphTarget`:

```python
from graph.base import BulkRecord, GraphTarget, GraphTargetConfig

class MyGraphTarget(GraphTarget):
    def __init__(self, config: GraphTargetConfig) -> None:
        super().__init__(config)
        # initialise your client here

    def upsert_node(self, node_id, label, props): ...
    def upsert_edge(self, edge_id, label, from_id, to_id, props): ...
    def delete_vertex(self, node_id): ...
    def delete_edge(self, edge_id): ...
    def upsert_concept_tag(self, tag_id, tag_name, tag_category, display_name): ...
    def upsert_tagged_as(self, node_id, tag_id): ...
    def begin_bulk(self): ...
    def write_bulk_node(self, record: BulkRecord): ...
    def write_bulk_edge(self, record: BulkRecord): ...
    def commit_bulk(self) -> str: ...
    def close(self): ...
```

2. Register it in `graph/__init__.py`:

```python
from graph.my_target import MyGraphTarget

_REGISTRY = {
    ...
    "mytarget": MyGraphTarget,
}
```

3. Set `EKG_GRAPH_TARGET=mytarget` — no other changes needed.

---

## Adding a New Secret Provider

1. Create `secrets/my_provider.py` subclassing `SecretsProvider`:

```python
from secrets.base import SecretsProvider

class MySecretsProvider(SecretsProvider):
    def resolve(self, ref: str):
        # return dict with at least username + password, or None
        ...
```

2. Register it in `secrets/base.py` inside `build_secrets_chain()`:

```python
from secrets.my_provider import MySecretsProvider

_MAP = {
    ...
    "myprovider": lambda: MySecretsProvider(**kwargs),
}
```

3. Add `myprovider` to `EKG_SECRET_PROVIDERS`.

---

## Security Considerations

- **No passwords in the database.** `meta.data_source` stores a
  `secret_ref` string, never a password. All credentials are resolved
  at runtime via the secrets chain.
- **INI file permissions.** The pipeline checks file permissions at
  load time and warns if the file is group- or world-readable. Always
  run `chmod 600 credentials.ini` immediately after creating the file.
- **Never commit credentials.ini** to version control. Add it to
  `.gitignore`: `echo "credentials.ini" >> .gitignore`
- **Tenant isolation** is enforced at the query level via CTE-embedded
  `WHERE gsr_client_id = … AND gsr_inst_id = …` filters. The meta DB
  user does not need row-level security enabled.
- **UUID validation** is performed on `gsr_client_id` and `gsr_inst_id`
  before they are interpolated into any SQL string.
- **Meta DB session** is set `READ ONLY` immediately after connecting,
  providing a safety net against accidental writes.
- **Gremlin string escaping** is applied to all property keys and values
  before they are embedded in Gremlin traversal strings.

---

## Troubleshooting

### `RuntimeError: No secrets provider could resolve ref 'prod/meta-db'`

- Check `EKG_SECRET_PROVIDERS` includes `ini` if you are using a credentials file
- Verify the section name in `credentials.ini` exactly matches the ref (e.g. `[prod/meta-db]`)
- Run the diagnostic: `python3 -c "from secrets.ini_file import IniSecretsProvider; from pathlib import Path; print(IniSecretsProvider(Path('credentials.ini')).resolve('prod/meta-db'))"`

### `SyntaxError` or import errors on startup

- Ensure you are running from the `ekg_etl/` directory: `cd ekg_etl && python ekg_etl.py`
- Check Python version: `python3 --version` (3.9+ required)

### `RuntimeError: No active control_sdts row found`

- Insert an active row into `meta.control_sdts` for your tenant with `is_active = TRUE` and a valid `gsr_sdts` timestamp
- Ensure only one row per tenant has `is_active = TRUE`

### `KeyError: SqlRegistry: no section [meta.cte.X]`

- A query's `ctes =` line references a CTE name that has no matching `[meta.cte.X]` section in `queries.ini`
- Check for typos in the CTE name

### Graph writes failing silently (incremental mode)

- Enable debug logging: `python ekg_etl.py --mode incremental --debug`
- Check that `load_order` on node mappings is lower than on edge mappings — edges require their endpoint nodes to exist first

### Full load stuck polling

- Neptune only: check the loader job status directly via the Neptune console or:
  `curl https://<endpoint>:8182/loader/<job_id>`
- Increase `EKG_TARGET_FAIL_ON_ERROR=true` to surface individual row errors
