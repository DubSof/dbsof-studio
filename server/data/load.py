# Not working yet
# Same as mock.py, but use Arango DB instead of mocking

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from arango import ArangoClient
from arango.database import StandardDatabase

from data.schema import (
    AiProgram,
    AiProgramStatus,
    AiTask,
    AiTaskStatus,
    AiTaskWithProgram,
    Database,
    ImportFile,
    ImportJob,
    ImportJobStatus,
    ImportSource,
    Instance,
    Migration,
    ProgramGraph,
    ProgramGraphEdge,
    ProgramGraphNode,
    ProgramGraphNodeStatus,
    SchemaSnapshot,
    SqlCommandResult,
    SqlHistoryItem,
    SqlHistoryPage,
    SqlMode,
    TableRowsPage,
    TableSchema,
    TableSummary,
    UserSettings,
)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

_client: ArangoClient | None = None
_meta_db: StandardDatabase | None = None
_init_lock = Lock()

_META_DB_NAME = "dbsof"
_COLLECTIONS = [
    "instances",
    "databases",
    "migrations",
    "user_settings",
    "sql_history",
    "schemas",
    "ai_tasks",
    "ai_programs",
    "import_jobs",
]


def _get_meta_db() -> StandardDatabase:
    global _client, _meta_db
    if _meta_db is not None:
        return _meta_db
    with _init_lock:
        if _meta_db is not None:
            return _meta_db
        url = os.environ["ARANGO_URL"]
        _client = ArangoClient(hosts=url)
        sys_db = _client.db("_system", username="", password="")
        if not sys_db.has_database(_META_DB_NAME):
            sys_db.create_database(_META_DB_NAME)
        db = _client.db(_META_DB_NAME, username="", password="")
        for col in _COLLECTIONS:
            if not db.has_collection(col):
                db.create_collection(col)
        _meta_db = db
    return _meta_db


def _get_data_db(instance_id: str, database: str) -> StandardDatabase:
    """Return (creating if needed) the per-(instance, database) ArangoDB database."""
    assert _client is not None or True  # ensure meta db initialised first
    db_name = f"inst_{instance_id}__{database}"
    # Re-use the client; we need sys access to create the db
    url = os.environ["ARANGO_URL"]
    client = _client or ArangoClient(hosts=url)
    sys_db = client.db("_system", username="", password="")
    if not sys_db.has_database(db_name):
        sys_db.create_database(db_name)
    return client.db(db_name, username="", password="")


def _db() -> StandardDatabase:
    return _get_meta_db()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _uid(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _strip_arango(doc: dict) -> dict:
    """Remove ArangoDB internal keys from a document."""
    return {k: v for k, v in doc.items() if not k.startswith("_")}


# ---------------------------------------------------------------------------
# Instances
# ---------------------------------------------------------------------------

def get_instances() -> list[Instance]:
    db = _db()
    cursor = db.aql.execute("FOR d IN instances RETURN d")
    return [Instance(**_strip_arango(d)) for d in cursor]


def get_databases(instance_id: str) -> list[Database]:
    db = _db()
    cursor = db.aql.execute(
        "FOR d IN databases FILTER d.instance_id == @inst RETURN d",
        bind_vars={"inst": instance_id},
    )
    return [Database(name=d["name"], last_migration=d.get("last_migration")) for d in cursor]


def create_database(
    instance_id: str, name: str, from_branch: str | None, copy_data: bool
) -> Database:
    db = _db()
    key = f"{instance_id}__{name}"

    # Resolve last_migration from source branch
    last_mig = None
    if from_branch:
        src_key = f"{instance_id}__{from_branch}"
        src = db.collection("databases").get(src_key)
        if src:
            last_mig = src.get("last_migration")

    doc = {
        "_key": key,
        "instance_id": instance_id,
        "name": name,
        "last_migration": last_mig,
    }
    col = db.collection("databases")
    if col.has(key):
        col.replace(doc)
    else:
        col.insert(doc)

    # Copy migrations from source branch
    if from_branch:
        cursor = db.aql.execute(
            "FOR m IN migrations FILTER m.instance_id == @inst AND m.db_name == @src RETURN m",
            bind_vars={"inst": instance_id, "src": from_branch},
        )
        mig_col = db.collection("migrations")
        for m in cursor:
            new_key = f"{instance_id}__{name}__{m['id']}"
            new_doc = dict(m)
            new_doc["_key"] = new_key
            new_doc["db_name"] = name
            for k in ("_id", "_rev"):
                new_doc.pop(k, None)
            if not mig_col.has(new_key):
                mig_col.insert(new_doc)

    return Database(name=name, last_migration=last_mig)


def get_migrations(instance_id: str, database: str) -> list[Migration]:
    db = _db()
    cursor = db.aql.execute(
        "FOR m IN migrations FILTER m.instance_id == @inst AND m.db_name == @db RETURN m",
        bind_vars={"inst": instance_id, "db": database},
    )
    return [
        Migration(id=m["id"], name=m.get("name"), parent_id=m.get("parent_id"))
        for m in cursor
    ]


# ---------------------------------------------------------------------------
# User settings
# ---------------------------------------------------------------------------

def get_user_settings(user_id: str) -> UserSettings:
    db = _db()
    doc = db.collection("user_settings").get(user_id)
    if not doc:
        return UserSettings()
    return UserSettings(**_strip_arango(doc))


def update_user_settings(user_id: str, incoming: UserSettings) -> UserSettings:
    db = _db()
    col = db.collection("user_settings")
    existing = col.get(user_id)
    if existing:
        merged = _strip_arango(existing)
    else:
        merged = {}
    merged.update(incoming.model_dump(exclude_unset=True))
    merged["_key"] = user_id
    if existing:
        col.replace(merged)
    else:
        col.insert(merged)
    return UserSettings(**{k: v for k, v in merged.items() if k != "_key"})


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

def execute_sql(
    instance_id: str,
    database: str,
    query: str,
    mode: SqlMode,
    params: dict | None = None,
) -> SqlCommandResult:
    ts = _now()
    bind_vars: dict[str, Any] = {}
    if params:
        bind_vars.update({k: v.get("value", v) if isinstance(v, dict) else v for k, v in params.items()})

    try:
        data_db = _get_data_db(instance_id, database)
        t0 = datetime.now(timezone.utc)
        cursor = data_db.aql.execute(query, bind_vars=bind_vars or None)
        duration_ms = (datetime.now(timezone.utc) - t0).total_seconds() * 1000
        rows_raw: list[Any] = list(cursor)

        if mode == SqlMode.raw:
            result = SqlCommandResult(
                id=_uid("cmd_raw_"),
                status="completed",
                duration_ms=duration_ms,
                raw_text=f"OK: {len(rows_raw)} rows",
                columns=None,
                rows=None,
                warnings=None,
            )
        else:
            if rows_raw and isinstance(rows_raw[0], dict):
                columns = list(rows_raw[0].keys())
                rows = [[row.get(c) for c in columns] for row in rows_raw]
            else:
                columns = ["value"]
                rows = [[r] for r in rows_raw]
            result = SqlCommandResult(
                id=_uid("cmd_tab_"),
                status="completed",
                duration_ms=duration_ms,
                columns=columns,
                rows=rows,
                raw_text=None,
                warnings=None,
            )
        status = "completed"
    except Exception as exc:
        duration_ms = 0.0
        result = SqlCommandResult(
            id=_uid("cmd_err_"),
            status="error",
            duration_ms=duration_ms,
            raw_text=str(exc),
            columns=None,
            rows=None,
            warnings=None,
        )
        status = "error"

    # Record in history
    sh_id = _uid("sh_")
    history_doc = {
        "_key": sh_id,
        "id": sh_id,
        "instance_id": instance_id,
        "db_name": database,
        "query": query,
        "params": params,
        "status": status,
        "created_at": ts.isoformat(),
        "duration_ms": result.duration_ms,
    }
    _db().collection("sql_history").insert(history_doc)

    return result


def get_sql_history(
    instance_id: str,
    database: str,
    limit: int = 50,
    cursor: str | None = None,
) -> SqlHistoryPage:
    db = _db()
    if cursor:
        aql = (
            "FOR d IN sql_history "
            "FILTER d.instance_id == @inst AND d.db_name == @db AND d._key > @cursor "
            "SORT d._key ASC LIMIT @limit RETURN d"
        )
        bind_vars = {"inst": instance_id, "db": database, "cursor": cursor, "limit": limit}
    else:
        aql = (
            "FOR d IN sql_history "
            "FILTER d.instance_id == @inst AND d.db_name == @db "
            "SORT d._key ASC LIMIT @limit RETURN d"
        )
        bind_vars = {"inst": instance_id, "db": database, "limit": limit}

    result_cursor = db.aql.execute(aql, bind_vars=bind_vars)
    docs = list(result_cursor)
    items = [
        SqlHistoryItem(
            id=d["id"],
            query=d.get("query"),
            params=d.get("params"),
            status=d.get("status"),
            created_at=datetime.fromisoformat(d["created_at"]) if d.get("created_at") else None,
            duration_ms=d.get("duration_ms"),
        )
        for d in docs
    ]

    # Determine next cursor: check if there are more items
    next_cursor = None
    if len(docs) == limit:
        check = db.aql.execute(
            "FOR d IN sql_history "
            "FILTER d.instance_id == @inst AND d.db_name == @db AND d._key > @last "
            "LIMIT 1 RETURN d._key",
            bind_vars={"inst": instance_id, "db": database, "last": docs[-1]["_key"]},
        )
        if list(check):
            next_cursor = docs[-1]["id"]

    return SqlHistoryPage(items=items, next_cursor=next_cursor)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def get_schema(instance_id: str, database: str) -> SchemaSnapshot:
    db = _db()
    key = f"{instance_id}__{database}"
    doc = db.collection("schemas").get(key)
    if not doc:
        return SchemaSnapshot(version="v0.0.0", types=[])
    import json
    raw = doc.get("snapshot")
    if raw:
        return SchemaSnapshot.model_validate_json(raw if isinstance(raw, str) else json.dumps(raw))
    return SchemaSnapshot(version="v0.0.0", types=[])


# ---------------------------------------------------------------------------
# Tables (backed by actual ArangoDB collections in the per-db database)
# ---------------------------------------------------------------------------

_SYSTEM_COLLECTIONS = {"_graphs", "_jobs", "_queues", "_statistics", "_statistics15", "_statisticsRaw"}


def get_tables(instance_id: str, database: str) -> list[TableSummary]:
    data_db = _get_data_db(instance_id, database)
    summaries = []
    for col_info in data_db.collections():
        name = col_info["name"]
        if name.startswith("_") or name in _SYSTEM_COLLECTIONS:
            continue
        col = data_db.collection(name)
        count = col.count()
        # Sample one doc to get column names
        sample_cursor = data_db.aql.execute(
            f"FOR d IN `{name}` LIMIT 1 RETURN d"
        )
        sample = list(sample_cursor)
        columns = None
        if sample and isinstance(sample[0], dict):
            from data.schema import TableColumn
            columns = [
                TableColumn(name=k, type="any", nullable=True)
                for k in sample[0].keys()
                if not k.startswith("_")
            ]
        summaries.append(TableSummary(name=name, row_count=count, columns=columns))
    return summaries


def get_table_schema(instance_id: str, database: str, table: str) -> TableSchema:
    from data.schema import TableColumn
    data_db = _get_data_db(instance_id, database)
    sample_cursor = data_db.aql.execute(
        f"FOR d IN `{table}` LIMIT 1 RETURN d"
    )
    sample = list(sample_cursor)
    if not sample or not isinstance(sample[0], dict):
        return TableSchema(name=table, columns=[], indexes=[], primary_key=[])
    columns = [
        TableColumn(name=k, type="any", nullable=True)
        for k in sample[0].keys()
        if not k.startswith("_")
    ]
    return TableSchema(name=table, columns=columns, indexes=[], primary_key=["_key"])


def get_table_rows(
    instance_id: str,
    database: str,
    table: str,
    limit: int = 100,
    offset: int = 0,
    where: str | None = None,
    order_by: str | None = None,
) -> TableRowsPage:
    data_db = _get_data_db(instance_id, database)

    filter_clause = f"FILTER {where}" if where else ""
    sort_clause = f"SORT d.{order_by}" if order_by else ""
    aql = (
        f"FOR d IN `{table}` {filter_clause} {sort_clause} "
        f"LIMIT @offset, @limit RETURN d"
    )
    cursor = data_db.aql.execute(aql, bind_vars={"offset": offset, "limit": limit})
    docs = list(cursor)

    if not docs:
        total_cursor = data_db.aql.execute(
            f"RETURN LENGTH(`{table}`)"
        )
        total = list(total_cursor)[0]
        return TableRowsPage(columns=[], rows=[], total=total)

    columns = [k for k in docs[0].keys() if not k.startswith("_")]
    rows = [[doc.get(c) for c in columns] for doc in docs]
    total_cursor = data_db.aql.execute(f"RETURN LENGTH(`{table}`)")
    total = list(total_cursor)[0]
    return TableRowsPage(columns=columns, rows=rows, total=total)


# ---------------------------------------------------------------------------
# AI Tasks
# ---------------------------------------------------------------------------

def create_ai_task(
    instance_id: str, database: str, feature: str, name: str | None = None
) -> AiTaskWithProgram:
    import random
    db = _db()
    ts = _now()
    program_id = _uid("prog_")
    task_id = _uid("task_")

    graph = ProgramGraph(
        nodes=[
            ProgramGraphNode(id="n1", label="Parse request", status=ProgramGraphNodeStatus.done),
            ProgramGraphNode(id="n2", label=feature, status=ProgramGraphNodeStatus.in_progress),
            ProgramGraphNode(id="n3", label="Return response", status=ProgramGraphNodeStatus.pending),
        ],
        edges=[
            ProgramGraphEdge(from_="n1", to="n2", label="nice"),
            ProgramGraphEdge(from_="n2", to="n3", label="finished"),
        ],
    )
    program = AiProgram(
        id=program_id,
        feature=feature,
        status=random.choice([AiProgramStatus.building, AiProgramStatus.failed, AiProgramStatus.ready]),
        created_at=ts,
        updated_at=ts,
        graph=graph,
    )
    task = AiTask(
        id=task_id,
        program_id=program_id,
        status=AiTaskStatus.pending,
        prompt=feature,
        created_at=ts,
        completed_at=None,
    )

    prog_doc = program.model_dump(mode="json")
    prog_doc["_key"] = program_id
    prog_doc["instance_id"] = instance_id
    prog_doc["db_name"] = database
    db.collection("ai_programs").insert(prog_doc)

    task_doc = task.model_dump(mode="json")
    task_doc["_key"] = task_id
    task_doc["instance_id"] = instance_id
    task_doc["db_name"] = database
    db.collection("ai_tasks").insert(task_doc)

    return AiTaskWithProgram(task=task, program=program)


def list_ai_tasks(
    instance_id: str, database: str, status_filter: AiTaskStatus | None = None
) -> list[AiTask]:
    db = _db()
    if status_filter:
        cursor = db.aql.execute(
            "FOR d IN ai_tasks FILTER d.instance_id == @inst AND d.db_name == @db "
            "AND d.status == @status RETURN d",
            bind_vars={"inst": instance_id, "db": database, "status": status_filter.value},
        )
    else:
        cursor = db.aql.execute(
            "FOR d IN ai_tasks FILTER d.instance_id == @inst AND d.db_name == @db RETURN d",
            bind_vars={"inst": instance_id, "db": database},
        )
    return [AiTask(**_strip_arango(d)) for d in cursor]


def get_ai_task(instance_id: str, database: str, task_id: str) -> AiTaskWithProgram:
    db = _db()
    task_doc = db.collection("ai_tasks").get(task_id)
    if not task_doc:
        task = AiTask(id=task_id, status=AiTaskStatus.pending)
        return AiTaskWithProgram(task=task, program=None)
    task = AiTask(**_strip_arango(task_doc))
    program = None
    if task.program_id:
        prog_doc = db.collection("ai_programs").get(task.program_id)
        if prog_doc:
            program = AiProgram(**_strip_arango(prog_doc))
    return AiTaskWithProgram(task=task, program=program)


# ---------------------------------------------------------------------------
# AI Programs
# ---------------------------------------------------------------------------

def create_ai_program(
    instance_id: str, database: str, feature: str, name: str | None = None
) -> AiTaskWithProgram:
    return create_ai_task(instance_id, database, feature, name)


def list_ai_programs(instance_id: str, database: str) -> list[AiProgram]:
    db = _db()
    cursor = db.aql.execute(
        "FOR d IN ai_programs FILTER d.instance_id == @inst AND d.db_name == @db RETURN d",
        bind_vars={"inst": instance_id, "db": database},
    )
    return [AiProgram(**_strip_arango(d)) for d in cursor]


def get_ai_program(instance_id: str, database: str, program_id: str) -> AiProgram:
    db = _db()
    doc = db.collection("ai_programs").get(program_id)
    if not doc:
        return AiProgram(id=program_id, status=AiProgramStatus.building)
    return AiProgram(**_strip_arango(doc))


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

def create_import(
    instance_id: str,
    database: str,
    name: str | None,
    source: str | None,
    notes: str | None,
    rows: int | None,
    files: list[tuple[str, int]] | None = None,
) -> ImportJob:
    db = _db()
    ts = _now()
    job_id = _uid("imp_")

    job = ImportJob(
        id=job_id,
        name=name or "Untitled import",
        source=ImportSource(source) if source else ImportSource.csv_upload,
        status=ImportJobStatus.running,
        progress=0.0,
        created_at=ts,
        completed_at=None,
        updated_at=ts,
        rows=rows,
        notes=notes,
        files=[ImportFile(filename=fn, size=sz) for fn, sz in files] if files else [],
    )

    doc = job.model_dump(mode="json")
    doc["_key"] = job_id
    doc["instance_id"] = instance_id
    doc["db_name"] = database
    db.collection("import_jobs").insert(doc)
    return job


def list_imports(instance_id: str, database: str) -> list[ImportJob]:
    db = _db()
    cursor = db.aql.execute(
        "FOR d IN import_jobs FILTER d.instance_id == @inst AND d.db_name == @db RETURN d",
        bind_vars={"inst": instance_id, "db": database},
    )
    return [ImportJob(**_strip_arango(d)) for d in cursor]


def get_import(instance_id: str, database: str, job_id: str) -> ImportJob:
    db = _db()
    doc = db.collection("import_jobs").get(job_id)
    if not doc:
        return ImportJob(id=job_id, status=ImportJobStatus.failed)
    return ImportJob(**_strip_arango(doc))
