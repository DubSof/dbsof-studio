from __future__ import annotations

import uuid
import random
from datetime import datetime, timezone

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
    SchemaReference,
    SchemaReferenceType,
    SchemaSnapshot,
    SchemaType,
    SchemaTypeKind,
    SqlCommandResult,
    SqlHistoryItem,
    SqlHistoryPage,
    SqlMode,
    TableColumn,
    TableIndex,
    TableRowsPage,
    TableSchema,
    TableSummary,
    UserSettings,
)


def _uid(prefix: str = "") -> str:
    """Generate a short unique id with an optional prefix."""
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


# In-memory stores

_instances: dict[str, Instance] = {}
# Keyed by (instance_id, db_name)
_databases: dict[str, dict[str, Database]] = {}
_migrations: dict[str, dict[str, list[Migration]]] = {}

_user_settings: dict[str, UserSettings] = {}

# Keyed by (instance_id, db_name) -> list
_sql_history: dict[str, dict[str, list[SqlHistoryItem]]] = {}

# Schema snapshots keyed by (instance_id, db_name)
_schemas: dict[str, dict[str, SchemaSnapshot]] = {}

# Tables: keyed by (instance_id, db_name) -> dict of table_name -> { schema, rows }
_tables: dict[str, dict[str, dict[str, dict]]] = {}

# AI tasks keyed by (instance_id, db_name) -> dict of task_id -> AiTask
_ai_tasks: dict[str, dict[str, dict[str, AiTask]]] = {}
# AI programs keyed by (instance_id, db_name) -> dict of program_id -> AiProgram
_ai_programs: dict[str, dict[str, dict[str, AiProgram]]] = {}

# Import jobs keyed by (instance_id, db_name) -> dict of job_id -> ImportJob
_import_jobs: dict[str, dict[str, dict[str, ImportJob]]] = {}


# Seed data

def _seed() -> None:
    """Populate stores with initial demo data."""
    _instances.clear()
    _databases.clear()
    _migrations.clear()
    _user_settings.clear()
    _sql_history.clear()
    _schemas.clear()
    _tables.clear()
    _ai_tasks.clear()
    _ai_programs.clear()
    _import_jobs.clear()

    now = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    earlier = datetime(2025, 6, 15, 11, 30, 0, tzinfo=timezone.utc)

    # -- Instances --
    for inst in [
        Instance(id="inst_001", name="Production"),
        Instance(id="inst_002", name="Staging"),
    ]:
        _instances[inst.id] = inst

    # -- Databases --
    for inst_id in ("inst_001", "inst_002"):
        _databases.setdefault(inst_id, {})
        _databases[inst_id]["main"] = Database(name="main", last_migration="mig_003")
        _databases[inst_id]["feature-auth"] = Database(name="feature-auth", last_migration="mig_001")

    # -- Migrations --
    migs = [
        Migration(id="mig_001", name="0001-initial", parent_id=None),
        Migration(id="mig_002", name="0002-add-users", parent_id="mig_001"),
        Migration(id="mig_003", name="0003-add-orders", parent_id="mig_002"),
    ]
    for inst_id in ("inst_001", "inst_002"):
        _migrations.setdefault(inst_id, {})
        for db_name in ("main", "feature-auth"):
            _migrations[inst_id][db_name] = list(migs)

    # -- User settings --
    _user_settings["default"] = UserSettings(
        theme="dark",
        language="en",
        editorFontSize=14,
        autoSave=True,
    )

    # -- Columns shared by "users" table --
    users_columns = [
        TableColumn(name="id", type="uuid", nullable=False, default="gen_random_uuid()"),
        TableColumn(name="name", type="text", nullable=False, default=None),
        TableColumn(name="email", type="text", nullable=True, default=None),
        TableColumn(name="created_at", type="timestamptz", nullable=False, default="now()"),
    ]
    orders_columns = [
        TableColumn(name="id", type="uuid", nullable=False, default="gen_random_uuid()"),
        TableColumn(name="user_id", type="uuid", nullable=False, default=None),
        TableColumn(name="total", type="numeric(10,2)", nullable=False, default="0"),
        TableColumn(name="status", type="text", nullable=False, default="'pending'"),
    ]

    # -- Tables (per instance/db) --
    for inst_id in ("inst_001", "inst_002"):
        _tables.setdefault(inst_id, {})
        for db_name in ("main", "feature-auth"):
            _tables[inst_id].setdefault(db_name, {})
            _tables[inst_id][db_name]["users"] = {
                "summary": TableSummary(name="users", row_count=142, columns=users_columns),
                "schema": TableSchema(
                    name="users",
                    columns=users_columns,
                    indexes=[
                        TableIndex(name="users_pkey", expression="id"),
                        TableIndex(name="users_email_idx", expression="email"),
                    ],
                    primary_key=["id"],
                ),
                "columns": ["id", "name", "email", "created_at"],
                "rows": [
                    ["a1b2c3d4", "Alice", "alice@example.com", "2025-01-10T08:00:00Z"],
                    ["e5f6g7h8", "Bob", "bob@example.com", "2025-02-14T10:30:00Z"],
                    ["i9j0k1l2", "Charlie", None, "2025-03-20T15:45:00Z"],
                ],
            }
            _tables[inst_id][db_name]["orders"] = {
                "summary": TableSummary(name="orders", row_count=1089, columns=orders_columns),
                "schema": TableSchema(
                    name="orders",
                    columns=orders_columns,
                    indexes=[
                        TableIndex(name="orders_pkey", expression="id"),
                    ],
                    primary_key=["id"],
                ),
                "columns": ["id", "user_id", "total", "status"],
                "rows": [],
            }

    # -- Schema snapshots --
    snapshot = SchemaSnapshot(
        version="v1.2.0",
        types=[
            SchemaType(name="User", kind=SchemaTypeKind.table, module="default", references=[]),
            SchemaType(
                name="Order",
                kind=SchemaTypeKind.table,
                module="default",
                references=[SchemaReference(target="User", type=SchemaReferenceType.link)],
            ),
            SchemaType(name="OrderStatus", kind=SchemaTypeKind.enum, module="default", references=[]),
            SchemaType(
                name="active_users",
                kind=SchemaTypeKind.view,
                module="default",
                references=[SchemaReference(target="User", type=SchemaReferenceType.dependency)],
            ),
        ],
    )
    for inst_id in ("inst_001", "inst_002"):
        _schemas.setdefault(inst_id, {})
        for db_name in ("main", "feature-auth"):
            _schemas[inst_id][db_name] = snapshot.model_copy(deep=True)

    # -- SQL history --
    history_items = [
        SqlHistoryItem(
            id="sh_001",
            query="SELECT User { id, name, email } LIMIT 10",
            params=None,
            status="completed",
            created_at=earlier,
            duration_ms=12.5,
        ),
        SqlHistoryItem(
            id="sh_002",
            query="INSERT User { name := 'Dave', email := 'dave@example.com' }",
            params=None,
            status="completed",
            created_at=now,
            duration_ms=8.3,
        ),
    ]
    for inst_id in ("inst_001", "inst_002"):
        _sql_history.setdefault(inst_id, {})
        for db_name in ("main", "feature-auth"):
            _sql_history[inst_id][db_name] = list(history_items)

    # -- AI programs & tasks --
    graph = ProgramGraph(
        nodes=[
            ProgramGraphNode(id="n1", label="Parse request", status=ProgramGraphNodeStatus.done),
            ProgramGraphNode(id="n2", label="Generate migration", status=ProgramGraphNodeStatus.done),
            ProgramGraphNode(id="n3", label="Apply migration", status=ProgramGraphNodeStatus.pending),
        ],
        edges=[
            ProgramGraphEdge(from_="n1", to="n2", label="parsed"),
            ProgramGraphEdge(from_="n2", to="n3", label="generated"),
        ],
    )
    program = AiProgram(
        id="prog_001",
        feature="Add a reviews table linked to orders",
        status=AiProgramStatus.building,
        created_at=earlier,
        updated_at=now,
        graph=graph,
    )
    task = AiTask(
        id="task_001",
        program_id="prog_001",
        status=AiTaskStatus.running,
        prompt="Add a reviews table linked to orders",
        created_at=earlier,
        completed_at=None,
    )
    for inst_id in ("inst_001", "inst_002"):
        _ai_programs.setdefault(inst_id, {})
        _ai_tasks.setdefault(inst_id, {})
        for db_name in ("main", "feature-auth"):
            _ai_programs[inst_id].setdefault(db_name, {})
            _ai_tasks[inst_id].setdefault(db_name, {})
            _ai_programs[inst_id][db_name]["prog_001"] = program.model_copy(deep=True)
            _ai_tasks[inst_id][db_name]["task_001"] = task.model_copy(deep=True)

    # -- Import jobs --
    job_completed = ImportJob(
        id="imp_001",
        name="Q2 customer data",
        source=ImportSource.csv_upload,
        status=ImportJobStatus.completed,
        progress=100.0,
        created_at=earlier,
        completed_at=now,
        updated_at=now,
        rows=2500,
        notes="Imported from marketing CRM export",
        files=[
            ImportFile(filename="customers_q2.csv", size=184320),
            ImportFile(filename="customers_q2_extra.csv", size=51200),
        ],
    )
    job_running = ImportJob(
        id="imp_002",
        name="Product catalogue sync",
        source=ImportSource.database_sync,
        status=ImportJobStatus.running,
        progress=63.0,
        created_at=now,
        completed_at=None,
        updated_at=now,
        rows=None,
        notes=None,
        files=[],
    )
    for inst_id in ("inst_001", "inst_002"):
        _import_jobs.setdefault(inst_id, {})
        for db_name in ("main", "feature-auth"):
            _import_jobs[inst_id].setdefault(db_name, {})
            _import_jobs[inst_id][db_name]["imp_001"] = job_completed.model_copy(deep=True)
            _import_jobs[inst_id][db_name]["imp_002"] = job_running.model_copy(deep=True)


# Run seed on module import
_seed()


# Helper to ensure nested dicts exist

def _ensure_inst_db(inst_id: str, db_name: str) -> None:
    """Make sure nested dicts are initialised for the given instance/db pair."""
    _databases.setdefault(inst_id, {})
    _migrations.setdefault(inst_id, {}).setdefault(db_name, [])
    _sql_history.setdefault(inst_id, {}).setdefault(db_name, [])
    _schemas.setdefault(inst_id, {})
    _tables.setdefault(inst_id, {}).setdefault(db_name, {})
    _ai_tasks.setdefault(inst_id, {}).setdefault(db_name, {})
    _ai_programs.setdefault(inst_id, {}).setdefault(db_name, {})
    _import_jobs.setdefault(inst_id, {}).setdefault(db_name, {})


# Instances

def get_instances() -> list[Instance]:
    return list(_instances.values())


def get_databases(instance_id: str) -> list[Database]:
    return list(_databases.get(instance_id, {}).values())


def create_database(instance_id: str, name: str, from_branch: str | None, copy_data: bool) -> Database:
    _databases.setdefault(instance_id, {})
    source = _databases[instance_id].get(from_branch) if from_branch else None
    last_mig = source.last_migration if source else None
    db = Database(name=name, last_migration=last_mig)
    _databases[instance_id][name] = db

    # Initialise sub-stores for the new database
    _ensure_inst_db(instance_id, name)

    # If copying from an existing branch, duplicate its migrations and (optionally) table data
    if from_branch and from_branch in _migrations.get(instance_id, {}):
        _migrations[instance_id][name] = list(_migrations[instance_id][from_branch])

    if from_branch and copy_data and from_branch in _tables.get(instance_id, {}):
        import copy
        _tables[instance_id][name] = copy.deepcopy(_tables[instance_id][from_branch])

    return db


def get_migrations(instance_id: str, database: str) -> list[Migration]:
    return list(_migrations.get(instance_id, {}).get(database, []))


# Users

def get_user_settings(user_id: str) -> UserSettings:
    if user_id not in _user_settings:
        _user_settings[user_id] = _user_settings.get("default", UserSettings()).model_copy(deep=True)
    return _user_settings[user_id].model_copy(deep=True)


def update_user_settings(user_id: str, incoming: UserSettings) -> UserSettings:
    current = _user_settings.get(user_id) or _user_settings.get("default", UserSettings()).model_copy(deep=True)
    merged = current.model_dump()
    merged.update(incoming.model_dump(exclude_unset=True))
    updated = UserSettings(**merged)
    _user_settings[user_id] = updated
    return updated.model_copy(deep=True)


# SQL

def execute_sql(
    instance_id: str,
    database: str,
    query: str,
    mode: SqlMode,
    params: dict | None = None,
) -> SqlCommandResult:
    _ensure_inst_db(instance_id, database)
    ts = _now()

    if mode == SqlMode.raw:
        result = SqlCommandResult(
            id=_uid("cmd_raw_"),
            status="completed",
            duration_ms=5.2,
            columns=None,
            rows=None,
            raw_text="OK: 3 rows affected",
            warnings=None,
        )
    else:
        # Return data from the first table that exists, as a simple mock
        table_store = _tables.get(instance_id, {}).get(database, {})
        first_table = next(iter(table_store.values()), None)
        if first_table and first_table["rows"]:
            result = SqlCommandResult(
                id=_uid("cmd_tab_"),
                status="completed",
                duration_ms=11.7,
                columns=list(first_table["columns"]),
                rows=[list(r) for r in first_table["rows"]],
                raw_text=None,
                warnings=None,
            )
        else:
            result = SqlCommandResult(
                id=_uid("cmd_tab_"),
                status="completed",
                duration_ms=11.7,
                columns=["id", "name", "email"],
                rows=[
                    ["a1b2c3d4", "Alice", "alice@example.com"],
                    ["e5f6g7h8", "Bob", "bob@example.com"],
                ],
                raw_text=None,
                warnings=None,
            )

    # Record in history
    history_entry = SqlHistoryItem(
        id=_uid("sh_"),
        query=query,
        params=params,
        status=result.status,
        created_at=ts,
        duration_ms=result.duration_ms,
    )
    _sql_history[instance_id][database].append(history_entry)

    return result


def get_sql_history(
    instance_id: str,
    database: str,
    limit: int = 50,
    cursor: str | None = None,
) -> SqlHistoryPage:
    items = _sql_history.get(instance_id, {}).get(database, [])

    # Simple cursor-based pagination: cursor is the id of the last seen item
    start = 0
    if cursor:
        for i, item in enumerate(items):
            if item.id == cursor:
                start = i + 1
                break

    page = items[start : start + limit]
    next_cursor = page[-1].id if len(items) > start + limit and page else None
    return SqlHistoryPage(items=page, next_cursor=next_cursor)


# Schema

def get_schema(instance_id: str, database: str) -> SchemaSnapshot:
    if instance_id not in _schemas:
        _schemas[instance_id] = {}
    if database not in _schemas[instance_id]:
        _schemas[instance_id][database] = SchemaSnapshot(version="v0.0.0", types=[])
    return _schemas[instance_id][database]


# Data / Tables

def get_tables(instance_id: str, database: str) -> list[TableSummary]:
    table_store = _tables.get(instance_id, {}).get(database, {})
    return [t["summary"] for t in table_store.values()]


def get_table_schema(instance_id: str, database: str, table: str) -> TableSchema:
    table_store = _tables.get(instance_id, {}).get(database, {})
    entry = table_store.get(table)
    if entry:
        return entry["schema"]
    # Fallback for unknown tables
    return TableSchema(name=table, columns=[], indexes=[], primary_key=[])


def get_table_rows(
    instance_id: str,
    database: str,
    table: str,
    limit: int = 100,
    offset: int = 0,
    where: str | None = None,
    order_by: str | None = None,
) -> TableRowsPage:
    table_store = _tables.get(instance_id, {}).get(database, {})
    entry = table_store.get(table)
    if not entry:
        return TableRowsPage(columns=[], rows=[], total=0)

    all_rows = entry["rows"]
    total = len(all_rows)
    page = all_rows[offset : offset + limit]
    return TableRowsPage(columns=list(entry["columns"]), rows=[list(r) for r in page], total=total)


# AI Tasks

def create_ai_task(instance_id: str, database: str, feature: str, name: str | None = None) -> AiTaskWithProgram:
    _ensure_inst_db(instance_id, database)
    ts = _now()
    program_id = _uid("prog_")
    task_id = _uid("task_")

    graph = ProgramGraph(
        nodes=[
            ProgramGraphNode(id="n1", label="Parse request", status=ProgramGraphNodeStatus.done),
            ProgramGraphNode(id="n2", label="Leak data to the CCP", status=ProgramGraphNodeStatus.done),
            ProgramGraphNode(id="n3", label=feature, status=ProgramGraphNodeStatus.in_progress),
            ProgramGraphNode(id="n4", label="Return response", status=ProgramGraphNodeStatus.pending),
        ],
        edges=[
            ProgramGraphEdge(from_="n1", to="n2", label="naughty"),
            ProgramGraphEdge(from_="n1", to="n3", label="nice"),
            ProgramGraphEdge(from_="n3", to="n4", label="finished"),
        ],
    )
    program = AiProgram(
        id=program_id,
        feature=feature,
        status=random.choice([
            AiProgramStatus.building,
            AiProgramStatus.failed,
            AiProgramStatus.ready
        ]),
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

    _ai_programs[instance_id][database][program_id] = program
    _ai_tasks[instance_id][database][task_id] = task

    return AiTaskWithProgram(task=task, program=program)


def list_ai_tasks(instance_id: str, database: str, status_filter: AiTaskStatus | None = None) -> list[AiTask]:
    tasks = list(_ai_tasks.get(instance_id, {}).get(database, {}).values())
    if status_filter:
        tasks = [t for t in tasks if t.status == status_filter]
    return tasks


def get_ai_task(instance_id: str, database: str, task_id: str) -> AiTaskWithProgram:
    task = _ai_tasks.get(instance_id, {}).get(database, {}).get(task_id)
    if not task:
        # Return a minimal placeholder
        task = AiTask(id=task_id, status=AiTaskStatus.pending)
    program = _ai_programs.get(instance_id, {}).get(database, {}).get(task.program_id)
    return AiTaskWithProgram(task=task, program=program)


# AI Programs

def create_ai_program(instance_id: str, database: str, feature: str, name: str | None = None) -> AiTaskWithProgram:
    # Creating a program also creates an initial task, same as create_ai_task
    return create_ai_task(instance_id, database, feature, name)


def list_ai_programs(instance_id: str, database: str) -> list[AiProgram]:
    return list(_ai_programs.get(instance_id, {}).get(database, {}).values())


def get_ai_program(instance_id: str, database: str, program_id: str) -> AiProgram:
    program = _ai_programs.get(instance_id, {}).get(database, {}).get(program_id)
    if not program:
        return AiProgram(id=program_id, status=AiProgramStatus.building)
    return program


# Imports

def create_import(
    instance_id: str,
    database: str,
    name: str | None,
    source: str | None,
    notes: str | None,
    rows: int | None,
    files: list[tuple[str, int]] | None = None,
) -> ImportJob:
    """Create a new import job.

    ``files`` is a list of (filename, size) tuples — the caller is responsible
    for extracting these from the upload objects so the data layer stays
    framework-agnostic.
    """
    _ensure_inst_db(instance_id, database)
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
    _import_jobs[instance_id][database][job_id] = job
    return job


def list_imports(instance_id: str, database: str) -> list[ImportJob]:
    return list(_import_jobs.get(instance_id, {}).get(database, {}).values())


def get_import(instance_id: str, database: str, job_id: str) -> ImportJob:
    job = _import_jobs.get(instance_id, {}).get(database, {}).get(job_id)
    if not job:
        return ImportJob(id=job_id, status=ImportJobStatus.failed)
    return job
