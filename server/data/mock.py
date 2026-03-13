from __future__ import annotations

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


# Mock objects --------------------

_NOW = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
_EARLIER = datetime(2025, 6, 15, 11, 30, 0, tzinfo=timezone.utc)

_INSTANCES = [
    Instance(id="inst_001", name="Production"),
    Instance(id="inst_002", name="Staging"),
]

_DATABASES = [
    Database(name="main", last_migration="mig_003"),
    Database(name="feature-auth", last_migration="mig_001"),
]

_MIGRATIONS = [
    Migration(id="mig_001", name="0001-initial", parent_id=None),
    Migration(id="mig_002", name="0002-add-users", parent_id="mig_001"),
    Migration(id="mig_003", name="0003-add-orders", parent_id="mig_002"),
]

_USER_SETTINGS = UserSettings(
    theme="dark",
    language="en",
    editorFontSize=14,
    autoSave=True,
)

_COLUMNS = [
    TableColumn(name="id", type="uuid", nullable=False, default="gen_random_uuid()"),
    TableColumn(name="name", type="text", nullable=False, default=None),
    TableColumn(name="email", type="text", nullable=True, default=None),
    TableColumn(name="created_at", type="timestamptz", nullable=False, default="now()"),
]

_TABLE_SUMMARIES = [
    TableSummary(name="users", row_count=142, columns=_COLUMNS),
    TableSummary(
        name="orders",
        row_count=1089,
        columns=[
            TableColumn(name="id", type="uuid", nullable=False, default="gen_random_uuid()"),
            TableColumn(name="user_id", type="uuid", nullable=False, default=None),
            TableColumn(name="total", type="numeric(10,2)", nullable=False, default="0"),
            TableColumn(name="status", type="text", nullable=False, default="'pending'"),
        ],
    ),
]

_TABLE_SCHEMA = TableSchema(
    name="users",
    columns=_COLUMNS,
    indexes=[
        TableIndex(name="users_pkey", expression="id"),
        TableIndex(name="users_email_idx", expression="email"),
    ],
    primary_key=["id"],
)

_TABLE_ROWS = TableRowsPage(
    columns=["id", "name", "email", "created_at"],
    rows=[
        ["a1b2c3d4", "Alice", "alice@example.com", "2025-01-10T08:00:00Z"],
        ["e5f6g7h8", "Bob", "bob@example.com", "2025-02-14T10:30:00Z"],
        ["i9j0k1l2", "Charlie", None, "2025-03-20T15:45:00Z"],
    ],
    total=142,
)

_SCHEMA_SNAPSHOT = SchemaSnapshot(
    version="v1.2.0",
    types=[
        SchemaType(
            name="User",
            kind=SchemaTypeKind.table,
            module="default",
            references=[],
        ),
        SchemaType(
            name="Order",
            kind=SchemaTypeKind.table,
            module="default",
            references=[
                SchemaReference(target="User", type=SchemaReferenceType.link),
            ],
        ),
        SchemaType(
            name="OrderStatus",
            kind=SchemaTypeKind.enum,
            module="default",
            references=[],
        ),
        SchemaType(
            name="active_users",
            kind=SchemaTypeKind.view,
            module="default",
            references=[
                SchemaReference(target="User", type=SchemaReferenceType.dependency),
            ],
        ),
    ],
)

_SQL_HISTORY_ITEMS = [
    SqlHistoryItem(
        id="sh_001",
        query="SELECT * FROM users LIMIT 10",
        params=None,
        status="completed",
        created_at=_EARLIER,
        duration_ms=12.5,
    ),
    SqlHistoryItem(
        id="sh_002",
        query="INSERT INTO users (name, email) VALUES ('Dave', 'dave@example.com')",
        params=None,
        status="completed",
        created_at=_NOW,
        duration_ms=8.3,
    ),
]

_PROGRAM_GRAPH = ProgramGraph(
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

_AI_PROGRAM = AiProgram(
    id="prog_001",
    feature="Add a reviews table linked to orders",
    status=AiProgramStatus.building,
    created_at=_EARLIER,
    updated_at=_NOW,
    graph=_PROGRAM_GRAPH,
)

_AI_TASK = AiTask(
    id="task_001",
    program_id="prog_001",
    status=AiTaskStatus.running,
    prompt="Add a reviews table linked to orders",
    created_at=_EARLIER,
    completed_at=None,
)

_AI_TASK_WITH_PROGRAM = AiTaskWithProgram(
    task=_AI_TASK,
    program=_AI_PROGRAM,
)

_IMPORT_JOB = ImportJob(
    id="imp_001",
    name="Q2 customer data",
    source=ImportSource.csv_upload,
    status=ImportJobStatus.completed,
    progress=100.0,
    created_at=_EARLIER,
    completed_at=_NOW,
    updated_at=_NOW,
    rows=2500,
    notes="Imported from marketing CRM export",
    files=[
        ImportFile(filename="customers_q2.csv", size=184320),
        ImportFile(filename="customers_q2_extra.csv", size=51200),
    ],
)

_IMPORT_JOB_RUNNING = ImportJob(
    id="imp_002",
    name="Product catalogue sync",
    source=ImportSource.database_sync,
    status=ImportJobStatus.running,
    progress=63.0,
    created_at=_NOW,
    completed_at=None,
    updated_at=_NOW,
    rows=None,
    notes=None,
    files=[],
)


# Instances --------------------

def get_instances() -> list[Instance]:
    return list(_INSTANCES)


def get_databases(instance_id: str) -> list[Database]:
    return list(_DATABASES)


def create_database(instance_id: str, name: str, from_branch: str | None, copy_data: bool) -> Database:
    return Database(name=name, last_migration=None)


def get_migrations(instance_id: str, database: str) -> list[Migration]:
    return list(_MIGRATIONS)


# Users --------------------

def get_user_settings(user_id: str) -> UserSettings:
    return _USER_SETTINGS.model_copy()


def update_user_settings(user_id: str, incoming: UserSettings) -> UserSettings:
    merged = _USER_SETTINGS.model_dump()
    merged.update(incoming.model_dump(exclude_unset=True))
    return UserSettings(**merged)


# SQL --------------------

def execute_sql(instance_id: str, database: str, query: str, mode: SqlMode, params: dict | None = None) -> SqlCommandResult:
    if mode == SqlMode.raw:
        return SqlCommandResult(
            id="cmd_raw_001",
            status="completed",
            duration_ms=5.2,
            columns=None,
            rows=None,
            raw_text="OK: 3 rows affected",
            warnings=None,
        )
    return SqlCommandResult(
        id="cmd_tab_001",
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


def get_sql_history(instance_id: str, database: str, limit: int = 50, cursor: str | None = None) -> SqlHistoryPage:
    return SqlHistoryPage(items=list(_SQL_HISTORY_ITEMS), next_cursor=None)


# Schema --------------------

def get_schema(instance_id: str, database: str) -> SchemaSnapshot:
    return _SCHEMA_SNAPSHOT


# Data / Tables --------------------

def get_tables(instance_id: str, database: str) -> list[TableSummary]:
    return list(_TABLE_SUMMARIES)


def get_table_schema(instance_id: str, database: str, table: str) -> TableSchema:
    return _TABLE_SCHEMA


def get_table_rows(
    instance_id: str,
    database: str,
    table: str,
    limit: int = 100,
    offset: int = 0,
    where: str | None = None,
    order_by: str | None = None,
) -> TableRowsPage:
    return _TABLE_ROWS


# AI Tasks --------------------

def create_ai_task(instance_id: str, database: str, feature: str, name: str | None = None) -> AiTaskWithProgram:
    return _AI_TASK_WITH_PROGRAM


def list_ai_tasks(instance_id: str, database: str, status_filter: AiTaskStatus | None = None) -> list[AiTask]:
    if status_filter:
        return [t for t in [_AI_TASK] if t.status == status_filter]
    return [_AI_TASK]


def get_ai_task(instance_id: str, database: str, task_id: str) -> AiTaskWithProgram:
    return _AI_TASK_WITH_PROGRAM


# AI Programs

def create_ai_program(instance_id: str, database: str, feature: str, name: str | None = None) -> AiTaskWithProgram:
    return _AI_TASK_WITH_PROGRAM


def list_ai_programs(instance_id: str, database: str) -> list[AiProgram]:
    return [_AI_PROGRAM]


def get_ai_program(instance_id: str, database: str, program_id: str) -> AiProgram:
    return _AI_PROGRAM


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
    return ImportJob(
        id="imp_new",
        name=name or "Untitled import",
        source=ImportSource(source) if source else ImportSource.csv_upload,
        status=ImportJobStatus.running,
        progress=0.0,
        created_at=_NOW,
        completed_at=None,
        updated_at=_NOW,
        rows=rows,
        notes=notes,
        files=[ImportFile(filename=fn, size=sz) for fn, sz in files] if files else [],
    )


def list_imports(instance_id: str, database: str) -> list[ImportJob]:
    return [_IMPORT_JOB, _IMPORT_JOB_RUNNING]


def get_import(instance_id: str, database: str, job_id: str) -> ImportJob:
    return _IMPORT_JOB
