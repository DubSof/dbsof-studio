from __future__ import annotations

from fastapi import FastAPI, File, Form, Path, Query, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware

from data import mock as data
from data.schema import (
    AiProgram,
    AiProgramCreate,
    AiTaskStatus,
    AiTaskWithProgram,
    AiTask,
    Database,
    DatabaseCreate,
    ImportJob,
    Instance,
    Migration,
    SchemaSnapshot,
    SqlCommandRequest,
    SqlCommandResult,
    SqlHistoryPage,
    TableRowsPage,
    TableSchema,
    TableSummary,
    UserSettings,
)

app = FastAPI(
    title="UI Template API",
    version="1.0.0",
    description="Minimal CRUD-style endpoints to support SQL execution, schema exploration, data browsing, and AI task flows.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Instances

@app.get("/instances", tags=["Instances"], summary="List instances")
async def list_instances() -> list[Instance]:
    return data.get_instances()


@app.get(
    "/instances/{instanceId}/databases",
    tags=["Instances"],
    summary="List databases (branches) for an instance",
)
async def list_databases(instanceId: str = Path(...)) -> list[Database]:
    return data.get_databases(instanceId)


@app.post(
    "/instances/{instanceId}/databases",
    tags=["Instances"],
    summary="Create a new database (branch)",
    status_code=status.HTTP_201_CREATED,
)
async def create_database(
    body: DatabaseCreate,
    instanceId: str = Path(...),
) -> Database:
    return data.create_database(instanceId, body.name, body.from_branch, body.copy_data)


@app.get(
    "/instances/{instanceId}/databases/{database}/migrations",
    tags=["Instances"],
    summary="Get migration history for a database (branch)",
)
async def list_migrations(
    instanceId: str = Path(...),
    database: str = Path(...),
) -> list[Migration]:
    return data.get_migrations(instanceId, database)


# Users

@app.get(
    "/users/{userId}/settings",
    tags=["Users"],
    summary="Get user settings",
)
async def get_user_settings(userId: str = Path(...)) -> UserSettings:
    return data.get_user_settings(userId)


@app.put(
    "/users/{userId}/settings",
    tags=["Users"],
    summary="Update user settings",
)
async def update_user_settings(
    body: UserSettings,
    userId: str = Path(...),
) -> UserSettings:
    return data.update_user_settings(userId, body)


# SQL

@app.post(
    "/instances/{instanceId}/databases/{database}/sql/commands",
    tags=["SQL"],
    summary="Execute a SQL command (REPL/editor)",
)
async def execute_sql(
    body: SqlCommandRequest,
    instanceId: str = Path(...),
    database: str = Path(...),
) -> SqlCommandResult:
    return data.execute_sql(instanceId, database, body.query, body.mode, body.params)


@app.get(
    "/instances/{instanceId}/databases/{database}/sql/history",
    tags=["SQL"],
    summary="Fetch SQL command history",
)
async def get_sql_history(
    instanceId: str = Path(...),
    database: str = Path(...),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None),
) -> SqlHistoryPage:
    return data.get_sql_history(instanceId, database, limit, cursor)


# Schema

@app.get(
    "/instances/{instanceId}/databases/{database}/schema",
    tags=["Schema"],
    summary="Get database schema (for text/graph views)",
)
async def get_schema(
    instanceId: str = Path(...),
    database: str = Path(...),
) -> SchemaSnapshot:
    return data.get_schema(instanceId, database)


# Data / Tables

@app.get(
    "/instances/{instanceId}/databases/{database}/tables",
    tags=["Data"],
    summary="List tables for data explorer",
)
async def list_tables(
    instanceId: str = Path(...),
    database: str = Path(...),
) -> list[TableSummary]:
    return data.get_tables(instanceId, database)


@app.get(
    "/instances/{instanceId}/databases/{database}/tables/{table}/schema",
    tags=["Data"],
    summary="Get table schema",
)
async def get_table_schema(
    instanceId: str = Path(...),
    database: str = Path(...),
    table: str = Path(...),
) -> TableSchema:
    return data.get_table_schema(instanceId, database, table)


@app.get(
    "/instances/{instanceId}/databases/{database}/tables/{table}/rows",
    tags=["Data"],
    summary="Fetch table rows for explorer",
)
async def get_table_rows(
    instanceId: str = Path(...),
    database: str = Path(...),
    table: str = Path(...),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    where: str | None = Query(None),
    orderBy: str | None = Query(None),
) -> TableRowsPage:
    return data.get_table_rows(instanceId, database, table, limit, offset, where, orderBy)


# AI Tasks

@app.post(
    "/instances/{instanceId}/databases/{database}/ai/tasks",
    tags=["AI"],
    summary="Create a new program build task (alias)",
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_ai_task(
    body: AiProgramCreate,
    instanceId: str = Path(...),
    database: str = Path(...),
) -> AiTaskWithProgram:
    return data.create_ai_task(instanceId, database, body.feature, body.name)


@app.get(
    "/instances/{instanceId}/databases/{database}/ai/tasks",
    tags=["AI"],
    summary="List AI tasks",
)
async def list_ai_tasks(
    instanceId: str = Path(...),
    database: str = Path(...),
    status_filter: AiTaskStatus | None = Query(None, alias="status"),
) -> list[AiTask]:
    return data.list_ai_tasks(instanceId, database, status_filter)


@app.get(
    "/instances/{instanceId}/databases/{database}/ai/tasks/{taskId}",
    tags=["AI"],
    summary="Get AI task status/output",
)
async def get_ai_task(
    instanceId: str = Path(...),
    database: str = Path(...),
    taskId: str = Path(...),
) -> AiTaskWithProgram:
    return data.get_ai_task(instanceId, database, taskId)


# AI Programs

@app.post(
    "/instances/{instanceId}/databases/{database}/ai/programs",
    tags=["AI"],
    summary="Create a new program build request",
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_ai_program(
    body: AiProgramCreate,
    instanceId: str = Path(...),
    database: str = Path(...),
) -> AiTaskWithProgram:
    return data.create_ai_program(instanceId, database, body.feature, body.name)


@app.get(
    "/instances/{instanceId}/databases/{database}/ai/programs",
    tags=["AI"],
    summary="List programs",
)
async def list_ai_programs(
    instanceId: str = Path(...),
    database: str = Path(...),
) -> list[AiProgram]:
    return data.list_ai_programs(instanceId, database)


@app.get(
    "/instances/{instanceId}/databases/{database}/ai/programs/{programId}",
    tags=["AI"],
    summary="Get program details/graph",
)
async def get_ai_program(
    instanceId: str = Path(...),
    database: str = Path(...),
    programId: str = Path(...),
) -> AiProgram:
    return data.get_ai_program(instanceId, database, programId)


# Imports

@app.post(
    "/instances/{instanceId}/databases/{database}/imports",
    tags=["Imports"],
    summary="Create a new import job",
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_import(
    instanceId: str = Path(...),
    database: str = Path(...),
    name: str | None = Form(None),
    source: str | None = Form(None),
    notes: str | None = Form(None),
    rows: int | None = Form(None),
    files: list[UploadFile] | None = File(None),
) -> ImportJob:
    file_tuples = (
        [(f.filename or "unknown", f.size or 0) for f in files] if files else None
    )
    return data.create_import(instanceId, database, name, source, notes, rows, file_tuples)


@app.get(
    "/instances/{instanceId}/databases/{database}/imports",
    tags=["Imports"],
    summary="List import jobs for a database",
)
async def list_imports(
    instanceId: str = Path(...),
    database: str = Path(...),
) -> list[ImportJob]:
    return data.list_imports(instanceId, database)


@app.get(
    "/instances/{instanceId}/databases/{database}/imports/{jobId}",
    tags=["Imports"],
    summary="Get import job details",
)
async def get_import(
    instanceId: str = Path(...),
    database: str = Path(...),
    jobId: str = Path(...),
) -> ImportJob:
    return data.get_import(instanceId, database, jobId)
