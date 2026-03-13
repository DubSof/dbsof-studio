from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# Instances --------------------

class Instance(BaseModel):
    id: str | None = None
    name: str | None = None


class Database(BaseModel):
    name: str | None = None
    last_migration: str | None = Field(None, alias="lastMigration")

    model_config = {"populate_by_name": True}


class DatabaseCreate(BaseModel):
    name: str = Field(..., pattern=r"^[^@][^@]*$", description="Name of the database/branch to create")
    from_branch: str | None = Field(None, alias="fromBranch", description="Optional source branch to copy from")
    copy_data: bool = Field(False, alias="copyData", description="If true and fromBranch is provided, copy data in addition to schema")

    model_config = {"populate_by_name": True}


class Migration(BaseModel):
    id: str | None = Field(None, description="Unique migration identifier")
    name: str | None = Field(None, description='Migration name (e.g., "0001-demo")')
    parent_id: str | None = Field(None, alias="parentId", description="ID of the parent migration, null for root migrations")

    model_config = {"populate_by_name": True}


# Users --------------------

class UserSettings(BaseModel):
    """User settings object. Can contain any key-value pairs for user preferences."""

    model_config = {"extra": "allow", "populate_by_name": True}


# SQL --------------------

class SqlMode(str, Enum):
    raw = "raw"
    tabular = "tabular"


class SqlParamValue(BaseModel):
    type_name: str | None = Field(None, alias="typeName")
    value: Any = None

    model_config = {"populate_by_name": True}


class SqlCommandRequest(BaseModel):
    query: str = Field(..., description="SQL text to execute")
    params: dict[str, SqlParamValue] | None = None
    mode: SqlMode = Field(SqlMode.tabular, description="raw = REPL/plain text; tabular = structured rows")


class SqlCommandResult(BaseModel):
    id: str | None = None
    status: str | None = None
    duration_ms: float | None = Field(None, alias="durationMs")
    columns: list[str] | None = None
    rows: list[list[Any]] | None = None
    raw_text: str | None = Field(None, alias="rawText", description="Present when mode=raw")
    warnings: list[str] | None = None

    model_config = {"populate_by_name": True}


class SqlHistoryItem(BaseModel):
    id: str | None = None
    query: str | None = None
    params: dict[str, Any] | None = None
    status: str | None = None
    created_at: datetime | None = Field(None, alias="createdAt")
    duration_ms: float | None = Field(None, alias="durationMs")

    model_config = {"populate_by_name": True}


class SqlHistoryPage(BaseModel):
    """Paginated SQL history response."""
    items: list[SqlHistoryItem] = []
    next_cursor: str | None = Field(None, alias="nextCursor")

    model_config = {"populate_by_name": True}


# Schema --------------------

class SchemaReferenceType(str, Enum):
    link = "link"
    dependency = "dependency"


class SchemaReference(BaseModel):
    target: str | None = None
    type: SchemaReferenceType | None = None


class SchemaTypeKind(str, Enum):
    table = "table"
    view = "view"
    function = "function"
    scalar = "scalar"
    enum = "enum"


class SchemaType(BaseModel):
    name: str | None = None
    kind: SchemaTypeKind | None = None
    module: str | None = None
    references: list[SchemaReference] | None = None


class SchemaSnapshot(BaseModel):
    types: list[SchemaType] | None = None
    version: str | None = None


# Data / Tables --------------------

class TableColumn(BaseModel):
    name: str | None = None
    type: str | None = None
    nullable: bool | None = None
    default: str | None = None


class TableSummary(BaseModel):
    name: str | None = None
    row_count: int | None = Field(None, alias="rowCount")
    columns: list[TableColumn] | None = None

    model_config = {"populate_by_name": True}


class TableIndex(BaseModel):
    name: str | None = None
    expression: str | None = None


class TableSchema(BaseModel):
    name: str | None = None
    columns: list[TableColumn] | None = None
    indexes: list[TableIndex] | None = None
    primary_key: list[str] | None = Field(None, alias="primaryKey")

    model_config = {"populate_by_name": True}


class TableRowsPage(BaseModel):
    columns: list[str] | None = None
    rows: list[list[Any]] | None = None
    total: int | None = None


# AI --------------------

class ProgramGraphNodeStatus(str, Enum):
    pending = "pending"
    in_progress = "in-progress"
    done = "done"


class ProgramGraphNode(BaseModel):
    id: str | None = None
    label: str | None = None
    status: ProgramGraphNodeStatus | None = None


class ProgramGraphEdge(BaseModel):
    from_: str | None = Field(None, alias="from")
    to: str | None = None
    label: str | None = None

    model_config = {"populate_by_name": True}


class ProgramGraph(BaseModel):
    nodes: list[ProgramGraphNode] | None = None
    edges: list[ProgramGraphEdge] | None = None


class AiProgramCreate(BaseModel):
    feature: str = Field(..., description="User request / feature description")
    name: str | None = Field(None, description="Optional friendly name")


class AiTaskStatus(str, Enum):
    pending = "pending"
    running = "running"
    failed = "failed"
    completed = "completed"


class AiTask(BaseModel):
    id: str | None = None
    program_id: str | None = Field(None, alias="programId")
    status: AiTaskStatus | None = None
    prompt: str | None = None
    created_at: datetime | None = Field(None, alias="createdAt")
    completed_at: datetime | None = Field(None, alias="completedAt")

    model_config = {"populate_by_name": True}


class AiProgramStatus(str, Enum):
    building = "building"
    ready = "ready"
    failed = "failed"


class AiProgram(BaseModel):
    id: str | None = None
    feature: str | None = None
    status: AiProgramStatus | None = None
    created_at: datetime | None = Field(None, alias="createdAt")
    updated_at: datetime | None = Field(None, alias="updatedAt")
    graph: ProgramGraph | None = None

    model_config = {"populate_by_name": True}


class AiTaskWithProgram(BaseModel):
    task: AiTask | None = None
    program: AiProgram | None = None


# Imports --------------------

class ImportSource(str, Enum):
    csv_upload = "CSV upload"
    json_upload = "JSON upload"
    api_pull = "API pull"
    database_sync = "Database sync"
    upload = "upload"
    manual = "manual"


class ImportJobStatus(str, Enum):
    running = "running"
    completed = "completed"
    failed = "failed"


class ImportFile(BaseModel):
    filename: str | None = None
    size: int | None = Field(None, description="File size in bytes")


class ImportJob(BaseModel):
    id: str | None = Field(None, description="Unique job identifier")
    name: str | None = Field(None, description="Import job name")
    source: ImportSource | None = Field(None, description="Source of the import")
    status: ImportJobStatus | None = Field(None, description="Current job status")
    progress: float | None = Field(None, ge=0, le=100, description="Progress percentage (0-100)")
    created_at: datetime | None = Field(None, alias="createdAt", description="Job creation timestamp")
    completed_at: datetime | None = Field(None, alias="completedAt", description="Job completion timestamp")
    updated_at: datetime | None = Field(None, alias="updatedAt", description="Last update timestamp")
    rows: int | None = Field(None, description="Number of rows imported")
    notes: str | None = Field(None, description="Optional notes about the import")
    files: list[ImportFile] | None = Field(None, description="List of files associated with the import")

    model_config = {"populate_by_name": True}


# Generic error response --------------------

class ErrorResponse(BaseModel):
    error: str
