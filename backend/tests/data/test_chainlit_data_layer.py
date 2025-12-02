import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple
from urllib.parse import quote_plus

import asyncpg  # type: ignore[import-untyped]
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from chainlit.data.chainlit_data_layer import ChainlitDataLayer
from chainlit.element import File

load_dotenv()


SCHEMA_NAME = "chainlit"


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS "User" (
        "id" UUID PRIMARY KEY,
        "identifier" TEXT NOT NULL UNIQUE,
        "metadata" JSONB NOT NULL DEFAULT '{}'::JSONB,
        "createdAt" TIMESTAMP,
        "updatedAt" TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS "Thread" (
        "id" UUID PRIMARY KEY,
        "createdAt" TIMESTAMP,
        "updatedAt" TIMESTAMP,
        "deletedAt" TIMESTAMP,
        "name" TEXT,
        "userId" UUID REFERENCES "User"("id") ON DELETE SET NULL,
        "userIdentifier" TEXT,
        "tags" TEXT[],
        "metadata" JSONB NOT NULL DEFAULT '{}'::JSONB
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS "Step" (
        "id" UUID PRIMARY KEY,
        "threadId" UUID NOT NULL REFERENCES "Thread"("id") ON DELETE CASCADE,
        "parentId" UUID,
        "name" TEXT,
        "type" TEXT NOT NULL,
        "input" TEXT,
        "output" TEXT,
        "metadata" JSONB NOT NULL DEFAULT '{}'::JSONB,
        "tags" TEXT[],
        "createdAt" TIMESTAMP,
        "startTime" TIMESTAMP,
        "endTime" TIMESTAMP,
        "streaming" BOOLEAN,
        "waitForAnswer" BOOLEAN,
        "isError" BOOLEAN,
        "generation" JSONB,
        "showInput" TEXT,
        "language" TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS "Element" (
        "id" UUID PRIMARY KEY,
        "threadId" UUID REFERENCES "Thread"("id") ON DELETE CASCADE,
        "stepId" UUID REFERENCES "Step"("id") ON DELETE CASCADE,
        "metadata" JSONB NOT NULL DEFAULT '{}'::JSONB,
        "mime" TEXT,
        "name" TEXT NOT NULL,
        "objectKey" TEXT,
        "url" TEXT,
        "chainlitKey" TEXT,
        "display" TEXT,
        "size" TEXT,
        "language" TEXT,
        "page" INT,
        "props" JSONB,
        "autoPlay" BOOLEAN,
        "playerConfig" JSONB
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS "Feedback" (
        "id" UUID PRIMARY KEY,
        "stepId" UUID NOT NULL REFERENCES "Step"("id") ON DELETE CASCADE,
        "name" TEXT,
        "value" DOUBLE PRECISION,
        "comment" TEXT
    );
    """,
]


def _load_pg_settings() -> Dict[str, str]:
    keys = ("PG_HOST", "PG_PORT", "PG_USER", "PG_PASS")
    settings = {key: os.getenv(key) or "" for key in keys}

    missing = [key for key in keys if not settings[key]]
    if missing:
        raise RuntimeError(
            f"Missing PostgreSQL configuration values: {', '.join(missing)}"
        )

    return settings


def _build_dsn(config: Dict[str, str]) -> str:
    user = quote_plus(config["PG_USER"])
    password = quote_plus(config["PG_PASS"])
    host = config["PG_HOST"]
    port = config["PG_PORT"]
    options = quote_plus(f"-csearch_path={SCHEMA_NAME}")
    return f"postgresql://{user}:{password}@{host}:{port}/postgres?options={options}"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def _insert_user(conn: asyncpg.Connection) -> Tuple[str, str]:
    user_id = str(uuid.uuid4())
    identifier = f"user-{user_id[:8]}"
    now = _utc_now()
    await conn.execute(
        'INSERT INTO "User" ("id", "identifier", "metadata", "createdAt", "updatedAt") '
        "VALUES ($1, $2, $3, $4, $5)",
        user_id,
        identifier,
        json.dumps({}),
        now,
        now,
    )
    return user_id, identifier


async def _insert_thread(
    conn: asyncpg.Connection,
    user_id: str,
    user_identifier: str,
) -> str:
    thread_id = str(uuid.uuid4())
    now = _utc_now()
    await conn.execute(
        'INSERT INTO "Thread" ("id", "name", "userId", "userIdentifier", "createdAt", "updatedAt", "metadata") '
        "VALUES ($1, $2, $3, $4, $5, $6, $7)",
        thread_id,
        f"thread-{thread_id[:8]}",
        user_id,
        user_identifier,
        now,
        now,
        json.dumps({}),
    )
    return thread_id


async def _insert_step(
    conn: asyncpg.Connection,
    thread_id: str,
    *,
    show_input: str = "true",
    input_text: str = "",
    start_offset: int = 0,
) -> str:
    step_id = str(uuid.uuid4())
    base_time = _utc_now() + timedelta(seconds=start_offset)
    await conn.execute(
        """
        INSERT INTO "Step" (
            "id", "threadId", "name", "type", "input", "output", "metadata",
            "createdAt", "startTime", "endTime", "showInput"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """,
        step_id,
        thread_id,
        f"step-{step_id[:8]}",
        "run",
        input_text,
        "",
        json.dumps({}),
        base_time,
        base_time,
        base_time,
        show_input,
    )
    return step_id


async def _insert_element(
    conn: asyncpg.Connection,
    *,
    thread_id: str,
    step_id: str,
    props: Dict | None,
    url: str,
    object_key: str,
) -> str:
    element_id = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO "Element" (
            "id", "threadId", "stepId", "mime", "name", "objectKey", "url", "display", "props", "metadata"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        element_id,
        thread_id,
        step_id,
        "text/plain",
        f"element-{element_id[:8]}",
        object_key,
        url,
        "inline",
        json.dumps(props) if props is not None else None,
        json.dumps({"type": "file"}),
    )
    return element_id


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    settings = _load_pg_settings()
    return _build_dsn(settings)


@pytest_asyncio.fixture(scope="session", autouse=True)
async def ensure_schema(pg_dsn: str):
    conn = await asyncpg.connect(pg_dsn)
    try:
        for ddl in DDL_STATEMENTS:
            await conn.execute(ddl)
    finally:
        await conn.close()


@pytest_asyncio.fixture(autouse=True)
async def truncate_tables(pg_dsn: str):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            'TRUNCATE TABLE "Feedback", "Element", "Step", "Thread", "User" RESTART IDENTITY CASCADE'
        )
        yield
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db_conn(pg_dsn: str):
    conn = await asyncpg.connect(pg_dsn)
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def data_layer(pg_dsn: str, mock_storage_client):
    data_layer = ChainlitDataLayer(
        database_url=pg_dsn, storage_client=mock_storage_client
    )
    await data_layer.connect()
    try:
        yield data_layer
    finally:
        await data_layer.close()


@pytest.mark.asyncio
async def test_get_element_returns_empty_props_when_null(data_layer, db_conn):
    user_id, user_identifier = await _insert_user(db_conn)
    thread_id = await _insert_thread(db_conn, user_id, user_identifier)
    step_id = await _insert_step(db_conn, thread_id)
    element_id = await _insert_element(
        db_conn,
        thread_id=thread_id,
        step_id=step_id,
        props=None,
        url="https://example.com/existing",
        object_key="test/user/file.txt",
    )

    element = await data_layer.get_element(thread_id, element_id)

    assert element is not None
    assert element["props"] == {}


@pytest.mark.asyncio
async def test_create_element_reuses_existing_object_key(
    data_layer,
    db_conn,
    mock_storage_client,
    mock_chainlit_context,
):
    user_id, user_identifier = await _insert_user(db_conn)
    thread_id = await _insert_thread(db_conn, user_id, user_identifier)
    step_id = await _insert_step(db_conn, thread_id)

    mock_storage_client.upload_file.reset_mock()
    mock_storage_client.get_read_url.reset_mock()

    file_element = File(
        id=str(uuid.uuid4()),
        thread_id=thread_id,
        for_id=step_id,
        name="existing.txt",
        object_key="threads/pre-existing",
        url="https://example.com/pre-existing",
        mime="text/plain",
    )

    async with mock_chainlit_context:
        await data_layer.create_element(file_element)

    stored = await db_conn.fetchrow(
        'SELECT "objectKey", "url" FROM "Element" WHERE "id" = $1', file_element.id
    )

    assert stored is not None
    assert stored["objectKey"] == "threads/pre-existing"
    assert stored["url"] == "https://example.com/pre-existing"
    mock_storage_client.upload_file.assert_not_awaited()
    mock_storage_client.get_read_url.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_thread_normalizes_show_input(data_layer, db_conn):
    user_id, user_identifier = await _insert_user(db_conn)
    thread_id = await _insert_thread(db_conn, user_id, user_identifier)

    hidden_step_id = await _insert_step(
        db_conn,
        thread_id,
        show_input="false",
        input_text="hidden payload",
        start_offset=1,
    )
    visible_step_id = await _insert_step(
        db_conn,
        thread_id,
        show_input="true",
        input_text="visible payload",
        start_offset=2,
    )
    language_step_id = await _insert_step(
        db_conn,
        thread_id,
        show_input="json",
        input_text="language payload",
        start_offset=3,
    )

    thread = await data_layer.get_thread(thread_id)
    assert thread is not None

    steps = {step["id"]: step for step in thread["steps"]}

    hidden_step = steps[hidden_step_id]
    assert hidden_step["showInput"] is False
    assert hidden_step["input"] == ""

    visible_step = steps[visible_step_id]
    assert visible_step["showInput"] is True
    assert visible_step["input"] == "visible payload"

    language_step = steps[language_step_id]
    assert language_step["showInput"] == "json"
    assert language_step["input"] == "language payload"


@pytest.mark.asyncio
async def test_get_thread_refreshes_element_url(
    data_layer, db_conn, mock_storage_client
):
    user_id, user_identifier = await _insert_user(db_conn)
    thread_id = await _insert_thread(db_conn, user_id, user_identifier)
    step_id = await _insert_step(db_conn, thread_id)

    await _insert_element(
        db_conn,
        thread_id=thread_id,
        step_id=step_id,
        props=None,
        url="https://stale-url",
        object_key="threads/test-refresh",
    )

    mock_storage_client.get_read_url.return_value = "https://fresh-url"

    thread = await data_layer.get_thread(thread_id)
    assert thread is not None
    assert len(thread["elements"]) == 1

    element = thread["elements"][0]
    assert element["url"] == "https://fresh-url"
    assert element["props"] == {}
    mock_storage_client.get_read_url.assert_awaited()
