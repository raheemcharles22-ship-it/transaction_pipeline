import pathlib
from collections.abc import AsyncGenerator

import asyncpg
import pytest_asyncio
import pathlib
from testcontainers.postgres import PostgresContainer

MIGRATION_SQL = (pathlib.Path(__file__).parent.parent.parent / "migrations" / "001_init.sql").read_text()
SEED_SQL = """
INSERT INTO transactions (id, merchant_id, amount_cents, currency, occurred_at, idempotency_key) VALUES
('00000000-0000-0000-0000-000000000001', 'm1', 1000, 'USD', now() - interval '6 hours', 'k1'),
('00000000-0000-0000-0000-000000000002', 'm1', 1200, 'USD', now() - interval '5 hours', 'k2'),
('00000000-0000-0000-0000-000000000003', 'm1', 1100, 'USD', now() - interval '4 hours', 'k3'),
('00000000-0000-0000-0000-000000000004', 'm1', 900,  'USD', now() - interval '3 hours', 'k4'),
('00000000-0000-0000-0000-000000000005', 'm1', 1050, 'USD', now() - interval '2 hours', 'k5'),
('00000000-0000-0000-0000-000000000006', 'm1', 1150, 'USD', now() - interval '1 hour',  'k6'),
('00000000-0000-0000-0000-000000000007', 'm1', 10000, 'USD', now(), 'k7');
"""

@pytest_asyncio.fixture
async def seeded_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    with PostgresContainer("postgres:16-alpine") as pg:
        pool = await asyncpg.create_pool(dsn=pg.get_connection_url(driver=None))
        await pool.execute(MIGRATION_SQL)
        await pool.execute(SEED_SQL)
        yield pool
        await pool.close()