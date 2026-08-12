import asyncpg
import pytest

from queries import ANOMALY_QUERY


@pytest.mark.asyncio
async def test_anomaly_flagged_when_amount_exceeds_trailing_average(seeded_pool: asyncpg.Pool) -> None:
    rows = await seeded_pool.fetch(ANOMALY_QUERY)
    flagged = [r for r in rows if r["total_amount_cents"] > 3 * r["trailing_avg_cents"]]
    assert len(flagged) == 1
    assert flagged[0]["total_amount_cents"] == 10000