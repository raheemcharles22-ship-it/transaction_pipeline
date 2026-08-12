from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException

from models import Anomaly, MerchantSummary
from queries import ANOMALY_QUERY, SUMMARY_QUERY

DB_DSN = "postgres://pipeline:pipeline@localhost:5432/transactions"
ANOMALY_THRESHOLD = 3.0



@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=10)
    yield
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}

@app.get("/debug/count")
async def debug_count() -> dict[str, int]:
    async with app.state.pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM transactions")
    return {"transaction_count": count}

@app.get("/merchants/{merchant_id}/summary", response_model=list[MerchantSummary])
async def merchant_summary(merchant_id: str) -> list[MerchantSummary]:
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(SUMMARY_QUERY, merchant_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"no data for merchant {merchant_id}")
    return [MerchantSummary(**dict(row)) for row in rows]

@app.get("/anomalies", response_model=list[Anomaly])
async def anomalies() -> list[Anomaly]:
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(ANOMALY_QUERY)

    flagged = []

    for row in rows:
        trailing_avg = row["trailing_avg_cents"]
        total_cents = row["total_amount_cents"]
        if total_cents is None:
            continue 
        if trailing_avg is None:
            continue
        trailing_avg = float(trailing_avg)
        total_cents = int(total_cents)
        if trailing_avg and total_cents > ANOMALY_THRESHOLD * trailing_avg:
            flagged.append(Anomaly(
                merchant_id=row["merchant_id"],
                window_start=row["window_start"],
                amount_cents= total_cents,
                trailing_avg_cents=trailing_avg,
                ratio= total_cents / trailing_avg,
            ))
    return flagged