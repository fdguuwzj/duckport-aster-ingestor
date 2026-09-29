"""
Aster historical kline loader via REST pagination.

Aster has no data.binance.vision bulk dump, so loadhist pages
GET /fapi/v1/klines (limit 1500) and writes through DuckportClient.

Usage:
    python -m aster_ingestor.loadhist
    loadhist
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pandas as pd

from aster_ingestor.config import (
    DATA_SOURCES,
    DUCKPORT_ADDR,
    DUCKPORT_SCHEMA,
    FETCH_CONCURRENCY,
    GENESIS_TIME,
    KLINE_INTERVAL,
    KLINE_LIMIT,
    KLINE_PANDAS_FREQ,
    START_DATE,
)
from aster_ingestor.data_jobs import KLINE_MARKETS
from aster_ingestor.duckport_client import DuckportClient
from aster_ingestor.bus import TRADE_TYPE_MAP
from aster_ingestor.component.candle_fetcher import BinanceFetcher, OptimizedKlineFetcher
from aster_ingestor.utils import create_aiohttp_session
from aster_ingestor.utils.log_kit import divider, logger

BOUNDARY_DAYS = int(os.getenv("LOADHIST_BOUNDARY_DAYS", "1"))


def get_enabled_kline_markets():
    return [market for market in KLINE_MARKETS if market in DATA_SOURCES]


def init_remote_schema(client: DuckportClient):
    client.init_schema(
        markets=list(KLINE_MARKETS),
        interval=KLINE_INTERVAL,
        data_sources=DATA_SOURCES,
        start_date=START_DATE,
    )
    client.verify_kline_interval(KLINE_INTERVAL)


def _boundary_time(client: DuckportClient, market: str) -> pd.Timestamp:
    duck_time = client.read_duck_time(market)
    if duck_time is None or duck_time == GENESIS_TIME:
        return pd.Timestamp(START_DATE).tz_localize(timezone.utc)
    return duck_time - timedelta(days=BOUNDARY_DAYS)


async def _ping_aster():
    async with create_aiohttp_session(10) as session:
        fetcher = BinanceFetcher("aster_perp", session)
        ts, weight = await fetcher.get_time_and_weight()
        logger.info(f"Aster REST ok, serverTime={ts}, weight={weight}")


async def _load_market(client: DuckportClient, market: str):
    start_time = _boundary_time(client, market)
    current_time = pd.Timestamp.now(tz=timezone.utc).floor(KLINE_PANDAS_FREQ)
    logger.info(f"{market} REST backfill from {start_time} to {current_time}")

    async with create_aiohttp_session(10) as session:
        fetcher = BinanceFetcher(market, session)
        exginfo = await fetcher.get_exchange_info()
        symbols = TRADE_TYPE_MAP[market][0](exginfo)
        logger.info(f"{market} trading symbols={len(symbols)}")
        optimized = OptimizedKlineFetcher(fetcher, max_concurrent=FETCH_CONCURRENCY)
        results = await optimized.get_all_klines(
            symbols,
            start_time=start_time,
            interval=KLINE_INTERVAL,
            limit=KLINE_LIMIT,
        )

    successful = [item["data"] for item in results if item.get("success") and item.get("num", 0) > 0]
    failed = [item for item in results if not item.get("success")]
    if failed:
        logger.warning(f"{market} failed symbols={len(failed)}")
        for item in failed[:10]:
            logger.warning(f"  {item.get('symbol')}: {item.get('error')}")
    if not successful:
        logger.warning(f"{market} no klines fetched, skip write")
        return

    df = pd.concat(successful, ignore_index=True)
    df.sort_values(["open_time", "symbol"], inplace=True)
    df = df[(df["open_time"] >= start_time) & (df["open_time"] < current_time)]
    if df.empty:
        logger.warning(f"{market} filtered frame empty")
        return

    if hasattr(df["open_time"].dt, "tz") and df["open_time"].dt.tz is not None:
        write_df = df.copy()
        write_df["open_time"] = write_df["open_time"].dt.tz_localize(None)
    else:
        write_df = df

    import pyarrow as pa
    arrow = pa.Table.from_pandas(write_df, preserve_index=False)
    new_max = pd.to_datetime(df["open_time"].max())
    if getattr(new_max, "tzinfo", None) is not None:
        new_max = new_max.tz_localize(None)
    client.bulk_write_kline(
        arrow,
        market,
        KLINE_INTERVAL,
        new_max.strftime("%Y-%m-%d %H:%M:%S"),
        sync_duck_time=False,
    )
    client.replace_duck_time(market, new_max.strftime("%Y-%m-%d %H:%M:%S"))
    logger.ok(f"{market} wrote {len(df)} rows, duck_time={new_max}")


def main():
    os.environ["TZ"] = "UTC"
    divider("Aster loadhist")
    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA, interval=KLINE_INTERVAL)
    try:
        info = client.ping()
        logger.info(f"duckport-rs server: {info}")
    except Exception as exc:
        logger.error(f"Cannot reach duckport-rs at {DUCKPORT_ADDR}: {exc}")
        raise SystemExit(1) from exc

    init_remote_schema(client)
    asyncio.run(_ping_aster())

    markets = get_enabled_kline_markets()
    if not markets:
        logger.error("DATA_SOURCES 未包含 aster_perp/aster_spot")
        client.close()
        raise SystemExit(1)

    for market in markets:
        asyncio.run(_load_market(client, market))

    client.close()
    logger.ok("Aster loadhist done")


if __name__ == "__main__":
    main()
