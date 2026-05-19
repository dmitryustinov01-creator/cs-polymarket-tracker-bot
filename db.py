"""
db.py — общий модуль PostgreSQL для обоих ботов
Использует asyncpg. Подключение через env var DATABASE_URL.
"""

import os
import asyncpg

DATABASE_URL = os.environ["DATABASE_URL"]

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool


async def init_db():
    """Создаёт все таблицы если не существуют. Вызывать при старте."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # ── Авто-бот ──────────────────────────────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auto_portfolio (
                id          INTEGER PRIMARY KEY DEFAULT 1,
                bank        DOUBLE PRECISION NOT NULL DEFAULT 100.0,
                bets        INTEGER NOT NULL DEFAULT 0,
                wins        INTEGER NOT NULL DEFAULT 0,
                losses      INTEGER NOT NULL DEFAULT 0,
                profit      DOUBLE PRECISION NOT NULL DEFAULT 0.0
            )
        """)
        # Гарантируем наличие единственной строки портфолио
        await conn.execute("""
            INSERT INTO auto_portfolio (id) VALUES (1)
            ON CONFLICT (id) DO NOTHING
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auto_open_bets (
                market_id       TEXT PRIMARY KEY,
                question        TEXT,
                market_url      TEXT,
                team            TEXT,
                opponent        TEXT,
                side_idx        INTEGER,
                rank            INTEGER,
                opp_rank        INTEGER,
                rank_diff       INTEGER,
                model_prob      DOUBLE PRECISION,
                market_prob     DOUBLE PRECISION,
                edge            DOUBLE PRECISION,
                bet_size        DOUBLE PRECISION,
                potential_payout DOUBLE PRECISION,
                opened_at       TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auto_closed_bets (
                id              SERIAL PRIMARY KEY,
                market_id       TEXT,
                question        TEXT,
                market_url      TEXT,
                team            TEXT,
                opponent        TEXT,
                side_idx        INTEGER,
                rank            INTEGER,
                opp_rank        INTEGER,
                rank_diff       INTEGER,
                model_prob      DOUBLE PRECISION,
                market_prob     DOUBLE PRECISION,
                edge            DOUBLE PRECISION,
                bet_size        DOUBLE PRECISION,
                potential_payout DOUBLE PRECISION,
                opened_at       TIMESTAMPTZ,
                won             BOOLEAN,
                profit          DOUBLE PRECISION,
                closed_at       TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # ── Старый бот (tracker) ──────────────────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id     BIGINT PRIMARY KEY
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS known_markets (
                market_id   TEXT PRIMARY KEY
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                chat_id         BIGINT,
                market_id       TEXT,
                question        TEXT,
                chosen_idx      INTEGER,
                chosen_team     TEXT,
                entry_price     DOUBLE PRECISION,
                last_price      DOUBLE PRECISION,
                market_url      TEXT,
                ts              TIMESTAMPTZ DEFAULT NOW(),
                end_dt          TEXT,
                outcome         TEXT,
                PRIMARY KEY (chat_id, market_id)
            )
        """)


# ═══════════════════════════════════════════════════════════════
#  АВТО-БОТ: функции работы с портфолио
# ═══════════════════════════════════════════════════════════════

async def get_portfolio() -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM auto_portfolio WHERE id=1")
        open_bets = await conn.fetch("SELECT * FROM auto_open_bets")

    return {
        "bank": row["bank"],
        "open": {
            r["market_id"]: dict(r) for r in open_bets
        },
        "stats": {
            "bets":   row["bets"],
            "wins":   row["wins"],
            "losses": row["losses"],
            "profit": row["profit"],
        },
    }


async def open_bet(market_id: str, bet: dict, bank_after: float):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO auto_open_bets
            (market_id, question, market_url, team, opponent, side_idx,
             rank, opp_rank, rank_diff, model_prob, market_prob, edge,
             bet_size, potential_payout)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        """,
            market_id,
            bet["question"], bet["market_url"],
            bet["team"], bet["opponent"], bet["side_idx"],
            bet["rank"], bet["opp_rank"], bet["rank_diff"],
            bet["model_prob"], bet["market_prob"], bet["edge"],
            bet["bet_size"], bet["potential_payout"],
        )
        await conn.execute("""
            UPDATE auto_portfolio
            SET bank=$1, bets=bets+1
            WHERE id=1
        """, bank_after)


async def close_bet(market_id: str, won: bool) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM auto_open_bets WHERE market_id=$1", market_id
        )
        if not row:
            return None

        bet = dict(row)
        profit = round(bet["potential_payout"] - bet["bet_size"], 4) if won \
                 else -bet["bet_size"]
        payout = bet["potential_payout"] if won else 0.0

        await conn.execute("""
            INSERT INTO auto_closed_bets
            (market_id, question, market_url, team, opponent, side_idx,
             rank, opp_rank, rank_diff, model_prob, market_prob, edge,
             bet_size, potential_payout, opened_at, won, profit)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        """,
            market_id,
            bet["question"], bet["market_url"],
            bet["team"], bet["opponent"], bet["side_idx"],
            bet["rank"], bet["opp_rank"], bet["rank_diff"],
            bet["model_prob"], bet["market_prob"], bet["edge"],
            bet["bet_size"], bet["potential_payout"], bet["opened_at"],
            won, profit,
        )

        await conn.execute(
            "DELETE FROM auto_open_bets WHERE market_id=$1", market_id
        )

        if won:
            await conn.execute("""
                UPDATE auto_portfolio
                SET bank=bank+$1, wins=wins+1, profit=profit+$2
                WHERE id=1
            """, payout, profit)
        else:
            await conn.execute("""
                UPDATE auto_portfolio
                SET losses=losses+1, profit=profit+$1
                WHERE id=1
            """, profit)

        bet["won"]    = won
        bet["profit"] = profit
        return bet


async def get_closed_bets(limit: int = 10) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM auto_closed_bets ORDER BY closed_at DESC LIMIT $1",
            limit,
        )
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════
#  СТАРЫЙ БОТ (tracker): subscribers, known_markets, predictions
# ═══════════════════════════════════════════════════════════════

async def get_subscribers() -> set:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id FROM subscribers")
    return {r["chat_id"] for r in rows}


async def add_subscriber(chat_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO subscribers (chat_id) VALUES ($1) ON CONFLICT DO NOTHING",
            chat_id,
        )


async def remove_subscriber(chat_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM subscribers WHERE chat_id=$1", chat_id
        )


async def get_known_markets() -> set:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT market_id FROM known_markets")
    return {r["market_id"] for r in rows}


async def add_known_market(market_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO known_markets (market_id) VALUES ($1) ON CONFLICT DO NOTHING",
            market_id,
        )


async def get_user_predictions(chat_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM predictions WHERE chat_id=$1", chat_id
        )
    return {r["market_id"]: dict(r) for r in rows}


async def get_all_pending_predictions() -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM predictions WHERE outcome IS NULL"
        )
    return [dict(r) for r in rows]


async def save_prediction(chat_id: int, market_id: str, pred: dict):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO predictions
            (chat_id, market_id, question, chosen_idx, chosen_team,
             entry_price, last_price, market_url, end_dt)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (chat_id, market_id) DO UPDATE
            SET last_price=$6, outcome=$10
        """,
            chat_id, market_id,
            pred["question"], pred["chosen_idx"], pred["chosen_team"],
            pred["entry_price"], pred.get("last_price", pred["entry_price"]),
            pred["market_url"], pred.get("end_dt", ""),
            pred.get("outcome"),
        )


async def update_prediction_price(chat_id: int, market_id: str, price: float):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE predictions SET last_price=$1 WHERE chat_id=$2 AND market_id=$3",
            price, chat_id, market_id,
        )


async def update_prediction_outcome(chat_id: int, market_id: str, outcome: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE predictions SET outcome=$1 WHERE chat_id=$2 AND market_id=$3",
            outcome, chat_id, market_id,
        )
