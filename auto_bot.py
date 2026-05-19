"""
CS2 Auto Prediction Bot
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Автоматически находит CS2 матчи на Polymarket,
оценивает вероятность через Elo-модель (HLTV),
делает бумажные ставки по Quarter Kelly.

Env vars:
  BOT_TOKEN   — токен бота
  CHAT_ID     — твой chat id (бот пишет только тебе)
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
)

# ─── CONFIG ──────────────────────────────────────────────────────────────────

BOT_TOKEN  = os.environ["BOT_TOKEN"]
CHAT_ID    = int(os.environ["CHAT_ID"])

DATA_FILE          = "auto_portfolio.json"
SCAN_INTERVAL      = 900    # 15 мин между сканами
RESULT_INTERVAL    = 300    # 5 мин проверка результатов
HLTV_INTERVAL      = 3600   # 1ч обновление HLTV

STARTING_BANK      = 100.0
MIN_EDGE           = 0.10   # минимум 10 центов edge
MAX_KELLY_FRACTION = 0.25   # Quarter Kelly
MAX_BET_SIZE       = 20.0   # лимит на одну ставку $
MIN_BET_SIZE       = 3.0    # минимум $3
MIN_VOLUME         = 1000   # минимальный объём рынка $
MAX_OPEN_BETS      = 6      # макс открытых позиций
MIN_RANK_DIFF      = 5      # минимальная разница рангов для сигнала

GAMMA_API  = "https://gamma-api.polymarket.com"
CS2_TAGS   = ["counter-strike", "cs2", "esports"]
CS2_WORDS  = ["counter-strike", "cs2", "csgo", "cs:go"]

EXCLUDE_WORDS = [
    "map 1", "map 2", "map 3", "map 4", "map 5",
    "first map", "pistol", "knife", "games total",
    "o/u", "over/under", "first blood", "first kill",
    "ace", "bomb", "most kills", "handicap",
    "signs for", "signs with", "will valve",
    "map pool", "which maps", "what will", "how many",
]

FALLBACK_RANKING = {
    "vitality": 1, "natus vincere": 2, "navi": 2,
    "faze": 3, "faze clan": 3, "g2": 4, "g2 esports": 4,
    "spirit": 5, "team spirit": 5, "liquid": 6, "team liquid": 6,
    "mouz": 7, "heroic": 8, "astralis": 9, "nip": 10,
    "complexity": 11, "ence": 12, "cloud9": 13, "big": 14,
    "eternal fire": 15, "fnatic": 16, "pain": 17, "3dmax": 18,
    "mibr": 19, "virtus.pro": 21, "flyquest": 22, "monte": 23,
    "saw": 24, "apeks": 25, "b8": 26, "betboom": 27,
    "betboom team": 27, "parivision": 28, "aurora": 29,
    "100 thieves": 30, "falcons": 31, "team falcons": 31,
    "furia": 32,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

# Глобальное состояние (из HLTV)
hltv_ranking:      dict = {}
hltv_last_updated: datetime | None = None


# ─── PORTFOLIO ────────────────────────────────────────────────────────────────

def load_portfolio() -> dict:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {
        "bank":      STARTING_BANK,
        "open":      {},     # market_id -> bet dict
        "closed":    [],     # list of closed bets
        "stats": {
            "bets": 0, "wins": 0, "losses": 0, "profit": 0.0,
        },
    }

def save_portfolio(p: dict):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(p, f, indent=2, ensure_ascii=False)


# ─── ELO MODEL ───────────────────────────────────────────────────────────────

def rank_to_elo(rank: int) -> float:
    """
    Конвертирует HLTV ранг в Elo.
    #1 = 2000, каждый следующий ранг -30 Elo.
    """
    return max(200.0, 2000.0 - (rank - 1) * 30)

def elo_win_prob(elo_a: float, elo_b: float) -> float:
    """Вероятность победы A над B по формуле Elo."""
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))

def get_rank(name: str) -> int | None:
    """Ищет ранг команды в HLTV или fallback."""
    ranking = hltv_ranking if hltv_ranking else FALLBACK_RANKING
    n = name.strip().lower()

    if n in ranking:
        return ranking[n]

    for key, rank in ranking.items():
        if len(key) >= 4:
            if key in n and len(n) - len(key) <= 4:
                return rank
            if n in key and len(key) - len(n) <= 4:
                return rank
    return None

def quarter_kelly(prob_model: float, prob_market: float, bank: float) -> float:
    """
    Размер ставки по Quarter Kelly.
    edge  = prob_model - prob_market
    odds  = 1 / prob_market (сколько получим за $1)
    kelly = edge / (odds - 1)
    """
    if prob_market <= 0 or prob_market >= 1:
        return 0.0
    odds = 1.0 / prob_market
    edge = prob_model - prob_market
    if edge <= 0:
        return 0.0
    kelly = edge / (odds - 1.0)
    bet = bank * kelly * MAX_KELLY_FRACTION
    return round(max(0.0, min(bet, MAX_BET_SIZE)), 2)


# ─── MARKET HELPERS ──────────────────────────────────────────────────────────

def is_cs2_event(event: dict) -> bool:
    text = (event.get("title", "") + " " + event.get("slug", "")).lower()
    return any(w in text for w in CS2_WORDS)

def is_valid_match(market: dict) -> bool:
    q = market.get("question", "").lower()
    if " vs " not in q and " vs. " not in q:
        return False
    if any(w in q for w in EXCLUDE_WORDS):
        return False
    if market.get("closed"):
        return False
    return True

def get_prices(market: dict) -> list[float]:
    try:
        raw = market.get("outcomePrices", "[]")
        prices = json.loads(raw) if isinstance(raw, str) else raw
        return [float(p) for p in prices[:2]]
    except Exception:
        return [0.5, 0.5]

def get_teams(market: dict) -> list[str]:
    try:
        raw = market.get("outcomes", "[]")
        outcomes = json.loads(raw) if isinstance(raw, str) else raw
        return [
            str(o).strip() for o in outcomes
            if str(o).strip().lower() not in
            ("yes", "no", "draw", "other", "neither", "over", "under")
        ][:2]
    except Exception:
        return []

def get_volume(market: dict) -> float:
    try:
        return float(market.get("volume", 0) or 0)
    except Exception:
        return 0.0

def market_url(market: dict) -> str:
    slug = market.get("slug") or market.get("id", "")
    return f"https://polymarket.com/event/{slug}"

def fmt_volume(v: float) -> str:
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"

def is_resolved(market: dict) -> tuple[bool, int | None]:
    """Возвращает (resolved, winner_idx)."""
    if not market.get("closed"):
        return False, None
    prices = get_prices(market)
    if len(prices) < 2:
        return False, None
    if prices[0] >= 0.95:
        return True, 0
    if prices[1] >= 0.95:
        return True, 1
    return False, None


# ─── EVALUATE MARKET (Elo model) ─────────────────────────────────────────────

def evaluate_market(market: dict) -> dict | None:
    """
    Оценивает рынок через Elo-модель.
    Возвращает dict с сигналом или None если нет edge.
    """
    teams  = get_teams(market)
    prices = get_prices(market)

    if len(teams) < 2 or len(prices) < 2:
        return None

    team_a, team_b = teams[0], teams[1]
    rank_a = get_rank(team_a)
    rank_b = get_rank(team_b)

    # Если хотя бы одна команда известна — работаем
    if rank_a is None and rank_b is None:
        return None

    # Если одна неизвестна — даём ей ранг 50 (аутсайдер)
    rank_a = rank_a or 50
    rank_b = rank_b or 50

    rank_diff = abs(rank_a - rank_b)

    # Слишком маленькая разница рангов — нет смысла
    if rank_diff < MIN_RANK_DIFF:
        return None

    elo_a   = rank_to_elo(rank_a)
    elo_b   = rank_to_elo(rank_b)
    model_a = elo_win_prob(elo_a, elo_b)  # вероятность победы team_a
    model_b = 1.0 - model_a

    market_a = prices[0]   # рыночная вероятность team_a
    market_b = prices[1]

    # Ищем сторону с позитивным edge
    edge_a = model_a - market_a
    edge_b = model_b - market_b

    if edge_a >= edge_b and edge_a >= MIN_EDGE:
        side, model_prob, market_prob, team = 0, model_a, market_a, team_a
        edge = edge_a
    elif edge_b > edge_a and edge_b >= MIN_EDGE:
        side, model_prob, market_prob, team = 1, model_b, market_b, team_b
        edge = edge_b
    else:
        return None

    return {
        "side":        side,
        "team":        team,
        "opponent":    teams[1 - side],
        "rank":        rank_a if side == 0 else rank_b,
        "opp_rank":    rank_b if side == 0 else rank_a,
        "model_prob":  round(model_prob, 4),
        "market_prob": round(market_prob, 4),
        "edge":        round(edge, 4),
        "rank_diff":   rank_diff,
    }


# ─── HLTV ─────────────────────────────────────────────────────────────────────

async def fetch_hltv(session: aiohttp.ClientSession) -> dict:
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    try:
        async with session.get(
            "https://www.hltv.org/ranking/teams",
            headers={"User-Agent": ua, "Accept-Language": "en-US,en;q=0.9"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                return FALLBACK_RANKING.copy()
            html = await r.text()
            ranking = {}
            blocks = re.findall(
                r'class="position">#(\d+).*?class="name">(.*?)<',
                html, re.DOTALL,
            )
            for rank_str, name_raw in blocks:
                name = re.sub(r"<[^>]+>", "", name_raw).strip().lower()
                try:
                    ranking[name] = int(rank_str)
                except ValueError:
                    pass
            return ranking if ranking else FALLBACK_RANKING.copy()
    except Exception:
        return FALLBACK_RANKING.copy()

async def refresh_hltv(session: aiohttp.ClientSession):
    global hltv_ranking, hltv_last_updated
    now = datetime.now(timezone.utc)
    if (
        not hltv_ranking or
        hltv_last_updated is None or
        (now - hltv_last_updated).total_seconds() > HLTV_INTERVAL
    ):
        hltv_ranking = await fetch_hltv(session)
        hltv_last_updated = now
        log.info("HLTV updated: %d teams", len(hltv_ranking))


# ─── GAMMA API ────────────────────────────────────────────────────────────────

async def fetch_cs2_markets(session: aiohttp.ClientSession) -> list:
    all_markets = []
    seen_events = set()

    for tag in CS2_TAGS:
        offset = 0
        while True:
            try:
                params = {
                    "tag_slug": tag, "closed": "false",
                    "limit": "100", "offset": str(offset),
                }
                async with session.get(
                    GAMMA_API + "/events", params=params,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as r:
                    if r.status != 200:
                        break
                    data = await r.json()
                    events = (
                        data if isinstance(data, list)
                        else data.get("events", data.get("data", []))
                    )
                    if not events:
                        break
                    for ev in events:
                        eid = ev.get("id")
                        if eid in seen_events:
                            continue
                        if tag == "esports" and not is_cs2_event(ev):
                            continue
                        seen_events.add(eid)
                        for m in ev.get("markets", []):
                            all_markets.append(m)
                    if len(events) < 100:
                        break
                    offset += 100
            except Exception as e:
                log.warning("fetch tag=%s: %s", tag, e)
                break

    seen_ids = set()
    result = []
    for m in all_markets:
        mid = m.get("id")
        if mid and mid not in seen_ids and is_valid_match(m):
            seen_ids.add(mid)
            result.append(m)

    log.info("CS2 markets: %d valid", len(result))
    return result

async def fetch_market(session: aiohttp.ClientSession, market_id: str) -> dict | None:
    try:
        async with session.get(
            GAMMA_API + "/markets/" + str(market_id),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.warning("fetch_market %s: %s", market_id, e)
    return None


# ─── KEYBOARDS ───────────────────────────────────────────────────────────────

def kb_market(url: str) -> InlineKeyboardMarkup:
    """Инлайн-кнопка под уведомлением об открытой ставке."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔗 Открыть рынок", url=url),
        InlineKeyboardButton(text="📂 Мои ставки",   callback_data="cb_open"),
    ]])

def kb_closed(url: str) -> InlineKeyboardMarkup:
    """Инлайн-кнопки под уведомлением о закрытой ставке."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔗 Рынок",    url=url),
            InlineKeyboardButton(text="📜 История",  callback_data="cb_history"),
        ],
        [
            InlineKeyboardButton(text="📊 Статус",   callback_data="cb_status"),
        ],
    ])

def kb_daily() -> InlineKeyboardMarkup:
    """Инлайн-кнопки под дейли-саммари."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📂 Ставки",   callback_data="cb_open"),
        InlineKeyboardButton(text="📜 История",  callback_data="cb_history"),
    ]])


# ─── NOTIFICATIONS ───────────────────────────────────────────────────────────

async def notify(text: str, kb=None):
    try:
        await bot.send_message(
            CHAT_ID, text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=kb,
        )
    except Exception as e:
        log.error("notify error: %s", e)

async def notify_bet_opened(market: dict, signal: dict, bet_size: float,
                            bank_after: float):
    q        = market.get("question", "")[:80]
    vol      = fmt_volume(get_volume(market))
    elo_str  = f"#{signal['rank']} vs #{signal['opp_rank']} (diff {signal['rank_diff']})"
    edge_pct = signal["edge"] * 100
    url      = market_url(market)

    await notify(
        f"🤖 <b>AUTO BET PLACED</b>  [PAPER]\n\n"
        f"🎯 <b>{signal['team']}</b> vs {signal['opponent']}\n"
        f"📋 {q}\n\n"
        f"📊 Elo:          {elo_str}\n"
        f"🧮 Модель:       {signal['model_prob']*100:.1f}%\n"
        f"📈 Рынок:        {signal['market_prob']*100:.1f}%\n"
        f"⚡ Edge:         +{edge_pct:.1f} цент\n\n"
        f"💵 Ставка:       <b>${bet_size:.2f}</b>\n"
        f"📦 Объём рынка:  {vol}\n"
        f"🏦 Банк после:   <b>${bank_after:.2f}</b>",
        kb=kb_market(url),
    )

async def notify_bet_closed(bet: dict, won: bool, bank: float, stats: dict):
    emoji   = "✅" if won else "❌"
    result  = "ВЫИГРАЛ" if won else "ПРОИГРАЛ"
    profit  = bet["profit"]
    p_str   = f"+${profit:.2f}" if profit >= 0 else f"−${abs(profit):.2f}"

    total    = stats["wins"] + stats["losses"]
    win_rate = stats["wins"] / total * 100 if total else 0
    pnl      = stats["profit"]
    pnl_str  = f"+${pnl:.2f}" if pnl >= 0 else f"−${abs(pnl):.2f}"

    await notify(
        f"{emoji} <b>BET CLOSED</b>  — {result}\n\n"
        f"🎯 {bet['team']} vs {bet['opponent']}\n"
        f"📋 {bet['question'][:70]}\n\n"
        f"💵 P&L: <b>{p_str}</b>\n\n"
        f"<b>📊 Статистика бота:</b>\n"
        f"├ Банк:     <b>${bank:.2f}</b>\n"
        f"├ Сделок:   {total}\n"
        f"├ Win rate: {win_rate:.0f}%\n"
        f"└ P&L всего: <b>{pnl_str}</b>",
        kb=kb_closed(bet["market_url"]),
    )

async def notify_daily(portfolio: dict):
    stats    = portfolio["stats"]
    total    = stats["wins"] + stats["losses"]
    win_rate = stats["wins"] / total * 100 if total else 0
    invested = sum(b["bet_size"] for b in portfolio["open"].values())
    pnl      = stats["profit"]
    delta    = portfolio["bank"] + invested - STARTING_BANK
    delta_s  = f"+${delta:.2f}" if delta >= 0 else f"−${abs(delta):.2f}"
    pnl_s    = f"+${pnl:.2f}" if pnl >= 0 else f"−${abs(pnl):.2f}"

    await notify(
        f"📊 <b>DAILY SUMMARY</b>  [AUTO BOT]\n\n"
        f"💰 Свободный банк:  <b>${portfolio['bank']:.2f}</b>\n"
        f"📦 В ставках:       ${invested:.2f}\n"
        f"📈 Всего капитал:   <b>${portfolio['bank']+invested:.2f}</b> "
        f"({delta_s} от старта)\n\n"
        f"🏁 Открытых: {len(portfolio['open'])}\n"
        f"✅ Побед:    {stats['wins']}"
        f"  |  ❌ Поражений: {stats['losses']}\n"
        f"🎯 Win rate: {win_rate:.0f}%\n"
        f"💵 P&L:      <b>{pnl_s}</b>",
        kb=kb_daily(),
    )


# ─── CORE LOOPS ──────────────────────────────────────────────────────────────

async def scan_loop():
    """Ищет новые матчи и открывает ставки."""
    log.info("Scan loop started")
    seen_markets: set = set()

    async with aiohttp.ClientSession() as session:
        # Заполняем seen на первом запуске — не ставим на старые
        await refresh_hltv(session)
        markets = await fetch_cs2_markets(session)
        for m in markets:
            seen_markets.add(str(m.get("id", "")))
        log.info("First run: %d known markets", len(seen_markets))

        while True:
            await asyncio.sleep(SCAN_INTERVAL)
            try:
                await refresh_hltv(session)
                markets = await fetch_cs2_markets(session)
                portfolio = load_portfolio()

                for market in markets:
                    mid = str(market.get("id", ""))
                    if not mid:
                        continue

                    # Уже видели этот рынок
                    if mid in seen_markets:
                        continue
                    seen_markets.add(mid)

                    # Уже есть открытая ставка
                    if mid in portfolio["open"]:
                        continue

                    # Объём слишком маленький
                    if get_volume(market) < MIN_VOLUME:
                        continue

                    # Лимит открытых позиций
                    if len(portfolio["open"]) >= MAX_OPEN_BETS:
                        log.info("Max open bets reached")
                        break

                    # Оцениваем через Elo-модель
                    signal = evaluate_market(market)
                    if not signal:
                        continue

                    # Quarter Kelly размер ставки
                    bet_size = quarter_kelly(
                        signal["model_prob"],
                        signal["market_prob"],
                        portfolio["bank"],
                    )
                    if bet_size < MIN_BET_SIZE:
                        log.info(
                            "Bet too small ($%.2f) for %s, skip",
                            bet_size, market.get("question", "")[:50],
                        )
                        continue

                    if portfolio["bank"] < bet_size:
                        log.info("Not enough bank ($%.2f)", portfolio["bank"])
                        break

                    # Открываем бумажную ставку
                    teams = get_teams(market)
                    prices = get_prices(market)

                    bet = {
                        "question":    market.get("question", ""),
                        "market_url":  market_url(market),
                        "team":        signal["team"],
                        "opponent":    signal["opponent"],
                        "side_idx":    signal["side"],
                        "rank":        signal["rank"],
                        "opp_rank":    signal["opp_rank"],
                        "rank_diff":   signal["rank_diff"],
                        "model_prob":  signal["model_prob"],
                        "market_prob": signal["market_prob"],
                        "edge":        signal["edge"],
                        "bet_size":    bet_size,
                        "potential_payout": round(
                            bet_size / signal["market_prob"], 2
                        ),
                        "opened_at":   datetime.now(timezone.utc).isoformat(),
                    }

                    portfolio["open"][mid] = bet
                    portfolio["bank"]     -= bet_size
                    portfolio["stats"]["bets"] += 1
                    save_portfolio(portfolio)

                    await notify_bet_opened(market, signal, bet_size, portfolio["bank"])
                    log.info(
                        "Bet opened: %s | edge %.2f | $%.2f",
                        signal["team"], signal["edge"], bet_size,
                    )
                    await asyncio.sleep(1)

            except Exception as e:
                log.error("scan_loop error: %s", e, exc_info=True)


async def monitor_loop():
    """Проверяет открытые ставки на предмет результата."""
    log.info("Monitor loop started")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                portfolio = load_portfolio()

                for mid, bet in list(portfolio["open"].items()):
                    market = await fetch_market(session, mid)
                    if not market:
                        continue

                    resolved, winner_idx = is_resolved(market)
                    if not resolved:
                        continue

                    won = (winner_idx == bet["side_idx"])

                    # Считаем P&L
                    if won:
                        payout = bet["potential_payout"]
                        profit = round(payout - bet["bet_size"], 4)
                        portfolio["stats"]["wins"] += 1
                        portfolio["bank"] += payout
                    else:
                        profit = -bet["bet_size"]
                        portfolio["stats"]["losses"] += 1
                        # банк уже уменьшен при открытии

                    portfolio["stats"]["profit"] = round(
                        portfolio["stats"]["profit"] + profit, 4
                    )

                    closed_bet = {
                        **bet,
                        "won":       won,
                        "profit":    profit,
                        "closed_at": datetime.now(timezone.utc).isoformat(),
                    }
                    portfolio["closed"].append(closed_bet)
                    del portfolio["open"][mid]
                    save_portfolio(portfolio)

                    await notify_bet_closed(
                        closed_bet, won,
                        portfolio["bank"],
                        portfolio["stats"],
                    )
                    log.info(
                        "Bet closed: %s | %s | $%+.2f",
                        bet["team"], "WIN" if won else "LOSS", profit,
                    )
                    await asyncio.sleep(1)

            except Exception as e:
                log.error("monitor_loop error: %s", e, exc_info=True)

            await asyncio.sleep(RESULT_INTERVAL)


async def daily_summary_loop():
    """Ежедневный отчёт в 09:00 UTC."""
    while True:
        now      = datetime.now(timezone.utc)
        next_9am = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= next_9am:
            next_9am += timedelta(days=1)
        await asyncio.sleep((next_9am - now).total_seconds())
        try:
            portfolio = load_portfolio()
            await notify_daily(portfolio)
        except Exception as e:
            log.error("daily summary error: %s", e)


# ─── TELEGRAM COMMANDS ───────────────────────────────────────────────────────

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статус"),    KeyboardButton(text="📂 Ставки")],
        [KeyboardButton(text="📜 История"),   KeyboardButton(text="📈 HLTV")],
    ],
    resize_keyboard=True,
    persistent=True,
)

@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await msg.answer(
        "🤖 <b>CS2 Auto Prediction Bot</b>\n\n"
        "Автоматически анализирует CS2 матчи через Elo-модель "
        "и делает бумажные ставки по Quarter Kelly.\n\n"
        "📊 Статус   — текущий банк и P&L\n"
        "📂 Ставки   — открытые позиции\n"
        "📜 История  — последние 10 закрытых\n"
        "📈 HLTV     — топ-20 с Elo рейтингом",
        parse_mode="HTML",
        reply_markup=MAIN_KB,
    )

@dp.message(lambda m: m.text == "📊 Статус")
async def btn_status(msg: types.Message):
    await cmd_status(msg)

@dp.message(lambda m: m.text == "📂 Ставки")
async def btn_open(msg: types.Message):
    await cmd_open(msg)

@dp.message(lambda m: m.text == "📜 История")
async def btn_history(msg: types.Message):
    await cmd_history(msg)

@dp.message(lambda m: m.text == "📈 HLTV")
async def btn_ranking(msg: types.Message):
    await cmd_ranking(msg)

@dp.message(Command("status"))
async def cmd_status(msg: types.Message):
    portfolio = load_portfolio()
    stats     = portfolio["stats"]
    total     = stats["wins"] + stats["losses"]
    win_rate  = stats["wins"] / total * 100 if total else 0
    invested  = sum(b["bet_size"] for b in portfolio["open"].values())
    pnl       = stats["profit"]
    pnl_s     = f"+${pnl:.2f}" if pnl >= 0 else f"−${abs(pnl):.2f}"
    delta     = portfolio["bank"] + invested - STARTING_BANK
    delta_s   = f"+${delta:.2f}" if delta >= 0 else f"−${abs(delta):.2f}"

    hltv_src = "live" if hltv_ranking else "fallback"

    await msg.answer(
        f"📊 <b>Bot Status</b>\n\n"
        f"💰 Банк (свободный):  <b>${portfolio['bank']:.2f}</b>\n"
        f"📦 В ставках:          ${invested:.2f}\n"
        f"📈 Итого капитал:      <b>${portfolio['bank']+invested:.2f}</b> "
        f"({delta_s})\n\n"
        f"🏁 Всего ставок:   {stats['bets']}\n"
        f"✅ Побед:          {stats['wins']}\n"
        f"❌ Поражений:      {stats['losses']}\n"
        f"🎯 Win rate:       {win_rate:.0f}%\n"
        f"💵 P&L:            <b>{pnl_s}</b>\n\n"
        f"📡 HLTV данные:    {hltv_src} ({len(hltv_ranking)} команд)\n"
        f"⚙️ Min edge:       {MIN_EDGE*100:.0f}%\n"
        f"⚙️ Quarter Kelly:  {MAX_KELLY_FRACTION*100:.0f}%",
        parse_mode="HTML",
    )

@dp.message(Command("open"))
async def cmd_open(msg: types.Message):
    portfolio = load_portfolio()
    open_bets = portfolio["open"]

    if not open_bets:
        await msg.answer("📭 Нет открытых ставок.")
        return

    lines = [f"📂 <b>Открытые ставки ({len(open_bets)}):</b>\n"]
    for mid, bet in open_bets.items():
        ts = bet["opened_at"][:10]
        lines.append(
            f"🎯 <b>{bet['team']}</b> vs {bet['opponent']}\n"
            f"   #{bet['rank']} vs #{bet['opp_rank']} "
            f"(diff {bet['rank_diff']})\n"
            f"   Модель: {bet['model_prob']*100:.1f}% | "
            f"Рынок: {bet['market_prob']*100:.1f}% | "
            f"Edge: +{bet['edge']*100:.1f}%\n"
            f"   Ставка: <b>${bet['bet_size']:.2f}</b> | "
            f"Потенциал: ${bet['potential_payout']:.2f}\n"
            f"   <a href=\"{bet['market_url']}\">Открыть</a> | {ts}"
        )

    await msg.answer("\n\n".join(lines), parse_mode="HTML",
                     disable_web_page_preview=True)

@dp.message(Command("history"))
async def cmd_history(msg: types.Message):
    portfolio = load_portfolio()
    closed    = portfolio["closed"][-10:]

    if not closed:
        await msg.answer("📭 Нет закрытых ставок.")
        return

    lines = [f"📜 <b>Последние {len(closed)} ставок:</b>\n"]
    for bet in reversed(closed):
        emoji  = "✅" if bet["won"] else "❌"
        profit = bet["profit"]
        p_str  = f"+${profit:.2f}" if profit >= 0 else f"−${abs(profit):.2f}"
        lines.append(
            f"{emoji} <b>{bet['team']}</b> vs {bet['opponent']}\n"
            f"   Ставка ${bet['bet_size']:.2f} | P&L <b>{p_str}</b>\n"
            f"   Edge был: +{bet['edge']*100:.1f}%\n"
            f"   <a href=\"{bet['market_url']}\">Рынок</a>"
        )

    await msg.answer("\n\n".join(lines), parse_mode="HTML",
                     disable_web_page_preview=True)

@dp.message(Command("ranking"))
async def cmd_ranking(msg: types.Message):
    ranking = hltv_ranking if hltv_ranking else FALLBACK_RANKING
    top20   = sorted(ranking.items(), key=lambda x: x[1])[:20]

    seen, unique = set(), []
    for name, rank in top20:
        if rank not in seen:
            seen.add(rank)
            unique.append((rank, name.title()))

    src = "live HLTV" if hltv_ranking else "fallback"
    lines = [f"📊 <b>HLTV Top 20</b> ({src}):\n"]
    for rank, name in unique:
        elo = rank_to_elo(rank)
        lines.append(f"#{rank}  {name}  <i>(Elo {elo:.0f})</i>")

    await msg.answer("\n".join(lines), parse_mode="HTML")


# ─── INLINE CALLBACKS ────────────────────────────────────────────────────────

@dp.callback_query(lambda c: c.data == "cb_status")
async def cb_status(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_status(callback.message)

@dp.callback_query(lambda c: c.data == "cb_open")
async def cb_open(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_open(callback.message)

@dp.callback_query(lambda c: c.data == "cb_history")
async def cb_history(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_history(callback.message)

@dp.callback_query(lambda c: c.data == "cb_ranking")
async def cb_ranking(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_ranking(callback.message)


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

async def main():
    await notify(
        f"🤖 <b>CS2 Auto Bot запущен</b>  [PAPER]\n\n"
        f"⚙️ Параметры модели:\n"
        f"├ Min edge:       {MIN_EDGE*100:.0f} центов\n"
        f"├ Quarter Kelly:  {MAX_KELLY_FRACTION*100:.0f}%\n"
        f"├ Max ставка:     ${MAX_BET_SIZE}\n"
        f"├ Min объём:      ${MIN_VOLUME:,}\n"
        f"└ Min rank diff:  {MIN_RANK_DIFF}\n\n"
        f"💰 Стартовый банк: ${STARTING_BANK}"
    )

    await asyncio.gather(
        dp.start_polling(bot),
        scan_loop(),
        monitor_loop(),
        daily_summary_loop(),
    )

if __name__ == "__main__":
    asyncio.run(main())