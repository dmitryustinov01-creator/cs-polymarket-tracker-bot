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

# Используем PostgreSQL если DATABASE_URL задан, иначе JSON файлы
USE_DB = bool(os.environ.get("DATABASE_URL"))
if USE_DB:
    try:
        import db
    except ImportError:
        USE_DB = False

DATA_FILE = "auto_portfolio.json"

# ─── CONFIG ──────────────────────────────────────────────────────────────────

BOT_TOKEN  = os.environ["BOT_TOKEN"]
CHAT_ID    = int(os.environ["CHAT_ID"])

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
    # ── Топ 10 ────────────────────────────────────────────────────────────
    "vitality": 1, "team vitality": 1,
    "natus vincere": 2, "navi": 2,
    "faze": 3, "faze clan": 3,
    "g2": 4, "g2 esports": 4,
    "spirit": 5, "team spirit": 5,
    "liquid": 6, "team liquid": 6,
    "mouz": 7, "mousesports": 7,
    "heroic": 8,
    "astralis": 9,
    "nip": 10, "ninjas in pyjamas": 10,

    # ── Топ 11–30 ─────────────────────────────────────────────────────────
    "complexity": 11, "col": 11,
    "ence": 12,
    "cloud9": 13, "c9": 13,
    "big": 14,
    "eternal fire": 15,
    "fnatic": 16,
    "pain": 17, "pain gaming": 17,
    "3dmax": 18,
    "mibr": 19,
    "paiN": 20,
    "virtus.pro": 21, "vp": 21,
    "flyquest": 22,
    "monte": 23,
    "saw": 24,
    "apeks": 25,
    "b8": 26,
    "betboom": 27, "betboom team": 27,
    "parivision": 28,
    "aurora": 29,
    "100 thieves": 30, "100t": 30,

    # ── Топ 31–60 ─────────────────────────────────────────────────────────
    "falcons": 31, "team falcons": 31,
    "furia": 32,
    "imperial": 33, "imperial esports": 33,
    "9z": 34, "9z team": 34,
    "og": 35,
    "forze": 36,
    "fluxo": 37,
    "entropiq": 38,
    "sprout": 39,
    "passion ua": 40,
    "ex-natus vincere": 41,
    "wildcard": 42, "wildcard gaming": 42,
    "gamerlegion": 43, "gl": 43,
    "amkal": 44,
    "pera": 45, "pera esports": 45,
    "lynn vision": 46,
    "tyloo": 47,
    "rare atom": 48,
    "the mongolz": 49, "mongolz": 49,
    "red canids": 50,
    "nouns": 51, "nouns esports": 51,
    "sinners": 52,
    "permitta": 53,
    "illuminar": 54,
    "nemiga": 55,
    "havu": 56,
    "sashi": 57,
    "rooster": 58,
    "nexus": 59,
    "sagrado": 60,

    # ── Топ 61–100 ────────────────────────────────────────────────────────
    "bestia": 61,
    "sharks": 62, "sharks esports": 62,
    "anonymo": 63,
    "endpoint": 64,
    "intz": 65,
    "vikings": 66,
    "atk": 67,
    "alliance": 68,
    "copenhagen flames": 69, "copenhagen": 69,
    "skade": 70,
    "eclot": 71,
    "navi junior": 72,
    "young ninjas": 73,
    "masonic": 74,
    "benched heroes": 75,
    "koi": 76,
    "sangal": 77,
    "iбerik": 78,
    "metizport": 79,
    "into the breach": 80, "itb": 80,
    "lyngby vikings": 81,
    "wopa": 82,
    "zero tenacity": 83,
    "rebels": 84, "rebels gaming": 84,
    "dynamo eclot": 85,
    "bald": 86,
    "chosen5": 87,
    "gmb": 88, "gmb esports": 88,
    "steel helmet": 89,
    "illuminati": 90,
    "fragmatic": 91,
    "verdant": 92,
    "housebets": 93,
    "timbermen": 94,
    "mythic": 95,
    "triumph": 96,
    "bad news bears": 97,
    "oxygen": 98,
    "esic": 99,
    "limitless": 100,

    # ── Топ 101–150 ───────────────────────────────────────────────────────
    "winstrike": 101,
    "bravado": 102,
    "white wolves": 103,
    "tricked": 104,
    "x-kom": 105,
    "pwr": 106,
    "orgless": 107,
    "nizhny novgorod": 108,
    "leks": 109,
    "quazar": 110,
    "sector one": 111,
    "pompa": 112,
    "afterlife": 113,
    "antwerp giants": 114,
    "e-sharks": 115,
    "griffins": 116,
    "ldlc": 117,
    "izt": 118,
    "karma": 119,
    "snogard dragons": 120,
    "project x": 121,
    "bisons": 122,
    "windigo": 123,
    "spectral": 124,
    "finest": 125,
    "warthox": 126,
    "raptors": 127,
    "honvéd": 128,
    "heimo": 129,
    "live to win": 130,
    "nolpenki": 131,
    "arctic": 132,
    "solid": 133,
    "teamone": 134,
    "eagles": 135,
    "nitro.vl": 136,
    "nexus gaming": 137,
    "black dragons": 138,
    "ex-imperial": 139,
    "hardfeelings": 140,
    "win a meal": 141,
    "sabre": 142,
    "aravt": 143,
    "lv": 144,
    "viperio": 145,
    "rway": 146,
    "amkal esports": 147,
    "skyfire": 148,
    "furia academy": 149,
    "forest": 150,
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


# ─── PORTFOLIO (JSON fallback / PostgreSQL) ───────────────────────────────────

def _empty_portfolio() -> dict:
    return {
        "bank":  STARTING_BANK,
        "open":  {},
        "stats": {"bets": 0, "wins": 0, "losses": 0, "profit": 0.0},
    }

def _load_json() -> dict:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, encoding="utf-8") as f:
            return json.load(f)
    return _empty_portfolio()

def _save_json(p: dict):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(p, f, indent=2, ensure_ascii=False)

async def portfolio_get() -> dict:
    if USE_DB:
        return await db.get_portfolio()
    return _load_json()

async def portfolio_open_bet(mid: str, bet: dict, bank_after: float):
    if USE_DB:
        await db.open_bet(mid, bet, bank_after)
    else:
        p = _load_json()
        p["open"][mid] = {**bet, "opened_at": datetime.now(timezone.utc).isoformat()}
        p["bank"] = bank_after
        p["stats"]["bets"] += 1
        _save_json(p)

async def portfolio_close_bet(mid: str, won: bool) -> dict | None:
    if USE_DB:
        return await db.close_bet(mid, won)
    p = _load_json()
    bet = p["open"].pop(mid, None)
    if not bet:
        return None
    profit = round(bet["potential_payout"] - bet["bet_size"], 4) if won else -bet["bet_size"]
    payout = bet["potential_payout"] if won else 0.0
    p["bank"] += payout
    if won:
        p["stats"]["wins"] += 1
    else:
        p["stats"]["losses"] += 1
    p["stats"]["profit"] = round(p["stats"]["profit"] + profit, 4)
    closed = {**bet, "won": won, "profit": profit,
              "closed_at": datetime.now(timezone.utc).isoformat()}
    p.setdefault("closed", []).append(closed)
    _save_json(p)
    return closed

async def portfolio_get_closed(limit: int = 10) -> list:
    if USE_DB:
        return await db.get_closed_bets(limit)
    p = _load_json()
    return list(reversed(p.get("closed", [])[-limit:]))


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
    """
    Ищет ранг команды. Порядок поиска:
    1. Точное совпадение
    2. Один содержит другой (с допуском по длине)
    3. Поиск по токенам (команда из 2+ слов)
    """
    if not name:
        return None

    ranking = hltv_ranking if hltv_ranking else FALLBACK_RANKING
    n = name.strip().lower()

    # 1. Точное совпадение
    if n in ranking:
        return ranking[n]

    # 2. Один содержит другой
    for key, rank in ranking.items():
        if len(key) < 3:
            continue
        if key in n and len(n) - len(key) <= 5:
            return rank
        if n in key and len(key) - len(n) <= 5:
            return rank

    # 3. Поиск по первому значимому слову (длиннее 3 символов)
    tokens = [t for t in n.split() if len(t) > 3]
    for token in tokens:
        for key, rank in ranking.items():
            if token in key or key.startswith(token):
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
    """
    Скрапит HLTV топ-30. Пробует несколько regex-паттернов
    на случай изменения HTML структуры.
    Итоговый рейтинг = merge(live данные, fallback) — чтобы
    команды 31-150 всегда были доступны через fallback.
    """
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    live: dict = {}

    try:
        async with session.get(
            "https://www.hltv.org/ranking/teams",
            headers={
                "User-Agent":      ua,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept":          "text/html,application/xhtml+xml",
            },
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status == 200:
                html = await r.text()

                # Паттерн 1 — старый формат
                for rank_str, name_raw in re.findall(
                    r'class="position">#(\d+).*?class="name">(.*?)<',
                    html, re.DOTALL,
                ):
                    name = re.sub(r"<[^>]+>", "", name_raw).strip().lower()
                    try:
                        live[name] = int(rank_str)
                    except ValueError:
                        pass

                # Паттерн 2 — новый формат (ranked-team блоки)
                if len(live) < 10:
                    for rank_str, name_raw in re.findall(
                        r'ranked-team.*?ranking-header.*?#(\d+).*?'
                        r'team-name["\s]+>([^<]+)<',
                        html, re.DOTALL,
                    ):
                        name = name_raw.strip().lower()
                        try:
                            live[name] = int(rank_str)
                        except ValueError:
                            pass

                # Паттерн 3 — data-атрибуты
                if len(live) < 10:
                    for rank_str, name_raw in re.findall(
                        r'data-teamid[^>]*>.*?#(\d+).*?<span[^>]*>([^<]{2,40})</span>',
                        html, re.DOTALL,
                    ):
                        name = name_raw.strip().lower()
                        try:
                            live[name] = int(rank_str)
                        except ValueError:
                            pass

                log.info("HLTV live: %d teams parsed", len(live))

    except Exception as e:
        log.warning("HLTV fetch error: %s", e)

    # Мержим: live перекрывает fallback для топ-30,
    # fallback добавляет команды 31-150 которых нет в live
    merged = dict(FALLBACK_RANKING)   # начинаем с полного fallback
    merged.update(live)               # live данные побеждают где пересекаются
    return merged

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
        log.info("HLTV updated: %d teams total", len(hltv_ranking))


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
        await refresh_hltv(session)
        markets = await fetch_cs2_markets(session)
        for m in markets:
            seen_markets.add(str(m.get("id", "")))
        log.info("First run: %d known markets", len(seen_markets))

        while True:
            await asyncio.sleep(SCAN_INTERVAL)
            await refresh_hltv(session)
            markets = await fetch_cs2_markets(session)
            portfolio = await portfolio_get()

            for market in markets:
                mid = str(market.get("id", ""))
                if not mid or mid in seen_markets:
                    continue
                seen_markets.add(mid)

                if mid in portfolio["open"]:
                    continue
                if get_volume(market) < MIN_VOLUME:
                    continue
                if len(portfolio["open"]) >= MAX_OPEN_BETS:
                    log.info("Max open bets reached")
                    break

                signal = evaluate_market(market)
                if not signal:
                    continue

                bet_size = quarter_kelly(
                    signal["model_prob"],
                    signal["market_prob"],
                    portfolio["bank"],
                )
                if bet_size < MIN_BET_SIZE:
                    continue
                if portfolio["bank"] < bet_size:
                    log.info("Not enough bank ($%.2f)", portfolio["bank"])
                    break

                bet = {
                    "question":       market.get("question", ""),
                    "market_url":     market_url(market),
                    "team":           signal["team"],
                    "opponent":       signal["opponent"],
                    "side_idx":       signal["side"],
                    "rank":           signal["rank"],
                    "opp_rank":       signal["opp_rank"],
                    "rank_diff":      signal["rank_diff"],
                    "model_prob":     signal["model_prob"],
                    "market_prob":    signal["market_prob"],
                    "edge":           signal["edge"],
                    "bet_size":       bet_size,
                    "potential_payout": round(bet_size / signal["market_prob"], 2),
                }

                bank_after = portfolio["bank"] - bet_size
                await portfolio_open_bet(mid, bet, bank_after)
                await notify_bet_opened(market, signal, bet_size, bank_after)
                log.info("Bet opened: %s | edge %.2f | $%.2f",
                         signal["team"], signal["edge"], bet_size)

                portfolio["open"][mid] = bet
                portfolio["bank"] = bank_after
                await asyncio.sleep(1)


async def monitor_loop():
    """Проверяет открытые ставки на предмет результата."""
    log.info("Monitor loop started")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                portfolio = await portfolio_get()

                for mid, bet in list(portfolio["open"].items()):
                    market = await fetch_market(session, mid)
                    if not market:
                        continue

                    resolved, winner_idx = is_resolved(market)
                    if not resolved:
                        continue

                    won = (winner_idx == bet["side_idx"])
                    closed_bet = await portfolio_close_bet(mid, won)
                    if not closed_bet:
                        continue

                    fresh = await portfolio_get()
                    await notify_bet_closed(
                        closed_bet, won,
                        fresh["bank"],
                        fresh["stats"],
                    )
                    log.info("Bet closed: %s | %s | $%+.2f",
                             bet["team"], "WIN" if won else "LOSS",
                             closed_bet["profit"])
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
            portfolio = await portfolio_get()
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
    portfolio = await portfolio_get()
    stats     = portfolio["stats"]
    total     = stats["wins"] + stats["losses"]
    win_rate  = stats["wins"] / total * 100 if total else 0
    invested  = sum(b["bet_size"] for b in portfolio["open"].values())
    pnl       = stats["profit"]
    pnl_s     = f"+${pnl:.2f}" if pnl >= 0 else f"−${abs(pnl):.2f}"
    delta     = portfolio["bank"] + invested - STARTING_BANK
    delta_s   = f"+${delta:.2f}" if delta >= 0 else f"−${abs(delta):.2f}"
    hltv_src  = "live" if hltv_ranking else "fallback"

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
    portfolio = await portfolio_get()
    open_bets = portfolio["open"]

    if not open_bets:
        await msg.answer("📭 Нет открытых ставок.")
        return

    lines = [f"📂 <b>Открытые ставки ({len(open_bets)}):</b>\n"]
    for mid, bet in open_bets.items():
        ts = str(bet.get("opened_at", ""))[:10]
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
    closed = await portfolio_get_closed(10)

    if not closed:
        await msg.answer("📭 Нет закрытых ставок.")
        return

    lines = [f"📜 <b>Последние {len(closed)} ставок:</b>\n"]
    for bet in closed:
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

async def safe_loop(name: str, coro_fn, restart_delay: int = 60):
    """
    Обёртка для любого loop — при падении логирует ошибку,
    уведомляет в Telegram и перезапускает через restart_delay секунд.
    Polling не перезапускаем — он управляется aiogram сам.
    """
    while True:
        try:
            log.info("Loop '%s' starting", name)
            await coro_fn()
        except asyncio.CancelledError:
            log.info("Loop '%s' cancelled", name)
            return
        except Exception as e:
            log.error("Loop '%s' crashed: %s", name, e, exc_info=True)
            try:
                await bot.send_message(
                    CHAT_ID,
                    f"⚠️ <b>Loop '{name}' упал</b>\n"
                    f"Ошибка: <code>{str(e)[:200]}</code>\n"
                    f"Перезапуск через {restart_delay}с...",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            await asyncio.sleep(restart_delay)

async def main():
    if USE_DB:
        await db.init_db()
        log.info("PostgreSQL initialized")
    else:
        log.info("Running in JSON mode (no DATABASE_URL)")

    await notify(
        f"🤖 <b>CS2 Auto Bot запущен</b>  [PAPER]\n"
        f"💾 Хранение: {'PostgreSQL' if USE_DB else 'JSON (локально)'}\n\n"
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
        safe_loop("scan",    scan_loop,    restart_delay=60),
        safe_loop("monitor", monitor_loop, restart_delay=30),
        safe_loop("daily",   daily_summary_loop, restart_delay=60),
    )

if __name__ == "__main__":
    asyncio.run(main())