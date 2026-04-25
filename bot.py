import asyncio
import logging
import os
import json
import re
from datetime import datetime, timezone

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "120"))
RESULT_CHECK_INTERVAL = 600
PRICE_CHECK_INTERVAL = 300
PRICE_ALERT_THRESHOLD = 0.07
HLTV_UPDATE_INTERVAL = 3600
PAPER_BET_SIZE = 10.0

# Файлы для хранения данных между рестартами
DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
PREDICTIONS_FILE = os.path.join(DATA_DIR, "predictions.json")
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")
KNOWN_MARKETS_FILE = os.path.join(DATA_DIR, "known_markets.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

subscribers = set()
known_market_ids = set()
is_first_run = True
hltv_ranking = {}
hltv_last_updated = None
predictions = {}

GAMMA_API = "https://gamma-api.polymarket.com"

CS2_TAG_SLUGS = ["counter-strike", "cs2", "esports"]
CS2_IDENTIFIERS = ["counter-strike", "cs2", "csgo", "cs:go"]

EXCLUDE_WORDS = [
    "map 1", "map 2", "map 3", "map 4", "map 5",
    "first map", "pistol", "knife",
    "games total", "o/u", "over/under",
    "first blood", "first kill", "ace", "bomb",
    "most kills", "handicap",
    "signs for", "signs with",
    "will valve", "map pool", "which maps",
    "what will", "how many",
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


# ─── Persistence ──────────────────────────────────────────────────────────────

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def save_predictions():
    try:
        ensure_data_dir()
        # Конвертируем int ключи в строки для JSON
        data = {str(k): v for k, v in predictions.items()}
        with open(PREDICTIONS_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.warning("save_predictions failed: %s", e)


def load_predictions():
    global predictions
    try:
        if os.path.exists(PREDICTIONS_FILE):
            with open(PREDICTIONS_FILE) as f:
                data = json.load(f)
            predictions = {int(k): v for k, v in data.items()}
            total = sum(len(v) for v in predictions.values())
            log.info("Loaded predictions: %d users, %d total", len(predictions), total)
    except Exception as e:
        log.warning("load_predictions failed: %s", e)
        predictions = {}


def save_subscribers():
    try:
        ensure_data_dir()
        with open(SUBSCRIBERS_FILE, "w") as f:
            json.dump(list(subscribers), f)
    except Exception as e:
        log.warning("save_subscribers failed: %s", e)


def load_subscribers():
    global subscribers
    try:
        if os.path.exists(SUBSCRIBERS_FILE):
            with open(SUBSCRIBERS_FILE) as f:
                subscribers = set(json.load(f))
            log.info("Loaded subscribers: %d", len(subscribers))
    except Exception as e:
        log.warning("load_subscribers failed: %s", e)
        subscribers = set()


def save_known_markets():
    try:
        ensure_data_dir()
        with open(KNOWN_MARKETS_FILE, "w") as f:
            json.dump(list(known_market_ids), f)
    except Exception as e:
        log.warning("save_known_markets failed: %s", e)


def load_known_markets():
    global known_market_ids
    try:
        if os.path.exists(KNOWN_MARKETS_FILE):
            with open(KNOWN_MARKETS_FILE) as f:
                known_market_ids = set(json.load(f))
            log.info("Loaded known markets: %d", len(known_market_ids))
    except Exception as e:
        log.warning("load_known_markets failed: %s", e)
        known_market_ids = set()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_cs2_event(event):
    text = (event.get("title", "") + " " + event.get("slug", "")).lower()
    return any(w in text for w in CS2_IDENTIFIERS)


def is_resolved(market):
    try:
        prices = market.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        if prices and len(prices) >= 2:
            p0 = round(float(prices[0]), 4)
            p1 = round(float(prices[1]), 4)
            if (p0 == 0.0 and p1 == 1.0) or (p0 == 1.0 and p1 == 0.0):
                return True
    except Exception:
        pass
    return False


def is_match_market(market):
    question = market.get("question", "").lower()
    if " vs " not in question and " vs. " not in question:
        return False
    if any(w in question for w in EXCLUDE_WORDS):
        return False
    if market.get("closed"):
        return False
    if is_resolved(market):
        return False
    return True


def get_prices(market):
    try:
        prices = market.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        return [float(p) for p in prices[:2]]
    except Exception:
        return [0.5, 0.5]


def get_price_str(market, idx):
    try:
        return str(round(get_prices(market)[idx] * 100)) + "c"
    except Exception:
        return "?"


def format_volume(market):
    try:
        v = float(market.get("volume", 0) or 0)
        if v >= 1000000:
            return "$" + str(round(v / 1000000, 1)) + "M"
        if v >= 1000:
            return "$" + str(round(v / 1000, 1)) + "K"
        return "$" + str(round(v))
    except Exception:
        return "$?"


def market_url(market):
    slug = market.get("slug") or market.get("id", "")
    return "https://polymarket.com/event/" + str(slug)


def get_team_rank(name):
    if not name:
        return ""
    ranking = hltv_ranking if hltv_ranking else FALLBACK_RANKING
    n = name.strip().lower()
    if n in ranking:
        return "#" + str(ranking[n])
    for key, rank in ranking.items():
        if key in n or n in key:
            return "#" + str(rank)
    return ""


def extract_teams(market):
    try:
        raw = market.get("outcomes", "[]")
        outcomes = json.loads(raw) if isinstance(raw, str) else raw
        teams = [str(o).strip() for o in outcomes
                 if str(o).strip().lower() not in (
                     "yes", "no", "draw", "other", "neither", "over", "under")]
        if teams:
            return teams[:2]
    except Exception:
        pass
    return []


def matchup_line(market):
    teams = extract_teams(market)
    p0 = get_price_str(market, 0)
    p1 = get_price_str(market, 1)
    if len(teams) >= 2:
        r0 = get_team_rank(teams[0])
        r1 = get_team_rank(teams[1])
        t0 = teams[0] + (" " + r0 if r0 else "") + " " + p0
        t1 = teams[1] + (" " + r1 if r1 else "") + " " + p1
        return t0 + " vs " + t1
    return "YES " + p0 + " / NO " + p1


def prediction_keyboard(market):
    mid = str(market.get("id", ""))
    teams = extract_teams(market)
    if len(teams) >= 2:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🟢 " + teams[0][:22],
                    callback_data="pick:" + mid + ":0"
                ),
                InlineKeyboardButton(
                    text="🟢 " + teams[1][:22],
                    callback_data="pick:" + mid + ":1"
                ),
            ],
            [InlineKeyboardButton(text="Open on Polymarket", url=market_url(market))]
        ])
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Open on Polymarket", url=market_url(market))
    ]])


def format_match_time(market):
    """Время матча из startDate или endDate."""
    for field in ("startDate", "startDateIso", "endDate"):
        raw = market.get(field, "")
        if raw:
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return dt.strftime("%d.%m.%Y %H:%M UTC")
            except Exception:
                pass
    return "?"


def new_market_text(market):
    question = market.get("question", "?")
    volume = format_volume(market)
    match_time = format_match_time(market)
    line = matchup_line(market)
    return (
        "<b>🎮 New CS2 match on Polymarket!</b>\n"
        + "🕐 " + match_time + "\n\n"
        + question + "\n\n"
        + line + "\n\n"
        + "Volume: " + volume + "\n\n"
        + "📊 Make your prediction:"
    )


PAGE_SIZE = 15

# Кэш списка матчей для пагинации (chat_id -> list of markets)
list_cache: dict = {}


def list_page_text(markets, offset=0):
    chunk = markets[offset:offset + PAGE_SIZE]
    lines = ["<b>🎮 CS2 matches on Polymarket:</b>\n"]
    for m in chunk:
        q = m.get("question", "?")[:60]
        url = market_url(m)
        match_time = format_match_time(m)
        line = matchup_line(m)
        lines.append(
            "- <a href=\"" + url + "\">" + q + "</a>\n"
            "  🕐 " + match_time + "\n"
            "  " + line
        )
    return "\n\n".join(lines)


def list_page_keyboard(total, offset=0):
    """Кнопка 'Show more' если есть ещё матчи."""
    shown = min(offset + PAGE_SIZE, total)
    remaining = total - shown
    if remaining <= 0:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📋 Show " + str(remaining) + " more",
            callback_data="list_more:" + str(shown)
        )
    ]])


# ─── HLTV ─────────────────────────────────────────────────────────────────────

async def fetch_hltv(session):
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
            blocks = re.findall(r'class="position">#(\d+).*?class="name">(.*?)<', html, re.DOTALL)
            for rank_str, name_raw in blocks:
                name = re.sub(r"<[^>]+>", "", name_raw).strip().lower()
                try:
                    ranking[name] = int(rank_str)
                except ValueError:
                    pass
            return ranking if ranking else FALLBACK_RANKING.copy()
    except Exception:
        return FALLBACK_RANKING.copy()


async def refresh_hltv(session):
    global hltv_ranking, hltv_last_updated
    now = datetime.now(timezone.utc)
    if (not hltv_ranking or hltv_last_updated is None or
            (now - hltv_last_updated).total_seconds() > HLTV_UPDATE_INTERVAL):
        hltv_ranking = await fetch_hltv(session)
        hltv_last_updated = now
        log.info("HLTV updated: %d teams", len(hltv_ranking))


# ─── Markets ──────────────────────────────────────────────────────────────────

async def fetch_markets(session):
    all_markets = []
    seen_event_ids = set()

    for tag_slug in CS2_TAG_SLUGS:
        offset = 0
        limit = 100
        while True:
            try:
                params = {
                    "tag_slug": tag_slug,
                    "closed": "false",
                    "limit": str(limit),
                    "offset": str(offset),
                    "order": "startDate",
                    "ascending": "false"
                }
                async with session.get(
                    GAMMA_API + "/events", params=params,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as r:
                    if r.status != 200:
                        break
                    data = await r.json()
                    events = data if isinstance(data, list) else data.get("events", data.get("data", []))
                    if not events:
                        break
                    for event in events:
                        eid = event.get("id")
                        if eid in seen_event_ids:
                            continue
                        if tag_slug == "esports" and not is_cs2_event(event):
                            continue
                        seen_event_ids.add(eid)
                        for m in event.get("markets", []):
                            all_markets.append(m)
                    if len(events) < limit:
                        break
                    offset += limit
            except Exception as e:
                log.warning("fetch tag=%s failed: %s", tag_slug, e)
                break

    seen = set()
    unique = []
    for m in all_markets:
        mid = m.get("id")
        if mid and mid not in seen:
            seen.add(mid)
            if is_match_market(m):
                unique.append(m)

    log.info("Markets: %d match markets", len(unique))
    return unique


async def fetch_market_by_id(session, market_id):
    try:
        async with session.get(
            GAMMA_API + "/markets/" + str(market_id),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.warning("fetch_market_by_id %s: %s", market_id, e)
    return None


# ─── Price tracking ───────────────────────────────────────────────────────────

async def check_price_changes(session):
    for chat_id, user_preds in list(predictions.items()):
        for market_id, pred in list(user_preds.items()):
            if pred.get("outcome"):
                continue

            market = await fetch_market_by_id(session, market_id)
            if not market:
                continue

            prices = get_prices(market)
            chosen_idx = pred["chosen_idx"]
            if chosen_idx >= len(prices):
                continue

            current_price = prices[chosen_idx]
            last_price = pred.get("last_price", pred["entry_price"])
            entry_price = pred["entry_price"]
            change = current_price - last_price
            change_from_entry = current_price - entry_price

            if abs(change) >= PRICE_ALERT_THRESHOLD:
                direction = "📈" if change > 0 else "📉"
                direction_word = "UP" if change > 0 else "DOWN"
                text = (
                    direction + " <b>Price moved " + direction_word + "!</b>\n\n"
                    + pred["question"] + "\n\n"
                    + "Your pick: <b>" + pred["chosen_team"] + "</b>\n"
                    + "Entry: " + str(round(entry_price * 100)) + "c\n"
                    + "Previous: " + str(round(last_price * 100)) + "c\n"
                    + "Now: <b>" + str(round(current_price * 100)) + "c</b>\n"
                    + "Change: " + ("+" if change_from_entry >= 0 else "") + str(round(change_from_entry * 100)) + "c from entry\n\n"
                    + "<a href=\"" + pred["market_url"] + "\">Open market</a>"
                )
                try:
                    await bot.send_message(chat_id, text, parse_mode="HTML",
                                           disable_web_page_preview=True)
                except Exception as e:
                    log.warning("price alert error %s: %s", chat_id, e)

                pred["last_price"] = current_price
                save_predictions()


# ─── Result checking ──────────────────────────────────────────────────────────

def get_winner_idx(market):
    try:
        prices = market.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        if prices and len(prices) >= 2:
            p0 = round(float(prices[0]), 4)
            p1 = round(float(prices[1]), 4)
            if p0 == 1.0 and p1 == 0.0:
                return 0
            if p0 == 0.0 and p1 == 1.0:
                return 1
    except Exception:
        pass
    return None


async def check_predictions(session):
    changed = False
    for chat_id, user_preds in predictions.items():
        for market_id, pred in list(user_preds.items()):
            if pred.get("outcome"):
                continue

            market = await fetch_market_by_id(session, market_id)
            if not market:
                continue

            winner_idx = get_winner_idx(market)
            if winner_idx is None:
                continue

            chosen_idx = pred["chosen_idx"]
            is_win = (chosen_idx == winner_idx)
            pred["outcome"] = "win" if is_win else "loss"
            changed = True

            entry_price = pred["entry_price"]
            pnl = round(PAPER_BET_SIZE * (1.0 / entry_price - 1), 2) if is_win else -PAPER_BET_SIZE

            teams = extract_teams(market)
            winner_name = teams[winner_idx] if winner_idx < len(teams) else "Unknown"

            result_text = (
                "<b>" + ("✅ Correct!" if is_win else "❌ Wrong!") + "</b>\n\n"
                + pred["question"] + "\n\n"
                + "Your pick: <b>" + pred["chosen_team"] + "</b>\n"
                + "Winner: <b>" + winner_name + "</b>\n"
                + "Entry: " + str(round(entry_price * 100)) + "c\n"
                + "Paper P&L: <b>" + ("+" if pnl >= 0 else "") + str(pnl) + "$</b>\n\n"
                + "<a href=\"" + pred["market_url"] + "\">View market</a>"
            )
            try:
                await bot.send_message(chat_id, result_text, parse_mode="HTML",
                                       disable_web_page_preview=True)
            except Exception as e:
                log.warning("send result error %s: %s", chat_id, e)

    if changed:
        save_predictions()


# ─── Stats ────────────────────────────────────────────────────────────────────

def get_user_stats(chat_id):
    user_preds = predictions.get(chat_id, {})
    total = len(user_preds)
    finished = [p for p in user_preds.values() if p.get("outcome")]
    wins = sum(1 for p in finished if p["outcome"] == "win")
    losses = len(finished) - wins
    pending = total - len(finished)
    win_rate = round(wins / len(finished) * 100) if finished else 0

    total_pnl = 0.0
    for p in finished:
        if p["outcome"] == "win":
            total_pnl += round(PAPER_BET_SIZE * (1.0 / p["entry_price"] - 1), 2)
        else:
            total_pnl -= PAPER_BET_SIZE

    return {
        "total": total, "finished": len(finished),
        "wins": wins, "losses": losses,
        "pending": pending, "win_rate": win_rate,
        "total_pnl": round(total_pnl, 2),
        "preds": user_preds,
    }


# ─── Tracker ──────────────────────────────────────────────────────────────────

async def tracker():
    global is_first_run
    async with aiohttp.ClientSession() as session:
        await refresh_hltv(session)
        result_counter = 0
        price_counter = 0
        while True:
            try:
                await refresh_hltv(session)
                markets = await fetch_markets(session)
                new_ones = []
                for m in markets:
                    mid = str(m.get("id", ""))
                    if mid and mid not in known_market_ids:
                        if not is_first_run:
                            new_ones.append(m)
                        known_market_ids.add(mid)

                if is_first_run:
                    log.info("First run: %d markets", len(markets))
                    is_first_run = False
                else:
                    log.info("Check: known=%d new=%d", len(known_market_ids), len(new_ones))
                    if new_ones:
                        save_known_markets()

                for market in new_ones:
                    text = new_market_text(market)
                    kb = prediction_keyboard(market)
                    for chat_id in list(subscribers):
                        try:
                            await bot.send_message(chat_id, text, parse_mode="HTML",
                                                   reply_markup=kb,
                                                   disable_web_page_preview=True)
                        except Exception as e:
                            log.warning("send error %s: %s", chat_id, e)
                            if "blocked" in str(e).lower() or "not found" in str(e).lower():
                                subscribers.discard(chat_id)
                                save_subscribers()

                result_counter += CHECK_INTERVAL
                if result_counter >= RESULT_CHECK_INTERVAL:
                    await check_predictions(session)
                    result_counter = 0

                price_counter += CHECK_INTERVAL
                if price_counter >= PRICE_CHECK_INTERVAL:
                    await check_price_changes(session)
                    price_counter = 0

            except Exception as e:
                log.error("tracker error: %s", e)
            await asyncio.sleep(CHECK_INTERVAL)


# ─── Handlers ─────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    subscribers.add(message.chat.id)
    save_subscribers()
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎮 Matches"), KeyboardButton(text="📈 My Stats")],
            [KeyboardButton(text="📊 HLTV Top-20"), KeyboardButton(text="ℹ️ Status")],
        ],
        resize_keyboard=True,
        persistent=True,
    )
    await message.answer(
        "<b>🎮 CS2 Polymarket Tracker</b>\n\n"
        "Tracks new CS2 matches and notifies you.\n"
        "Tap a team on match notifications to make paper predictions.\n\n"
        "/list - current CS2 matches\n"
        "/mystats - prediction stats\n"
        "/ranking - HLTV top 20\n"
        "/status - bot status\n"
        "/stop - unsubscribe",
        parse_mode="HTML",
        reply_markup=kb,
    )


# ─── Reply keyboard text handlers ─────────────────────────────────────────────

@dp.message(lambda m: m.text == "🎮 Matches")
async def btn_matches(message: types.Message):
    await cmd_list(message)


@dp.message(lambda m: m.text == "📈 My Stats")
async def btn_mystats(message: types.Message):
    await cmd_mystats(message)


@dp.message(lambda m: m.text == "📊 HLTV Top-20")
async def btn_ranking(message: types.Message):
    await cmd_ranking(message)


@dp.message(lambda m: m.text == "ℹ️ Status")
async def btn_status(message: types.Message):
    await cmd_status(message)


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    subscribers.discard(message.chat.id)
    save_subscribers()
    await message.answer("Unsubscribed. /start to subscribe again.")


# Кэш списка матчей для пагинации (chat_id -> list)
list_cache: dict = {}


def render_list_page(markets, offset=0):
    PAGE = 15
    chunk = markets[offset:offset + PAGE]
    lines = ["<b>🎮 CS2 matches on Polymarket (" + str(len(markets)) + " total):</b>\n"]
    for m in chunk:
        q = m.get("question", "?")[:55]
        url = market_url(m)
        match_time = format_match_time(m)
        line = matchup_line(m)
        lines.append(
            "- <a href=\"" + url + "\">" + q + "</a>\n"
            "  🕐 " + match_time + "\n"
            "  " + line
        )
    text = "\n\n".join(lines)
    shown = offset + len(chunk)
    remaining = len(markets) - shown
    kb = None
    if remaining > 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="📋 Show " + str(remaining) + " more",
                callback_data="list_more:" + str(shown)
            )
        ]])
    return text, kb


@dp.message(Command("list"))
async def cmd_list(message: types.Message):
    msg = await message.answer("Loading CS2 matches...")
    async with aiohttp.ClientSession() as session:
        await refresh_hltv(session)
        markets = await fetch_markets(session)
    if not markets:
        await msg.edit_text("No active CS2 matches right now. Bot will notify when new matches appear 🔔")
        return
    list_cache[message.chat.id] = markets
    text, kb = render_list_page(markets, offset=0)
    await msg.edit_text(text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)


@dp.message(Command("mystats"))
async def cmd_mystats(message: types.Message):
    chat_id = message.chat.id
    stats = get_user_stats(chat_id)

    if stats["total"] == 0:
        await message.answer("📊 No predictions yet!\n\nMake predictions by tapping team buttons on new match notifications.")
        return

    pnl_str = ("+" if stats["total_pnl"] >= 0 else "") + str(stats["total_pnl"])
    text = (
        "<b>📊 Your prediction stats</b>\n\n"
        "Total: " + str(stats["total"]) + " | Pending: " + str(stats["pending"]) + "\n\n"
        "✅ Wins: " + str(stats["wins"]) + "\n"
        "❌ Losses: " + str(stats["losses"]) + "\n"
        "Win rate: <b>" + str(stats["win_rate"]) + "%</b>\n\n"
        "Paper P&amp;L ($10/bet): <b>" + pnl_str + "$</b>\n\n"
        "─────────────────\n"
        "<b>Your picks:</b>\n\n"
    )

    for pred in stats["preds"].values():
        entry = pred["entry_price"]
        last = pred.get("last_price", entry)
        delta = last - entry
        delta_str = ("+" if delta >= 0 else "") + str(round(delta * 100)) + "c"

        if pred.get("outcome") == "win":
            icon = "✅"
        elif pred.get("outcome") == "loss":
            icon = "❌"
        else:
            icon = "⏳"

        try:
            ts = datetime.fromisoformat(pred["ts"]).strftime("%d.%m %H:%M")
        except Exception:
            ts = "?"

        price_info = ""
        if not pred.get("outcome"):
            price_info = " | now " + str(round(last * 100)) + "c (" + delta_str + ")"

        end_dt = pred.get("end_dt", "")
        match_time = ("   📅 " + end_dt + " UTC\n") if end_dt else ""

        # Экранируем спецсимволы HTML в названиях команд и вопросах
        question = pred["question"][:45].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        chosen = pred["chosen_team"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        text += (
            icon + " <a href=\"" + pred["market_url"] + "\">" + question + "</a>\n"
            + "   Pick: <b>" + chosen + "</b> @ " + str(round(entry * 100)) + "c"
            + price_info + "\n"
            + match_time
            + "   🕐 " + ts + "\n\n"
        )

    if len(text) > 4000:
        text = text[:4000] + "..."

    try:
        await message.answer(text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        log.warning("mystats send error: %s", e)
        # Отправляем упрощённую версию без HTML если парсинг упал
        await message.answer(
            "📊 Stats: " + str(stats["wins"]) + "W / " + str(stats["losses"]) + "L | "
            "Win rate: " + str(stats["win_rate"]) + "% | P&L: " + pnl_str + "$"
        )


@dp.message(Command("ranking"))
async def cmd_ranking(message: types.Message):
    ranking = hltv_ranking if hltv_ranking else FALLBACK_RANKING
    top = sorted(ranking.items(), key=lambda x: x[1])[:20]
    lines = ["<b>HLTV Top-20:</b>\n"]
    seen_ranks = set()
    for name, rank in top:
        if rank not in seen_ranks:
            seen_ranks.add(rank)
            lines.append("#" + str(rank) + " " + name.title())
    lines.append("\nSource: " + ("live HLTV" if hltv_ranking else "fallback"))
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    subbed = "yes" if message.chat.id in subscribers else "no"
    user_preds = predictions.get(message.chat.id, {})
    pending = sum(1 for p in user_preds.values() if not p.get("outcome"))
    await message.answer(
        "<b>Status</b>\n"
        "Subscribed: " + subbed + "\n"
        "Known markets: " + str(len(known_market_ids)) + "\n"
        "HLTV teams: " + str(len(hltv_ranking)) + "\n"
        "Predictions: " + str(len(user_preds)) + " total, " + str(pending) + " pending\n"
        "Interval: " + str(CHECK_INTERVAL) + "s",
        parse_mode="HTML",
    )


# ─── Callbacks ────────────────────────────────────────────────────────────────

@dp.callback_query(lambda c: c.data == "list")
async def cb_list(callback: types.CallbackQuery):
    await callback.answer("Loading...")
    await cmd_list(callback.message)


@dp.callback_query(lambda c: c.data and c.data.startswith("list_more:"))
async def cb_list_more(callback: types.CallbackQuery):
    await callback.answer()
    chat_id = callback.message.chat.id
    markets = list_cache.get(chat_id)
    if not markets:
        async with aiohttp.ClientSession() as session:
            markets = await fetch_markets(session)
        list_cache[chat_id] = markets
    try:
        offset = int(callback.data.split(":")[1])
    except Exception:
        offset = 0
    text, kb = render_list_page(markets, offset=offset)
    await callback.message.answer(text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)


@dp.callback_query(lambda c: c.data == "ranking")
async def cb_ranking(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_ranking(callback.message)


@dp.callback_query(lambda c: c.data == "mystats")
async def cb_mystats(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_mystats(callback.message)


@dp.callback_query(lambda c: c.data and c.data.startswith("pick:"))
async def cb_pick(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Error")
        return

    market_id = parts[1]
    chosen_idx = int(parts[2])
    chat_id = callback.message.chat.id

    if chat_id in predictions and market_id in predictions[chat_id]:
        existing = predictions[chat_id][market_id]
        await callback.answer(
            "Already picked " + existing["chosen_team"] + "!", show_alert=True
        )
        return

    async with aiohttp.ClientSession() as session:
        market = await fetch_market_by_id(session, market_id)

    if not market:
        await callback.answer("Could not load market", show_alert=True)
        return

    teams = extract_teams(market)
    if not teams or chosen_idx >= len(teams):
        await callback.answer("Market is no longer available", show_alert=True)
        return

    chosen_team = teams[chosen_idx]
    prices = get_prices(market)
    entry_price = prices[chosen_idx] if chosen_idx < len(prices) else 0.5

    if chat_id not in predictions:
        predictions[chat_id] = {}

    end_raw = market.get("endDate", "")
    try:
        end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).strftime("%d.%m.%Y %H:%M")
    except Exception:
        end_dt = ""

    predictions[chat_id][market_id] = {
        "question": market.get("question", "?"),
        "chosen_team": chosen_team,
        "chosen_idx": chosen_idx,
        "entry_price": entry_price,
        "last_price": entry_price,
        "market_url": market_url(market),
        "ts": datetime.now(timezone.utc).isoformat(),
        "end_dt": end_dt,
        "outcome": None,
    }
    save_predictions()

    pot_win = round(PAPER_BET_SIZE * (1.0 / entry_price - 1), 2) if entry_price > 0 else 0
    await callback.answer(
        "✅ Picked " + chosen_team + " @ " + str(round(entry_price * 100)) + "c\n"
        "Potential win: +$" + str(pot_win) + "\n"
        "Price alerts: ON (±7%)",
        show_alert=True
    )
    log.info("Prediction saved: chat=%s market=%s team=%s price=%.2f",
             chat_id, market_id, chosen_team, entry_price)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set!")

    # Загружаем данные при старте
    load_subscribers()
    load_predictions()
    load_known_markets()

    log.info("Starting bot... subscribers=%d predictions_users=%d known_markets=%d",
             len(subscribers), len(predictions), len(known_market_ids))

    asyncio.create_task(tracker())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())