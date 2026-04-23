import asyncio
import logging
import os
import json
import re
from datetime import datetime, timezone

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "120"))
RESULT_CHECK_INTERVAL = 600  # проверка результатов каждые 10 минут
HLTV_UPDATE_INTERVAL = 3600
PAPER_BET_SIZE = 10.0  # $10 на каждый прогноз

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

subscribers = set()
known_market_ids = set()
is_first_run = True
hltv_ranking = {}
hltv_last_updated = None

# paper trading хранилище
# predictions[chat_id][market_id] = {
#   "question": str, "chosen_team": str, "chosen_idx": int,
#   "entry_price": float, "market_url": str, "ts": str,
#   "outcome": None | "win" | "loss"
# }
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
    "complexity": 11, "col": 11,
    "ence": 12,
    "cloud9": 13, "c9": 13,
    "big": 14,
    "eternal fire": 15,
    "fnatic": 16,
    "pain": 17, "pain gaming": 17,
    "3dmax": 18,
    "mibr": 19,
    "imperial": 20, "imperial esports": 20,
    "virtus.pro": 21, "vp": 21,
    "flyquest": 22,
    "monte": 23,
    "saw": 24,
    "apeks": 25,
    "b8": 26,
    "betboom": 27, "betboom team": 27,
    "parivision": 28,
    "aurora": 29,
    "100 thieves": 30,
    "falcons": 31, "team falcons": 31,
    "furia": 32,
    "rare atom": 33,
    "sangal": 34, "sangal esports": 34,
    "sinners": 35, "sinners esports": 35,
    "oddik": 36,
    "ecstatic": 37,
    "nemiga": 38,
    "pera": 39, "pera esports": 39,
    "9z": 40, "9z team": 40,
    "sprout": 41,
    "rebels": 42, "rebels gaming": 42,
    "sashi": 43, "sashi esports": 43,
    "unity": 44, "unity esports": 44, "unify esports": 44,
    "into the breach": 45,
    "endpoint": 46,
    "entropiq": 47,
    "young ninjas": 48,
    "k23": 49,
    "eyeballers": 50,
    "passion ua": 51,
    "gamerlegion": 52,
    "movistar riders": 53,
    "og": 54,
    "forze": 55,
    "copenhagen flames": 56,
    "tricked": 57,
    "tyloo": 58,
    "atox": 59,
    "true rippers": 60,
    "imperial academy": 61,
    "big academy": 62,
    "g2 ares": 63,
    "aurora young blood": 64,
    "b8 academy": 65,
    "navi junior": 66,
    "lph gaming": 67,
    "infurity gaming": 68,
    "drama esports": 69,
    "kolesie": 70,
    "ctrl alt defeat": 71,
    "oldboys": 72,
    "clair obscur": 73,
    "misa esports": 74,
    "aimclub": 75,
    "yngods": 76,
    "hashiras": 77,
    "rustec": 78,
    "eternal premium": 79,
    "glitchtech esports": 80, "glitchtech": 80,
    "havens": 81,
    "rethink": 82,
    "enjoy": 83,
    "jumbo team": 84,
    "nerve of cow": 85,
    "playersclub": 86,
    "masonic": 87,
    "megoshort": 88,
    "csdiilit": 89,
    "donstu esports": 90, "donstu": 90,
    "depo": 91,
    "sangal alters": 92,
    "fokus reality": 93,
}


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
        prices = get_prices(market)
        return str(round(prices[idx] * 100)) + "c"
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
    """Кнопки для прогноза — имена команд."""
    mid = str(market.get("id", ""))
    teams = extract_teams(market)
    if len(teams) >= 2:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🟢 " + teams[0][:20],
                    callback_data="pick:" + mid + ":0"
                ),
                InlineKeyboardButton(
                    text="🟢 " + teams[1][:20],
                    callback_data="pick:" + mid + ":1"
                ),
            ],
            [InlineKeyboardButton(text="Open market", url=market_url(market))]
        ])
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Open market", url=market_url(market))
    ]])


def new_market_text(market):
    question = market.get("question", "?")
    volume = format_volume(market)
    end_raw = market.get("endDate", "")
    try:
        end_date = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).strftime("%d.%m.%Y")
    except Exception:
        end_date = "?"
    line = matchup_line(market)
    return (
        "<b>🎮 New CS2 match on Polymarket!</b>\n\n"
        + question + "\n\n"
        + line + "\n\n"
        + "Volume: " + volume + " | Closes: " + end_date + "\n\n"
        + "📊 Make your prediction:"
    )


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
    """Получить один рынок по ID для проверки результата."""
    try:
        async with session.get(
            GAMMA_API + "/markets/" + str(market_id),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.warning("fetch_market_by_id %s failed: %s", market_id, e)
    return None


# ─── Paper trading ────────────────────────────────────────────────────────────

def get_winner_idx(market):
    """Определить победителя по ценам. Возвращает 0 или 1, или None."""
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
    """Проверяем все незавершённые прогнозы."""
    for chat_id, user_preds in predictions.items():
        for market_id, pred in list(user_preds.items()):
            if pred.get("outcome"):
                continue  # уже завершён

            market = await fetch_market_by_id(session, market_id)
            if not market:
                continue

            winner_idx = get_winner_idx(market)
            if winner_idx is None:
                continue  # матч ещё не завершён

            # Определяем исход
            chosen_idx = pred["chosen_idx"]
            is_win = (chosen_idx == winner_idx)
            pred["outcome"] = "win" if is_win else "loss"

            # Считаем P&L
            entry_price = pred["entry_price"]
            if is_win:
                pnl = round(PAPER_BET_SIZE * (1.0 / entry_price - 1), 2)
            else:
                pnl = -PAPER_BET_SIZE

            teams = extract_teams(market)
            winner_name = teams[winner_idx] if winner_idx < len(teams) else "Unknown"
            chosen_name = pred["chosen_team"]

            result_text = (
                "<b>" + ("✅ Correct!" if is_win else "❌ Wrong!") + "</b>\n\n"
                + "Match: " + pred["question"] + "\n"
                + "Your pick: <b>" + chosen_name + "</b>\n"
                + "Winner: <b>" + winner_name + "</b>\n"
                + "Entry price: " + str(round(entry_price * 100)) + "c\n"
                + "Paper P&L: <b>" + ("+" if pnl >= 0 else "") + str(pnl) + "$</b>\n\n"
                + "<a href=\"" + pred["market_url"] + "\">View market</a>"
            )

            try:
                await bot.send_message(chat_id, result_text, parse_mode="HTML",
                                       disable_web_page_preview=True)
            except Exception as e:
                log.warning("send result error %s: %s", chat_id, e)


def get_user_stats(chat_id):
    """Считаем статистику пользователя."""
    user_preds = predictions.get(chat_id, {})
    total = len(user_preds)
    finished = [p for p in user_preds.values() if p.get("outcome")]
    wins = sum(1 for p in finished if p["outcome"] == "win")
    losses = len(finished) - wins

    total_pnl = 0.0
    for p in finished:
        if p["outcome"] == "win":
            total_pnl += round(PAPER_BET_SIZE * (1.0 / p["entry_price"] - 1), 2)
        else:
            total_pnl -= PAPER_BET_SIZE

    pending = total - len(finished)
    win_rate = round(wins / len(finished) * 100) if finished else 0

    return {
        "total": total,
        "finished": len(finished),
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "win_rate": win_rate,
        "total_pnl": round(total_pnl, 2),
    }


# ─── Tracker ──────────────────────────────────────────────────────────────────

async def tracker():
    global is_first_run
    async with aiohttp.ClientSession() as session:
        await refresh_hltv(session)
        result_counter = 0
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

                # Проверяем результаты каждые RESULT_CHECK_INTERVAL секунд
                result_counter += CHECK_INTERVAL
                if result_counter >= RESULT_CHECK_INTERVAL:
                    await check_predictions(session)
                    result_counter = 0

            except Exception as e:
                log.error("tracker error: %s", e)
            await asyncio.sleep(CHECK_INTERVAL)


# ─── Handlers ─────────────────────────────────────────────────────────────────

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🎮 Matches"), KeyboardButton(text="📈 My Stats")],
        [KeyboardButton(text="📊 HLTV Top-20"), KeyboardButton(text="ℹ️ Status")],
    ],
    resize_keyboard=True,
    persistent=True,
)


async def send_matches(target, markets):
    """Отправляет матчи с кнопками прогноза. target — message или chat."""
    if not markets:
        await target.answer(
            "No active CS2 matches right now. Bot will notify when new matches appear 🔔",
            reply_markup=MAIN_MENU,
        )
        return

    await target.answer(
        "<b>🎮 CS2 matches — " + str(len(markets)) + " active</b>\n"
        "Tap a team name to make a paper prediction 👇",
        parse_mode="HTML",
        reply_markup=MAIN_MENU,
    )
    for m in markets[:15]:
        question = m.get("question", "?")
        line = matchup_line(m)
        text = "<b>" + question[:80] + "</b>\n" + line
        kb = prediction_keyboard(m)
        await target.answer(text, parse_mode="HTML", reply_markup=kb,
                            disable_web_page_preview=True)
        await asyncio.sleep(0.2)

    if len(markets) > 15:
        await target.answer("...and " + str(len(markets) - 15) + " more matches.")


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    subscribers.add(message.chat.id)
    await message.answer(
        "<b>🎮 CS2 Polymarket Tracker</b>\n\n"
        "Tracking CS2 matches on Polymarket.\n"
        "Tap team buttons to make paper predictions!\n\n"
        "Use the menu below 👇",
        parse_mode="HTML",
        reply_markup=MAIN_MENU,
    )


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    subscribers.discard(message.chat.id)
    await message.answer("Unsubscribed. Send /start to subscribe again.")


async def show_matches(message: types.Message):
    msg = await message.answer("Loading CS2 matches...", reply_markup=MAIN_MENU)
    async with aiohttp.ClientSession() as session:
        await refresh_hltv(session)
        markets = await fetch_markets(session)
    await msg.delete()
    await send_matches(message, markets)


@dp.message(Command("list"))
async def cmd_list(message: types.Message):
    await show_matches(message)


@dp.message(lambda m: m.text == "🎮 Matches")
async def btn_matches(message: types.Message):
    await show_matches(message)


async def show_mystats(message: types.Message):
    chat_id = message.chat.id
    stats = get_user_stats(chat_id)

    if stats["total"] == 0:
        await message.answer(
            "📊 No predictions yet!\n\n"
            "Tap team buttons on match notifications to make predictions.",
            parse_mode="HTML",
            reply_markup=MAIN_MENU,
        )
        return

    pnl_str = ("+" if stats["total_pnl"] >= 0 else "") + str(stats["total_pnl"])
    text = (
        "<b>📈 Your prediction stats</b>\n\n"
        "Total: " + str(stats["total"]) + " | "
        "✅ " + str(stats["wins"]) + " | "
        "❌ " + str(stats["losses"]) + " | "
        "⏳ " + str(stats["pending"]) + "\n"
        "Win rate: <b>" + str(stats["win_rate"]) + "%</b>\n"
        "Paper P&L ($10/bet): <b>" + pnl_str + "$</b>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=MAIN_MENU)

    # История ставок — последние 20
    user_preds = predictions.get(chat_id, {})
    if not user_preds:
        return

    sorted_preds = sorted(
        user_preds.items(),
        key=lambda x: x[1].get("ts", ""),
        reverse=True
    )[:20]

    lines = ["<b>📋 Bet history (last 20):</b>\n"]
    for mid, p in sorted_preds:
        outcome = p.get("outcome")
        if outcome == "win":
            icon = "✅"
            pnl = round(PAPER_BET_SIZE * (1.0 / p["entry_price"] - 1), 2)
            pnl_s = "+" + str(pnl) + "$"
        elif outcome == "loss":
            icon = "❌"
            pnl_s = "-" + str(PAPER_BET_SIZE) + "$"
        else:
            icon = "⏳"
            pnl_s = "pending"

        q = p.get("question", "?")
        q = q.replace("Counter-Strike: ", "").replace("Counter-Strike:", "")
        if " (" in q:
            q = q[:q.index(" (")]
        q = q[:40]

        chosen = p.get("chosen_team", "?")[:15]
        price = str(round(p.get("entry_price", 0.5) * 100)) + "c"

        lines.append(icon + " " + q + "\n   → " + chosen + " @ " + price + " | " + pnl_s)

    history_text = "\n".join(lines)
    if len(history_text) > 4000:
        history_text = history_text[:4000] + "..."
    await message.answer(history_text, parse_mode="HTML")


@dp.message(Command("mystats"))
async def cmd_mystats(message: types.Message):
    await show_mystats(message)


@dp.message(lambda m: m.text == "📈 My Stats")
async def btn_mystats(message: types.Message):
    await show_mystats(message)


async def show_ranking(message: types.Message):
    ranking = hltv_ranking if hltv_ranking else FALLBACK_RANKING
    top = sorted(ranking.items(), key=lambda x: x[1])[:20]
    lines = ["<b>📊 HLTV Top-20:</b>\n"]
    seen_ranks = set()
    for name, rank in top:
        if rank not in seen_ranks:
            seen_ranks.add(rank)
            lines.append("#" + str(rank) + " " + name.title())
    lines.append("\nSource: " + ("live HLTV" if hltv_ranking else "fallback"))
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=MAIN_MENU)


@dp.message(Command("ranking"))
async def cmd_ranking(message: types.Message):
    await show_ranking(message)


@dp.message(lambda m: m.text == "📊 HLTV Top-20")
async def btn_ranking(message: types.Message):
    await show_ranking(message)


async def show_status(message: types.Message):
    subbed = "yes" if message.chat.id in subscribers else "no"
    user_preds = predictions.get(message.chat.id, {})
    pending = sum(1 for p in user_preds.values() if not p.get("outcome"))
    await message.answer(
        "<b>ℹ️ Status</b>\n"
        "Subscribed: " + subbed + "\n"
        "Known markets: " + str(len(known_market_ids)) + "\n"
        "HLTV teams: " + str(len(hltv_ranking)) + "\n"
        "Your predictions: " + str(len(user_preds)) + " total, " + str(pending) + " pending\n"
        "Check interval: " + str(CHECK_INTERVAL) + "s",
        parse_mode="HTML",
        reply_markup=MAIN_MENU,
    )


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    await show_status(message)


@dp.message(lambda m: m.text == "ℹ️ Status")
async def btn_status(message: types.Message):
    await show_status(message)


# ─── Callbacks ────────────────────────────────────────────────────────────────

@dp.callback_query(lambda c: c.data == "list")
async def cb_list(callback: types.CallbackQuery):
    await callback.answer("Loading...")
    async with aiohttp.ClientSession() as session:
        await refresh_hltv(session)
        markets = await fetch_markets(session)
    await send_matches(callback.message, markets)


@dp.callback_query(lambda c: c.data == "ranking")
async def cb_ranking(callback: types.CallbackQuery):
    await callback.answer()
    await show_ranking(callback.message)


@dp.callback_query(lambda c: c.data == "mystats")
async def cb_mystats(callback: types.CallbackQuery):
    await callback.answer()
    await show_mystats(callback.message)


@dp.callback_query(lambda c: c.data and c.data.startswith("pick:"))
async def cb_pick(callback: types.CallbackQuery):
    """Обработка прогноза: pick:{market_id}:{team_idx}"""
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Error")
        return

    market_id = parts[1]
    chosen_idx = int(parts[2])
    chat_id = callback.message.chat.id

    # Проверяем не сделан ли уже прогноз
    if chat_id in predictions and market_id in predictions[chat_id]:
        existing = predictions[chat_id][market_id]
        await callback.answer(
            "You already picked " + existing["chosen_team"] + "!", show_alert=True
        )
        return

    # Получаем данные рынка
    async with aiohttp.ClientSession() as session:
        market = await fetch_market_by_id(session, market_id)

    if not market:
        await callback.answer("Could not load market data", show_alert=True)
        return

    teams = extract_teams(market)
    if chosen_idx >= len(teams):
        await callback.answer("Error: team not found", show_alert=True)
        return

    chosen_team = teams[chosen_idx]
    prices = get_prices(market)
    entry_price = prices[chosen_idx] if chosen_idx < len(prices) else 0.5

    # Сохраняем прогноз
    if chat_id not in predictions:
        predictions[chat_id] = {}

    predictions[chat_id][market_id] = {
        "question": market.get("question", "?"),
        "chosen_team": chosen_team,
        "chosen_idx": chosen_idx,
        "entry_price": entry_price,
        "market_url": market_url(market),
        "ts": datetime.now(timezone.utc).isoformat(),
        "outcome": None,
    }

    pot_win = round(PAPER_BET_SIZE * (1.0 / entry_price - 1), 2) if entry_price > 0 else 0

    await callback.answer(
        "✅ Picked " + chosen_team + " @ " + str(round(entry_price * 100)) + "c\n"
        "Potential win: +$" + str(pot_win),
        show_alert=True
    )
    log.info("Prediction: chat=%s market=%s team=%s price=%.2f",
             chat_id, market_id, chosen_team, entry_price)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set!")
    log.info("Starting bot...")
    asyncio.create_task(tracker())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())