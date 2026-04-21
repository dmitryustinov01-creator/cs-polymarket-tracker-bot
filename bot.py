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
HLTV_UPDATE_INTERVAL = 3600

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

subscribers = set()
known_market_ids = set()
is_first_run = True
hltv_ranking = {}
hltv_last_updated = None
cs2_tag_ids = []

GAMMA_API = "https://gamma-api.polymarket.com"

CS2_IDENTIFIERS = [
    "counter-strike", "cs2", "csgo", "cs:go"
]

WINNER_EXCLUDE = [
    "map 1", "map 2", "map 3", "map 4", "map 5",
    "first map", "pistol", "knife round", "total maps",
    "half", "round", "first blood", "first kill",
    "overtime", "ace", "bomb", "most kills", "most damage",
    "first to", "rating", "mvp", "reach", "handicap",
    "what will be said", "which maps", "roster"
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


def is_cs_market(market):
    text = (
        market.get("question", "") + " " +
        market.get("description", "") + " " +
        market.get("groupItemTitle", "") + " " +
        market.get("category", "") + " " +
        market.get("slug", "")
    ).lower()
    return any(w in text for w in CS2_IDENTIFIERS)


def is_winner_market(market):
    question = market.get("question", "").lower()
    if any(w in question for w in WINNER_EXCLUDE):
        return False
    return True


def get_price(market, idx):
    try:
        prices = market.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        return str(round(float(prices[idx]) * 100)) + "c"
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
                 if str(o).strip().lower() not in ("yes", "no", "draw", "other", "neither")]
        if teams:
            return teams[:2]
    except Exception:
        pass
    return []


def matchup_line(market):
    teams = extract_teams(market)
    p0 = get_price(market, 0)
    p1 = get_price(market, 1)
    if len(teams) >= 2:
        r0 = get_team_rank(teams[0])
        r1 = get_team_rank(teams[1])
        t0 = teams[0] + (" " + r0 if r0 else "") + " " + p0
        t1 = teams[1] + (" " + r1 if r1 else "") + " " + p1
        return t0 + " vs " + t1
    if len(teams) == 1:
        r0 = get_team_rank(teams[0])
        return teams[0] + (" " + r0 if r0 else "") + " " + p0 + " | NO " + p1
    return "YES " + p0 + " / NO " + p1


def new_market_text(market):
    question = market.get("question", "?")
    volume = format_volume(market)
    end_raw = market.get("endDate", "")
    try:
        end_date = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).strftime("%d.%m.%Y")
    except Exception:
        end_date = "?"
    url = market_url(market)
    line = matchup_line(market)
    return (
        "<b>New CS2 market on Polymarket!</b>\n\n"
        + question + "\n\n"
        + line + "\n\n"
        + "Volume: " + volume + " | Closes: " + end_date + "\n"
        + "<a href=\"" + url + "\">Open market</a>"
    )


def list_text(markets):
    if not markets:
        return "No active CS2 markets found on Polymarket."
    lines = ["<b>CS2 markets on Polymarket:</b>\n"]
    for m in markets[:15]:
        q = m.get("question", "?")[:60]
        url = market_url(m)
        line = matchup_line(m)
        lines.append("- <a href=\"" + url + "\">" + q + "</a>\n  " + line)
    if len(markets) > 15:
        lines.append("...and " + str(len(markets) - 15) + " more")
    return "\n\n".join(lines)


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


async def find_cs2_tag_ids(session):
    global cs2_tag_ids
    try:
        # Пробуем получить все теги (без лимита)
        async with session.get(
            GAMMA_API + "/tags",
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status == 200:
                tags = await r.json()
                found = []
                for t in tags:
                    label = (t.get("label", "") + " " + t.get("slug", "")).lower()
                    if any(w in label for w in [
                        "cs2", "counter-strike", "counter strike", "csgo",
                        "esport", "esports"
                    ]):
                        found.append(str(t.get("id", "")))
                        log.info("Found tag: %s id=%s", t.get("label"), t.get("id"))
                if found:
                    cs2_tag_ids = found
                log.info("Tags total: %d, CS2/esports found: %d", len(tags), len(found))
    except Exception as e:
        log.warning("Tag search failed: %s", e)


async def fetch_events_by_volume(session, offset=0):
    """Тянем события отсортированные по volume — CS2 матчи с большим объёмом будут в топе."""
    try:
        params = {
            "active": "true",
            "closed": "false",
            "limit": "100",
            "offset": str(offset),
            "order": "volume",
            "ascending": "false"
        }
        async with session.get(
            GAMMA_API + "/events", params=params,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status != 200:
                return []
            data = await r.json()
            return data if isinstance(data, list) else data.get("events", data.get("data", []))
    except Exception as e:
        log.warning("fetch_events_by_volume offset=%d failed: %s", offset, e)
        return []


async def fetch_events_by_end_date(session, offset=0):
    """Тянем события которые скоро закончатся — активные матчи."""
    try:
        params = {
            "active": "true",
            "closed": "false",
            "limit": "100",
            "offset": str(offset),
            "order": "endDate",
            "ascending": "true"
        }
        async with session.get(
            GAMMA_API + "/events", params=params,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status != 200:
                return []
            data = await r.json()
            return data if isinstance(data, list) else data.get("events", data.get("data", []))
    except Exception as e:
        log.warning("fetch_events_by_end_date offset=%d failed: %s", offset, e)
        return []


async def fetch_markets_by_tag(session):
    results = []
    for tag_id in cs2_tag_ids:
        try:
            params = {"tag_id": tag_id, "active": "true", "closed": "false", "limit": "200"}
            async with session.get(
                GAMMA_API + "/markets", params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    items = data if isinstance(data, list) else data.get("markets", [])
                    results.extend(items)
                    log.info("tag_id %s: %d markets", tag_id, len(items))
        except Exception as e:
            log.warning("tag_id %s failed: %s", tag_id, e)
    return results


def extract_cs2_from_events(events):
    markets = []
    for event in events:
        title = (
            event.get("title", "") + " " +
            event.get("description", "") + " " +
            event.get("slug", "")
        ).lower()
        if any(w in title for w in CS2_IDENTIFIERS):
            for m in event.get("markets", []):
                markets.append(m)
    return markets


async def fetch_markets(session):
    results = []

    # Метод 1: по тег ID если нашли
    if cs2_tag_ids:
        results.extend(await fetch_markets_by_tag(session))

    # Метод 2: события по volume (CS2 с $3M объёмом будут вверху)
    events_vol = await fetch_events_by_volume(session, offset=0)
    cs2_from_vol = extract_cs2_from_events(events_vol)
    log.info("By volume: %d events, %d CS2 markets", len(events_vol), len(cs2_from_vol))
    results.extend(cs2_from_vol)

    # Метод 3: события по endDate (ближайшие матчи)
    events_end = await fetch_events_by_end_date(session, offset=0)
    cs2_from_end = extract_cs2_from_events(events_end)
    log.info("By endDate: %d events, %d CS2 markets", len(events_end), len(cs2_from_end))
    results.extend(cs2_from_end)

    # Метод 4: если ничего не нашли — пагинируем глубже по endDate
    if not results:
        for offset in [100, 200, 300, 400, 500]:
            events = await fetch_events_by_end_date(session, offset=offset)
            if not events:
                break
            cs2 = extract_cs2_from_events(events)
            results.extend(cs2)
            log.info("Deep search offset=%d: %d events, %d CS2", offset, len(events), len(cs2))
            if cs2:
                break  # нашли — дальше не ищем

    # Дедупликация и фильтрация
    seen = set()
    unique = []
    for m in results:
        mid = m.get("id")
        if mid and mid not in seen:
            if is_cs_market(m) and is_winner_market(m):
                seen.add(mid)
                unique.append(m)

    log.info("Total unique CS2 winner markets: %d", len(unique))
    return unique


async def tracker():
    global is_first_run
    async with aiohttp.ClientSession() as session:
        await refresh_hltv(session)
        await find_cs2_tag_ids(session)
        while True:
            try:
                await refresh_hltv(session)
                if not cs2_tag_ids:
                    await find_cs2_tag_ids(session)
                markets = await fetch_markets(session)
                new_ones = []
                for m in markets:
                    mid = str(m.get("id", ""))
                    if mid and mid not in known_market_ids:
                        if not is_first_run:
                            new_ones.append(m)
                        known_market_ids.add(mid)
                if is_first_run:
                    log.info("First run: %d CS2 winner markets", len(markets))
                    is_first_run = False
                else:
                    log.info("Check: known=%d new=%d", len(known_market_ids), len(new_ones))
                for market in new_ones:
                    text = new_market_text(market)
                    kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="Open market", url=market_url(market))
                    ]])
                    for chat_id in list(subscribers):
                        try:
                            await bot.send_message(chat_id, text, parse_mode="HTML",
                                                   reply_markup=kb, disable_web_page_preview=True)
                        except Exception as e:
                            log.warning("send error %d: %s", chat_id, e)
                            if "blocked" in str(e).lower() or "not found" in str(e).lower():
                                subscribers.discard(chat_id)
            except Exception as e:
                log.error("tracker error: %s", e)
            await asyncio.sleep(CHECK_INTERVAL)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    subscribers.add(message.chat.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Current CS2 markets", callback_data="list")],
        [InlineKeyboardButton(text="HLTV Top-20", callback_data="ranking")],
    ])
    await message.answer(
        "<b>CS2 Polymarket Tracker</b>\n\n"
        "Tracking new CS2 markets on Polymarket.\n"
        "You will get a notification when a new market appears.\n"
        "Check interval: " + str(CHECK_INTERVAL // 60) + " min\n\n"
        "/list - current CS2 markets\n"
        "/ranking - HLTV top 20\n"
        "/status - bot status\n"
        "/debug - API diagnostics\n"
        "/stop - unsubscribe",
        parse_mode="HTML",
        reply_markup=kb,
    )


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    subscribers.discard(message.chat.id)
    await message.answer("Unsubscribed. /start to subscribe again.")


@dp.message(Command("list"))
async def cmd_list(message: types.Message):
    msg = await message.answer("Loading CS2 markets...")
    async with aiohttp.ClientSession() as session:
        await refresh_hltv(session)
        markets = await fetch_markets(session)
    await msg.edit_text(list_text(markets), parse_mode="HTML", disable_web_page_preview=True)


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
    await message.answer(
        "<b>Status</b>\n"
        "Subscribed: " + subbed + "\n"
        "Known markets: " + str(len(known_market_ids)) + "\n"
        "HLTV teams: " + str(len(hltv_ranking)) + "\n"
        "CS2 tag IDs found: " + str(cs2_tag_ids) + "\n"
        "Interval: " + str(CHECK_INTERVAL) + "s",
        parse_mode="HTML",
    )


@dp.message(Command("debug"))
async def cmd_debug(message: types.Message):
    msg = await message.answer("Running diagnostics (15-20s)...")
    lines = ["<b>API Debug v3</b>\n"]

    async with aiohttp.ClientSession() as session:
        # Тест 1: события по volume
        try:
            params = {"active": "true", "closed": "false", "limit": "10",
                      "order": "volume", "ascending": "false"}
            async with session.get(GAMMA_API + "/events", params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                events = data if isinstance(data, list) else data.get("events", data.get("data", []))
                cs = [e for e in events if any(
                    w in (e.get("title","") + e.get("slug","")).lower() for w in CS2_IDENTIFIERS)]
                lines.append("Top 10 by volume: <b>" + str(len(events)) + " events</b>")
                lines.append("CS2 found: <b>" + str(len(cs)) + "</b>")
                lines.append("Titles:")
                for e in events[:5]:
                    lines.append("  • " + e.get("title", "?")[:50])
        except Exception as e:
            lines.append("Volume test failed: " + str(e))

        # Тест 2: события по endDate
        try:
            params = {"active": "true", "closed": "false", "limit": "10",
                      "order": "endDate", "ascending": "true"}
            async with session.get(GAMMA_API + "/events", params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                events = data if isinstance(data, list) else data.get("events", data.get("data", []))
                cs = [e for e in events if any(
                    w in (e.get("title","") + e.get("slug","")).lower() for w in CS2_IDENTIFIERS)]
                lines.append("\nNearest by endDate: <b>" + str(len(events)) + "</b>")
                lines.append("CS2 found: <b>" + str(len(cs)) + "</b>")
                for e in events[:5]:
                    lines.append("  • " + e.get("title", "?")[:50])
        except Exception as e:
            lines.append("EndDate test failed: " + str(e))

        # Тест 3: markets по volume
        try:
            params = {"active": "true", "closed": "false", "limit": "10",
                      "order": "volume", "ascending": "false"}
            async with session.get(GAMMA_API + "/markets", params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                items = data if isinstance(data, list) else data.get("markets", [])
                cs = [m for m in items if any(
                    w in (m.get("question","") + m.get("slug","")).lower() for w in CS2_IDENTIFIERS)]
                lines.append("\nTop 10 markets by volume: <b>" + str(len(items)) + "</b>")
                lines.append("CS2 found: <b>" + str(len(cs)) + "</b>")
                for m in items[:5]:
                    lines.append("  • " + m.get("question", "?")[:50])
        except Exception as e:
            lines.append("Markets test failed: " + str(e))

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "..."
    await msg.edit_text(text, parse_mode="HTML")


@dp.callback_query(lambda c: c.data == "list")
async def cb_list(callback: types.CallbackQuery):
    await callback.answer("Loading...")
    async with aiohttp.ClientSession() as session:
        await refresh_hltv(session)
        markets = await fetch_markets(session)
    await callback.message.answer(list_text(markets), parse_mode="HTML", disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == "ranking")
async def cb_ranking(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_ranking(callback.message)


async def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set!")
    log.info("Starting bot...")
    asyncio.create_task(tracker())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())