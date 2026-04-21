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

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

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
        market.get("title", "") + " " +
        market.get("description", "") + " " +
        market.get("slug", "") + " " +
        market.get("conditionId", "")
    ).lower()
    return any(w in text for w in CS2_IDENTIFIERS)


def is_winner_market(market):
    question = (market.get("question", "") or market.get("title", "")).lower()
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
        v = float(market.get("volume", 0) or market.get("volume24hr", 0) or 0)
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
    question = market.get("question", market.get("title", "?"))
    volume = format_volume(market)
    end_raw = market.get("endDate", market.get("end_date_iso", ""))
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
        q = (m.get("question") or m.get("title", "?"))[:60]
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


async def fetch_from_clob(session):
    """Пробуем CLOB API — содержит все рынки включая esports."""
    results = []
    try:
        # CLOB markets endpoint
        next_cursor = ""
        pages = 0
        while pages < 10:
            params = {"active": "true", "closed": "false", "limit": "500"}
            if next_cursor:
                params["next_cursor"] = next_cursor
            async with session.get(
                CLOB_API + "/markets", params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                if r.status != 200:
                    log.warning("CLOB status %d", r.status)
                    break
                data = await r.json()
                items = data.get("data", [])
                for m in items:
                    if is_cs_market(m):
                        results.append(m)
                log.info("CLOB page %d: %d markets, %d CS2", pages, len(items), len(results))
                next_cursor = data.get("next_cursor", "")
                if not next_cursor or not items:
                    break
                pages += 1
    except Exception as e:
        log.warning("CLOB fetch failed: %s", e)
    return results


async def fetch_from_gamma_esports(session):
    """Пробуем gamma API с category/tag параметрами для esports."""
    results = []

    # Попытки с разными параметрами
    attempts = [
        {"category": "esports", "active": "true", "closed": "false", "limit": "200"},
        {"category": "Sports", "active": "true", "closed": "false", "limit": "200"},
        {"tag_slug": "esports", "active": "true", "closed": "false", "limit": "200"},
        {"tag_slug": "counter-strike", "active": "true", "closed": "false", "limit": "200"},
        {"tag_slug": "cs2", "active": "true", "closed": "false", "limit": "200"},
    ]

    for params in attempts:
        try:
            async with session.get(
                GAMMA_API + "/events", params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    events = data if isinstance(data, list) else data.get("events", data.get("data", []))
                    for event in events:
                        title = (event.get("title", "") + event.get("slug", "")).lower()
                        if any(w in title for w in CS2_IDENTIFIERS):
                            for m in event.get("markets", []):
                                results.append(m)
                    if results:
                        log.info("Gamma esports params %s: found %d CS2", params, len(results))
                        break
        except Exception:
            pass

    return results


async def fetch_markets(session):
    results = []

    # Метод 1: CLOB API (содержит все рынки)
    clob_results = await fetch_from_clob(session)
    results.extend(clob_results)

    # Метод 2: Gamma с esports параметрами
    if not results:
        gamma_results = await fetch_from_gamma_esports(session)
        results.extend(gamma_results)

    # Дедупликация и фильтрация
    seen = set()
    unique = []
    for m in results:
        mid = m.get("id") or m.get("condition_id")
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
        while True:
            try:
                await refresh_hltv(session)
                markets = await fetch_markets(session)
                new_ones = []
                for m in markets:
                    mid = str(m.get("id") or m.get("condition_id", ""))
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
        "Interval: " + str(CHECK_INTERVAL) + "s",
        parse_mode="HTML",
    )


@dp.message(Command("debug"))
async def cmd_debug(message: types.Message):
    msg = await message.answer("Running diagnostics (20-30s)...")
    lines = ["<b>API Debug v4</b>\n"]

    async with aiohttp.ClientSession() as session:

        # Тест 1: CLOB API /markets
        try:
            async with session.get(
                CLOB_API + "/markets",
                params={"active": "true", "limit": "5"},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                lines.append("CLOB /markets status: <b>" + str(r.status) + "</b>")
                if r.status == 200:
                    data = await r.json()
                    items = data.get("data", data if isinstance(data, list) else [])
                    cs = [m for m in items if is_cs_market(m)]
                    lines.append("Items: " + str(len(items)) + ", CS2: " + str(len(cs)))
                    if items:
                        sample = items[0]
                        lines.append("Keys: " + str(list(sample.keys()))[:100])
                        lines.append("Sample: " + str(sample.get("question", sample.get("market_slug", "?")))[:60])
        except Exception as e:
            lines.append("CLOB test failed: " + str(e)[:80])

        # Тест 2: Gamma /events с category=esports
        for cat in ["esports", "Esports", "sports", "Sports"]:
            try:
                async with session.get(
                    GAMMA_API + "/events",
                    params={"category": cat, "active": "true", "limit": "5"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        events = data if isinstance(data, list) else data.get("events", data.get("data", []))
                        if events:
                            lines.append("\nGamma category='" + cat + "': <b>" + str(len(events)) + " events</b>")
                            for e in events[:3]:
                                lines.append("  • " + e.get("title", "?")[:50])
                            break
            except Exception:
                pass

        # Тест 3: Gamma /events с tag_slug
        for slug in ["esports", "counter-strike", "cs2", "gaming"]:
            try:
                async with session.get(
                    GAMMA_API + "/events",
                    params={"tag_slug": slug, "active": "true", "limit": "5"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        events = data if isinstance(data, list) else data.get("events", data.get("data", []))
                        if events:
                            lines.append("\nGamma tag_slug='" + slug + "': <b>" + str(len(events)) + "</b>")
                            for e in events[:2]:
                                lines.append("  • " + e.get("title", "?")[:50])
            except Exception:
                pass

        # Тест 4: Polymarket strapi/internal API
        try:
            async with session.get(
                "https://polymarket.com/api/events",
                params={"category": "esports", "limit": "5"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                lines.append("\nPolymarket /api/events status: " + str(r.status))
        except Exception as e:
            lines.append("\nPolymarket API: " + str(e)[:60])

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