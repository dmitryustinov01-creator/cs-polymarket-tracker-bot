asyncio
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

FALLBACK_RANKING = {
    "vitality": 1,
    "natus vincere": 2,
    "navi": 2,
    "faze": 3,
    "faze clan": 3,
    "g2": 4,
    "g2 esports": 4,
    "spirit": 5,
    "team spirit": 5,
    "liquid": 6,
    "team liquid": 6,
    "mouz": 7,
    "heroic": 8,
    "astralis": 9,
    "nip": 10,
    "complexity": 11,
    "ence": 12,
    "cloud9": 13,
    "big": 14,
    "eternal fire": 15,
    "fnatic": 16,
    "pain": 17,
    "3dmax": 18,
    "mibr": 19,
    "virtus.pro": 21,
    "flyquest": 22,
    "monte": 23,
    "saw": 24,
    "apeks": 25,
    "b8": 26,
    "betboom": 27,
    "betboom team": 27,
    "parivision": 28,
    "100 thieves": 29,
    "aurora": 30,
}


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
    if not name or not hltv_ranking:
        return ""
    n = name.strip().lower()
    if n in hltv_ranking:
        return "#" + str(hltv_ranking[n])
    for key, rank in hltv_ranking.items():
        if key in n or n in key:
            return "#" + str(rank)
    return ""


def extract_teams(market):
    try:
        raw = market.get("outcomes", "[]")
        outcomes = json.loads(raw) if isinstance(raw, str) else raw
        teams = [str(o).strip() for o in outcomes
                 if str(o).strip().lower() not in ("yes", "no", "draw", "other")]
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
        return "No active CS2 markets found."
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
    headers = {"User-Agent": ua, "Accept-Language": "en-US,en;q=0.9"}
    try:
        async with session.get(
            "https://www.hltv.org/ranking/teams",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                return FALLBACK_RANKING.copy()
            html = await r.text()
            ranking = {}
            blocks = re.findall(
                r'class="position">#(\d+).*?class="name">(.*?)<',
                html, re.DOTALL
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


async def refresh_hltv(session):
    global hltv_ranking, hltv_last_updated
    now = datetime.now(timezone.utc)
    stale = (
        not hltv_ranking
        or hltv_last_updated is None
        or (now - hltv_last_updated).total_seconds() > HLTV_UPDATE_INTERVAL
    )
    if stale:
        hltv_ranking = await fetch_hltv(session)
        hltv_last_updated = now
        log.info("HLTV updated: %d teams", len(hltv_ranking))


async def fetch_markets(session):
    results = []

    # Method 1: tag_slug=cs2
    try:
        params = {"tag_slug": "cs2", "active": "true", "closed": "false", "limit": "100"}
        async with session.get(
            GAMMA_API + "/markets",
            params=params,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status == 200:
                data = await r.json()
                items = data if isinstance(data, list) else data.get("markets", [])
                results.extend(items)
                log.info("tag cs2: %d markets", len(items))
    except Exception as e:
        log.warning("tag cs2 failed: %s", e)

    # Method 2: tag_slug=counter-strike
    try:
        params = {"tag_slug": "counter-strike", "active": "true", "closed": "false", "limit": "100"}
        async with session.get(
            GAMMA_API + "/markets",
            params=params,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status == 200:
                data = await r.json()
                items = data if isinstance(data, list) else data.get("markets", [])
                results.extend(items)
                log.info("tag counter-strike: %d markets", len(items))
    except Exception as e:
        log.warning("tag counter-strike failed: %s", e)

    # Method 3: search by keyword
    for kw in ["counter-strike", "cs2"]:
        try:
            params = {"_c": kw, "active": "true", "limit": "50"}
            async with session.get(
                GAMMA_API + "/markets",
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    items = data if isinstance(data, list) else data.get("markets", [])
                    results.extend(items)
                    log.info("keyword %s: %d markets", kw, len(items))
        except Exception as e:
            log.warning("keyword %s failed: %s", kw, e)

    # Deduplicate
    seen = set()
    unique = []
    for m in results:
        mid = m.get("id")
        if mid and mid not in seen:
            seen.add(mid)
            unique.append(m)

    log.info("Total unique CS2 markets: %d", len(unique))
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
                    mid = str(m.get("id", ""))
                    if mid and mid not in known_market_ids:
                        if not is_first_run:
                            new_ones.append(m)
                        known_market_ids.add(mid)
                if is_first_run:
                    log.info("First run: %d markets loaded", len(markets))
                    is_first_run = False
                else:
                    log.info("Check done. Known: %d, New: %d", len(known_market_ids), len(new_ones))
                for market in new_ones:
                    text = new_market_text(market)
                    kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="Open market", url=market_url(market))
                    ]])
                    for chat_id in list(subscribers):
                        try:
                            await bot.send_message(
                                chat_id, text,
                                parse_mode="HTML",
                                reply_markup=kb,
                                disable_web_page_preview=True,
                            )
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
    src = "live" if hltv_ranking else "fallback"
    lines.append("\nSource: " + src)
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