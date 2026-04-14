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

# ─── Config ──────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "120")) # seconds
HLTV_UPDATE_INTERVAL = 3600 # refresh ranking every hour

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ─── State ───────────────────────────────────────────────────────────────────
subscribers: set[int] = set()
known_market_ids: set[str] = set()
is_first_run: bool = True

# HLTV ranking cache: {"team name lowercase": rank_int}
hltv_ranking: dict[str, int] = {}
hltv_last_updated: datetime | None = None

CS_KEYWORDS = [
 "counter-strike", "cs2", "cs:go", "csgo", "cs major",
 "major championship", "blast", "esl one", "iem", "pgl",
 "faceit major", "navi", "faze clan", "astralis", "vitality",
 "team liquid", "nip", "m0nesy", "zywoo", "electronic", "device",
 "b1t", "jeks", "hooxi", "donk", "sh1ro"
]

# ─── HLTV Ranking ─────────────────────────────────────────────────────────────

# Fallback hardcoded ranking (used when HLTV is unreachable)
FALLBACK_RANKING: dict[str, int] = {
 "vitality": 1,
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
 "9z": 20, "9z team": 20,
 "virtus.pro": 21, "vp": 21,
 "flyquest": 22,
 "monte": 23,
 "saw": 24,
 "apeks": 25,
}

async def fetch_hltv_ranking(session: aiohttp.ClientSession) -> dict[str, int]:
 """Parse HLTV world ranking. Falls back to hardcoded dict on any failure."""
 headers = {
 "User-Agent": (
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
 "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
 ),
 "Accept-Language": "en-US,en;q=0.9",
 "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
 "Referer": "https://www.google.com/",
 }
 try:
 async with session.get(
 "https://www.hltv.org/ranking/teams",
 headers=headers,
 timeout=aiohttp.ClientTimeout(total=15),
 allow_redirects=True,
 ) as r:
 if r.status != 200:
 log.warning(f"HLTV returned HTTP {r.status}, using fallback")
 return FALLBACK_RANKING.copy()

 html = await r.text()
 ranking: dict[str, int] = {}

 # Try structured data first
 json_match = re.search(r'"rankedTeams"\s*:\s*(\[.*?\])', html, re.DOTALL)
 if json_match:
 try:
 teams = json.loads(json_match.group(1))
 for t in teams:
 name = t.get("name", "").strip().lower()
 rank = int(t.get("rank", 0))
 if name and rank:
 ranking[name] = rank
 if ranking:
 log.info(f"HLTV: {len(ranking)} teams from JSON")
 return ranking
 except Exception:
 pass

 # HTML fallback
 rank_blocks = re.findall(
 r'<span class="position">#(\d+)</span>.*?<span class="name">(.*?)</span>',
 html, re.DOTALL
 )
 for rank_str, name_raw in rank_blocks:
 name = re.sub(r'<[^>]+>', '', name_raw).strip().lower()
 try:
 ranking[name] = int(rank_str)
 except ValueError:
 pass

 if ranking:
 log.info(f"HLTV: {len(ranking)} teams from HTML")
 return ranking

 log.warning("HLTV: could not parse, using fallback")
 return FALLBACK_RANKING.copy()

 except Exception as e:
 log.warning(f"HLTV fetch error: {e}, using fallback")
 return FALLBACK_RANKING.copy()

async def ensure_hltv_ranking(session: aiohttp.ClientSession):
 """Refresh HLTV ranking cache if stale (> 1 hour)."""
 global hltv_ranking, hltv_last_updated
 now = datetime.now(timezone.utc)
 if (
 not hltv_ranking
 or hltv_last_updated is None
 or (now - hltv_last_updated).total_seconds() > HLTV_UPDATE_INTERVAL
 ):
 log.info("Refreshing HLTV ranking...")
 hltv_ranking = await fetch_hltv_ranking(session)
 hltv_last_updated = now
 log.info(f"HLTV ranking ready: {len(hltv_ranking)} teams")

def get_team_rank(team_name: str) -> str:
 """Returns '#N' for a team or '' if not found."""
 if not team_name or not hltv_ranking:
 return ""
 name = team_name.strip().lower()
 if name in hltv_ranking:
 return f"#{hltv_ranking[name]}"
 for key, rank in hltv_ranking.items():
 if key in name or name in key:
 return f"#{rank}"
 return ""

def extract_teams_from_market(market: dict) -> list[str]:
 """
 Extract team names from market outcomes or question.
 Returns up to 2 names.
 """
 outcomes_raw = market.get("outcomes", "[]")
 try:
 outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_ra except Exception:
 outcomes = []

 teams = [
 str(o).strip() for o in outcomes
 if str(o).strip().lower() not in ("yes", "no", "draw", "other", "neither")
 ]
 if teams:
 return teams[:2]

 # Fallback: scan question for known team names
 question = market.get("question", "").lower()
 found = []
 for key in sorted(FALLBACK_RANKING.keys(), key=len, reverse=True):
 if key in question:
 found.append(key.title())
 if len(found) == 2:
 break
 return found

# ─── Polymarket API ───────────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"

def is_cs_market(market: dict) -> bool:
 text = " ".join([
 market.get("question", ""),
 market.get("description", ""),
 market.get("groupItemTitle", ""),
 " ".join(market.get("tags", []) if isinstance(market.get("tags"), list) else []),
 ]).lower()
 return any(kw in text for kw in CS_KEYWORDS)

def get_price(market: dict, outcome_index: int) -> str:
 try:
 prices = market.get("outcomePrices", "[]")
 if isinstance(prices, str):
 prices = json.loads(prices)
 p = float(prices[outcome_index])
 return f"{p * 100:.0f}¢"
 except Exception:
 return "—"

def format_volume(market: dict) -> str:
 try:
 v = float(market.get("volume", 0) or market.get("liquidityNum", 0) or 0)
 if v >= 1_000_000:
 return f"${v/1_000_000:.1f}M"
 if v >= 1_000:
 return f"${v/1_000:.1f}K"
 return f"${v:.0f}"
 except Exception:
 return "$—"

def market_url(market: dict) -> str:
 slug = market.get("slug") or market.get("id", "")
 return f"https://polymarket.com/event/{slug}"

async def fetch_cs_markets(session: aiohttp.ClientSession) -> list[dict]:
 results = []
 try:
 params = {"tag_slug": "esports", "active": "true", "closed": "false", "limit": "100"}
 async with session.get(
 f"{GAMMA_API}/markets", params=params,
 timeout=aiohttp.ClientTimeout(total=20)
 ) as r:
 if r.status == 200:
 data = await r.json()
 markets = data if isinstance(data, list) else data.get("markets", data.get("d results = [m for m in markets if is_cs_market(m)]
 log.info(f"Esports: {len(markets)} total, CS: {len(results)}")
 except Exception as e:
 log.warning(f"Esports fetch failed: {e}")

 if not results:
 for kw in ["counter-strike", "cs2", "cs major"]:
 try:
 params = {"_c": kw, "active": "true", "limit": "30"}
 async with session.get(
 f"{GAMMA_API}/markets", params=params,
 timeout=aiohttp.ClientTimeout(total=15)
 ) as r:
 if r.status == 200:
 data = await r.json()
 markets = data if isinstance(data, list) else data.get("markets", dat results.extend(markets)
 except Exception as e:
 log.warning(f"Keyword '{kw}' failed: {e}")
 seen: set[str] = set()
 unique = []
 for m in results:
 if m.get("id") not in seen:
 seen.add(m["id"])
 unique.append(m)
 results = unique

 return results

# ─── Message builders ─────────────────────────────────────────────────────────

def format_team_with_rank(team_name: str, price: str) -> str:
 """Returns e.g. 'Vitality #1 51¢' or 'Vitality 51¢' if rank unknown."""
 rank = get_team_rank(team_name)
 parts = [f"<b>{team_name}</b>"]
 if rank:
 parts.append(rank)
 parts.append(price)
 return " ".join(parts)

def build_matchup_line(market: dict) -> str:
 """
 Builds: Vitality #1 51¢ vs FaZe #3 49¢
 or falls back to YES/NO if no teams found.
 """
 teams = extract_teams_from_market(market)
 p0 = get_price(market, 0)
 p1 = get_price(market, 1)

 if len(teams) >= 2:
 return f"{format_team_with_rank(teams[0], p0)} vs {format_team_with_rank(teams[1],  elif len(teams) == 1:
 return f"{format_team_with_rank(teams[0], p0)} | NO {p1}"
 else:
 return f" YES <code>{p0}</code> NO <code>{p1}</code>"

def build_new_market_message(market: dict) -> str:
 question = market.get("question", "Без названия")
 volume = format_volume(market)
 end_raw = market.get("endDate", "")
 try:
 end_date = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).strftime("%d.%m.%Y" except Exception:
 end_date = "—"
 url = market_url(market)
 matchup = build_matchup_line(market)

 return (
 " <b>Новый CS рынок на Polymarket!</b>\n\n"
 f" {question}\n\n"
 f" {matchup}\n\n"
 f" Объём: <b>{volume}</b> До: <b>{end_date}</b>\n"
 f" <a href=\"{url}\">Открыть рынок</a>"
 )

def build_market_list_message(markets: list[dict]) -> str:
 if not markets:
 return " Активных CS рынков на Polymarket не найдено."
 lines = [" <b>Актуальные CS рынки на Polymarket:</b>\n"]
 for m in markets[:10]:
 question = m.get("question", "—")[:55]
 matchup = build_matchup_line(m)
 url = market_url(m)
 lines.append(f"• <a href=\"{url}\">{question}</a>\n {matchup}")
 if len(markets) > 10:
 lines.append(f"\n…и ещё {len(markets) - 10} рынков")
 lines.append(f"\n<i>Рейтинг: HLTV</i>")
 return "\n\n".join(lines)


# ─── Tracker loop ─────────────────────────────────────────────────────────────
async def tracker_loop():
 global is_first_run
 log.info("Tracker loop started")

 async with aiohttp.ClientSession() as session:
 await ensure_hltv_ranking(session)

 while True:
 try:
 await ensure_hltv_ranking(session)
 markets = await fetch_cs_markets(session)
 now = datetime.now().strftime("%H:%M:%S")
 new_markets = []
 for m in markets:
 mid = str(m.get("id", ""))
 if mid and mid not in known_market_ids:
 if not is_first_run:
 new_markets.append(m)
 known_market_ids.add(mid)

 if is_first_run:
 log.info(f"[{now}] First run: {len(markets)} CS markets loaded")
 is_first_run = False
 else:
 log.info(f"[{now}] Check done. Known: {len(known_market_ids)}, New: {len( 

if new_markets and subscribers:
 for market in new_markets:
 text = build_new_market_message(market)
 kb = InlineKeyboardMarkup(inline_keyboard=[[
 InlineKeyboardButton(text=" Открыть рынок", url=market_url(mark
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
 log.warning(f"Failed to send to {chat_id}: {e}")
 if "blocked" in str(e).lower() or "chat not found" in str(e). subscribers.discard(chat_id)

 except Exception as e:
 log.error(f"Tracker error: {e}")

 await asyncio.sleep(CHECK_INTERVAL)


# ─── Commands ─────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
 subscribers.add(message.chat.id)
 kb = InlineKeyboardMarkup(inline_keyboard=[
 [InlineKeyboardButton(text=" Текущие рынки", callback_data="list")],
 [InlineKeyboardButton(text=" Рейтинг HLTV", callback_data="ranking")],
 ])
 await message.answer(
 " <b>CS Polymarket Tracker</b>\n\n"
 "Слежу за новыми CS рынками на Polymarket.\n"
 "При появлении нового рынка пришлю уведомление "
 "с ценами и рейтингом команд по <b>HLTV</b>.\n\n"
 f" Проверка каждые <b>{CHECK_INTERVAL // 60} мин.</b>\n\n"
 "/list — актуальные рынки\n"
 "/ranking — топ-20 HLTV\n"
 "/status — статус бота\n"
 "/stop — отписаться",
 parse_mode="HTML",
 reply_markup=kb,
 )


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
 subscribers.discard(message.chat.id)
 await message.answer(" Отписан. /start — подписаться снова.")

@dp.message(Command("list"))
async def cmd_list(message: types.Message):
 msg = await message.answer(" Загружаю рынки...")
 async with aiohttp.ClientSession() as session:
 await ensure_hltv_ranking(session)
 markets = await fetch_cs_markets(session)
 await msg.edit_text(
 build_market_list_message(markets),
 parse_mode="HTML",
 disable_web_page_preview=True,
 )


@dp.message(Command("ranking"))
async def cmd_ranking(message: types.Message):
 if not hltv_ranking:
 await message.answer(" Рейтинг ещё не загружен, попробуй через минуту.")
 return
 top = sorted(hltv_ranking.items(), key=lambda x: x[1])[:20]
 lines = [" <b>HLTV Top-20:</b>\n"]
 for name, rank in top:
 lines.append(f"<code>#{rank:>2}</code> {name.title()}")
 updated = hltv_last_updated.strftime("%d.%m %H:%M") if hltv_last_updated else "—"
 lines.append(f"\n<i>Обновлено: {updated} UTC</i>")
 await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
 subbed = " Подписан" if message.chat.id in subscribers else " Не подписан"
 updated = hltv_last_updated.strftime("%H:%M UTC") if hltv_last_updated else "—"
 await message.answer(
 f" <b>Статус</b>\n\n"
 f"Ты: {subbed}\n"
 f"Известно рынков: <b>{len(known_market_ids)}</b>\n"
 f"Подписчиков: <b>{len(subscribers)}</b>\n"
 f"HLTV команд в кэше: <b>{len(hltv_ranking)}</b>\n"
 f"Рейтинг обновлён: <b>{updated}</b>\n"
 f"Интервал проверки: <b>{CHECK_INTERVAL} сек.</b>",
 parse_mode="HTML",
 )


@dp.callback_query(lambda c: c.data == "list")
async def cb_list(callback: types.CallbackQuery):
 await callback.answer("Загружаю...")
 async with aiohttp.ClientSession() as session:
 await ensure_hltv_ranking(session)
 markets = await fetch_cs_markets(session)
 await callback.message.answer(
 build_market_list_message(markets),
 parse_mode="HTML",
 disable_web_page_preview=True,
 )


@dp.callback_query(lambda c: c.data == "ranking")
async def cb_ranking(callback: types.CallbackQuery):
 await callback.answer()
 await cmd_ranking(callback.message)


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main():
 if not BOT_TOKEN:
 raise ValueError("BOT_TOKEN environment variable is not set!")
 log.info("Starting CS Polymarket Bot...")
 asyncio.create_task(tracker_loop())
 await dp.start_polling(bot)


if __name__ == "__main__":
 asyncio.run(main())