import asyncio
import math
import logging
import os
import re
from collections import defaultdict

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (InlineKeyboardMarkup, InlineKeyboardButton,
                           ReplyKeyboardMarkup, KeyboardButton)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATA_API = "https://data-api.polymarket.com"
PAGE = 500          # лимит API на страницу
MAX_PAGES = 40      # потолок пагинации (40×500 = 20000 записей)
PAUSE = 0.1         # пауза между страницами

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Кэш последнего разбора по чату (для кнопок углубления)
last_analysis: dict = {}

WALLET_RE = re.compile(r"0x[a-fA-F0-9]{40}")

# ── Справочник погодных городов Polymarket: координаты + часовой пояс ──────────
CITY_COORDS = {
    "tokyo":(35.69,139.69,9),"seoul":(37.57,126.98,9),"shanghai":(31.23,121.47,8),
    "shenzhen":(22.54,114.06,8),"guangzhou":(23.13,113.26,8),"hong kong":(22.30,114.17,8),
    "beijing":(39.90,116.41,8),"taipei":(25.03,121.57,8),"singapore":(1.35,103.82,8),
    "karachi":(24.86,67.00,5),"lucknow":(26.85,80.95,5),"delhi":(28.61,77.21,5),
    "mumbai":(19.08,72.88,5),"london":(51.51,-0.13,0),"paris":(48.86,2.35,1),
    "madrid":(40.42,-3.70,1),"milan":(45.46,9.19,1),"munich":(48.14,11.58,1),
    "moscow":(55.76,37.62,3),"istanbul":(41.01,28.98,3),"new york":(40.71,-74.01,-5),
    "houston":(29.76,-95.37,-6),"austin":(30.27,-97.74,-6),"los angeles":(34.05,-118.24,-8),
    "chicago":(41.88,-87.63,-6),"sao paulo":(-23.55,-46.63,-3),"panama city":(8.98,-79.52,-5),
    "dubai":(25.20,55.27,4),
}

def _city_from_title(title):
    t = (title or "").lower()
    for city in CITY_COORDS:
        if city in t:
            return city
    return None

def _parse_bucket(title):
    """Парсит температурный бакет из title. (lo, hi) в шкале title."""
    t = title or ""
    # Убираем дату YYYY-MM-DD, иначе '2026-06' ловится как диапазон температур
    t = re.sub(r"\d{4}-\d{2}-\d{2}", "", t)
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*(?:-|to|–|—)\s*(-?\d+(?:\.\d+)?)", t)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    m = re.search(r"(?:above|higher than|greater than|over|more than|≥|>=?)\s*(-?\d+(?:\.\d+)?)", t, re.I)
    if m:
        return (float(m.group(1)), 999.0)
    m = re.search(r"(?:below|lower than|less than|under|≤|<=?)\s*(-?\d+(?:\.\d+)?)", t, re.I)
    if m:
        return (-999.0, float(m.group(1)))
    m = re.search(r"\bbe\s+(-?\d+(?:\.\d+)?)\s*°?[CF]", t, re.I)
    if m:
        v = float(m.group(1))
        return (v - 0.5, v + 0.5)
    return None

def _is_fahrenheit(title):
    t = (title or "").lower()
    if "°f" in t or "fahrenheit" in t:
        return True
    for c in ["new york","houston","austin","los angeles","chicago"]:
        if c in t:
            return True
    return False

async def fetch_hourly_actual(session, lat, lon, date_str, fahrenheit):
    """Почасовая ФАКТИЧЕСКАЯ температура за день (Open-Meteo archive)."""
    unit = "fahrenheit" if fahrenheit else "celsius"
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {"latitude": lat, "longitude": lon, "start_date": date_str,
              "end_date": date_str, "hourly": "temperature_2m",
              "temperature_unit": unit, "timezone": "UTC"}
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            temps = data.get("hourly", {}).get("temperature_2m", [])
            times = data.get("hourly", {}).get("time", [])
            return list(zip(times, temps))
    except Exception as e:
        log.warning("open-meteo %s %s: %s", lat, date_str, e)
        return None



# ─── Загрузка данных с пагинацией ─────────────────────────────────────────────

async def fetch_all(session, endpoint, params, page_size, max_pages):
    """Качает все записи через offset-пагинацию. page_size зависит от
    endpoint: /positions и /trades = 500, /closed-positions = 50 (лимит API)."""
    out = []
    offset = 0
    for _ in range(max_pages):
        p = dict(params)
        p["limit"] = page_size
        p["offset"] = offset
        try:
            async with session.get(f"{DATA_API}/{endpoint}", params=p,
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    log.warning("%s status %s", endpoint, r.status)
                    break
                batch = await r.json()
        except Exception as e:
            log.warning("fetch %s offset=%s: %s", endpoint, offset, e)
            break
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
        await asyncio.sleep(PAUSE)
    return out


# ─── Помощники анализа ────────────────────────────────────────────────────────

def is_weather(title):
    t = (title or "").lower()
    return any(w in t for w in
               ["temperature", "°c", "°f", "hottest", "warmest", "rain",
                "snow", "weather", "degrees", "highest temp", "lowest temp"])


def city_of(title):
    """Достаёт город из заголовка погодного рынка."""
    t = title or ""
    m = re.search(r"\bin ([A-Z][a-zA-Z .'-]+?)(?: be| on| this| today| tomorrow|$)", t)
    if m:
        return m.group(1).strip()[:20]
    return "?"


def esc(s):
    """Экранирует HTML-спецсимволы (названия рынков могут содержать < > &)."""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def fmt_money(x):
    s = f"{x:+,.2f}"
    return s


def pct(part, whole):
    return f"{part/whole*100:.0f}%" if whole else "0%"


# ─── Главный разбор ───────────────────────────────────────────────────────────

def build_analysis(closed, active, trades):
    """Разбор стратегии. closed = закрытые (для P&L/винрейта),
    active = текущие (для 'в игре'), trades = действия."""
    name = "?"
    for src in (trades, closed, active):
        for t in src:
            if t.get("name"):
                name = t["name"]; break
        if name != "?":
            break

    # Погодный? — по всем позициям
    allpos = closed + active
    weather_share = 0
    if allpos:
        w = sum(1 for p in allpos if is_weather(p.get("title")))
        weather_share = w / len(allpos)
    is_weather_trader = weather_share >= 0.5

    L = [f"📊 {esc(name)}"]
    if is_weather_trader:
        L.append(f"🌤 Погодный трейдер ({weather_share*100:.0f}% позиций)")
    L.append("")

    # ── P&L = реализованный по ЗАКРЫТЫМ позициям ──
    nc = len(closed)
    realized = sum(float(p.get("realizedPnl", 0) or 0) for p in closed)
    # Винрейт по закрытым: выиграл = realizedPnl > 0
    wins = [p for p in closed if float(p.get("realizedPnl", 0) or 0) > 0]
    losses = [p for p in closed if float(p.get("realizedPnl", 0) or 0) < 0]
    invested = sum(float(p.get("totalBought", 0) or 0) for p in closed)

    L.append(f"💰 Реализованный P&L: ${fmt_money(realized)}")
    if invested > 0:
        L.append(f"   ROI: {realized/invested*100:+.0f}% (вложено ${invested:,.0f})")
    if nc:
        L.append(f"📈 Винрейт: {len(wins)}/{nc} ({pct(len(wins), nc)}) по закрытым")
    L.append(f"   ✅ {len(wins)} побед / ❌ {len(losses)} проигрышей")

    # Активные позиции — отдельно, НЕ в итог
    if active:
        na = len(active)
        unreal = sum(float(p.get("cashPnl", 0) or 0) for p in active)
        cur_val = sum(float(p.get("currentValue", 0) or 0) for p in active)
        L.append(f"🎲 В игре: {na} позиций, ${cur_val:,.0f} "
                 f"(бумажный {fmt_money(unreal)})")
    L.append("")

    # ── Зоны входа (по закрытым — полная картина) ──
    prices = [float(p.get("avgPrice", 0) or 0) for p in closed if p.get("avgPrice")]
    if prices:
        med = sorted(prices)[len(prices)//2]
        zones = {"1-5¢":0,"5-15¢":0,"15-35¢":0,"35-50¢":0,"50-65¢":0,"65¢+":0}
        for pr in prices:
            if pr < 0.05: zones["1-5¢"] += 1
            elif pr < 0.15: zones["5-15¢"] += 1
            elif pr < 0.35: zones["15-35¢"] += 1
            elif pr < 0.50: zones["35-50¢"] += 1
            elif pr < 0.65: zones["50-65¢"] += 1
            else: zones["65¢+"] += 1
        top_zone = max(zones.items(), key=lambda kv: kv[1])
        L.append(f"🎯 Вход: медиана {med*100:.0f}¢, "
                 f"чаще {top_zone[0]} ({pct(top_zone[1], len(prices))})")
        zline = " ".join(f"{z}:{c}" for z,c in zones.items() if c)
        L.append(f"   {zline}")

        # Винрейт ПО ЗОНАМ — где трейдер реально зарабатывает
        zone_stats = {}
        for p in closed:
            pr = float(p.get("avgPrice",0) or 0)
            rp = float(p.get("realizedPnl",0) or 0)
            if pr < 0.15: z="дешёвые до 15¢"
            elif pr < 0.50: z="средние 15-50¢"
            else: z="дорогие 50¢+"
            if z not in zone_stats: zone_stats[z]={"n":0,"w":0,"pnl":0.0}
            zone_stats[z]["n"]+=1
            zone_stats[z]["pnl"]+=rp
            if rp>0: zone_stats[z]["w"]+=1
        L.append("   P&L по зонам входа:")
        for z in ["дешёвые до 15¢","средние 15-50¢","дорогие 50¢+"]:
            if z in zone_stats:
                s=zone_stats[z]
                L.append(f"     {z}: {s['w']}/{s['n']} ${fmt_money(s['pnl'])}")

    # ── YES/NO (по закрытым) ──
    yes = sum(1 for p in closed if p.get("outcome") == "Yes")
    no = nc - yes
    if nc:
        side = "покупает YES" if yes > no*1.5 else ("покупает NO" if no > yes*1.5 else "YES и NO поровну")
        L.append(f"⚖️ {side}: YES {yes} / NO {no}")

    # ── Размер ставки ──
    sizes = [float(p.get("totalBought", 0) or 0) for p in closed if p.get("totalBought")]
    if sizes:
        med_s = sorted(sizes)[len(sizes)//2]
        L.append(f"📏 Ставка: медиана ${med_s:.0f} "
                 f"(${min(sizes):.0f}–${max(sizes):.0f})")
    L.append("")

    # ── Держит или торгует ──
    if trades:
        buys = sum(1 for t in trades if t.get("side") == "BUY")
        sells = sum(1 for t in trades if t.get("side") == "SELL")
        ts = [t.get("timestamp", 0) for t in trades if t.get("timestamp")]
        L.append(f"🔄 Действий загружено: {len(trades)} (BUY {buys} / SELL {sells})")
        if sells < buys * 0.3:
            L.append("   → ДЕРЖИТ до резолюции (почти не продаёт)")
        elif sells > buys * 0.7:
            L.append("   → активно торгует выходами")
        if ts:
            span = (max(ts)-min(ts))/86400
            if span >= 1:
                L.append(f"   {span:.0f} дней наблюдения")

    return "\n".join(L), is_weather_trader


def build_cities(closed):
    """Разбор по городам (для погодных трейдеров) — по закрытым позициям."""
    by_city = defaultdict(lambda: {"n":0, "pnl":0.0, "win":0})
    for p in closed:
        if not is_weather(p.get("title")):
            continue
        c = city_of(p.get("title"))
        d = by_city[c]; d["n"] += 1
        rp = float(p.get("realizedPnl", 0) or 0)
        d["pnl"] += rp
        if rp > 0: d["win"] += 1
    if not by_city:
        return "Нет закрытых погодных позиций для разбора по городам."
    items = sorted(by_city.items(), key=lambda kv: -kv[1]["pnl"])
    L = ["🏙 По городам (реализованный P&L):", ""]
    for c, d in items[:25]:
        if d["n"] < 1: continue
        mark = "🟢" if d["pnl"] > 0 else "🔴"
        L.append(f"{mark} {esc(c)}: {d['win']}/{d['n']} ${fmt_money(d['pnl'])}")
    return "\n".join(L)


def build_recent(trades, limit=15):
    """Последние сделки."""
    L = ["🕐 Последние сделки:", ""]
    for t in trades[:limit]:
        side = t.get("side", "?")
        price = float(t.get("price", 0) or 0)
        title = esc((t.get("title", "?") or "?")[:35])
        out = t.get("outcome", "")
        emoji = "🟢" if side == "BUY" else "🔴"
        L.append(f"{emoji} {side} {out} @ {price*100:.0f}¢ — {title}")
    return "\n".join(L)


def analysis_keyboard(is_weather_trader):
    rows = []
    if is_weather_trader:
        rows.append([InlineKeyboardButton(text="🏙 По городам", callback_data="a:cities")])
    rows.append([InlineKeyboardButton(text="🕐 Последние сделки", callback_data="a:recent")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─── Обработчик: разбор кошелька ──────────────────────────────────────────────

async def run_analysis(message, wallet):
    msg = await message.answer(f"⏳ Качаю историю {wallet[:12]}…")
    try:
        async with aiohttp.ClientSession() as session:
            # Закрытые позиции — для P&L и винрейта. Берём ВЫБОРКУ до ~2500
            # (50 страниц × 50): для статистики винрейта/зон этого достаточно,
            # тянуть все 9000 слишком долго (Railway/Telegram оборвут).
            closed = await fetch_all(session, "closed-positions",
                                     {"user": wallet, "sortBy": "TIMESTAMP"},
                                     page_size=50, max_pages=50)
            await msg.edit_text(f"⏳ Закрытых: {len(closed)}. Активные…")
            active = await fetch_all(session, "positions",
                                     {"user": wallet, "sizeThreshold": 0},
                                     page_size=500, max_pages=10)
            await msg.edit_text(f"⏳ Закрытых {len(closed)}, активных {len(active)}. "
                                f"Сделки…")
            trades = await fetch_all(session, "trades", {"user": wallet},
                                     page_size=500, max_pages=4)

        if not closed and not active and not trades:
            await msg.edit_text("❌ Ничего не нашёл. Проверь адрес кошелька.")
            return

        text, is_w = build_analysis(closed, active, trades)
        last_analysis[message.chat.id] = {
            "closed": closed, "active": active, "trades": trades, "wallet": wallet}
        # edit_text вместо delete+answer — если упадёт, юзер увидит хоть что-то.
        # Кнопки отдельным сообщением.
        if len(text) > 4000:
            text = text[:4000]
        await msg.edit_text(text,
                            disable_web_page_preview=True)
        await message.answer("Подробнее:", reply_markup=analysis_keyboard(is_w))
    except Exception as e:
        log.exception("run_analysis failed")
        err = f"❌ Ошибка при разборе: {type(e).__name__}: {e}\nПопробуй ещё раз."
        try:
            # без parse_mode — ошибка может содержать спецсимволы
            await msg.edit_text(err)
        except Exception:
            await message.answer(err)


async def build_weather_check(session, activity, limit=10):
    """ПИЛОТ: сверяет входы трейдера с ФАКТИЧЕСКОЙ погодой на момент входа.
    Гипотеза: они входят, когда дневной максимум УЖЕ виден (вход на факт).
    Для каждой погодной сделки:
      - парсит город, дату, бакет, время входа
      - тянет почасовой факт того дня (Open-Meteo)
      - считает фактический максимум ДО часа входа (по локальному времени)
      - сравнивает с бакетом: максимум уже В бакете / НИЖЕ / ВЫШЕ
    """
    trades = [a for a in activity if a.get("type") == "TRADE"
              and a.get("side") == "BUY"]
    # Группируем BUY по рынку+стороне; берём ОСНОВНОЙ вход (с макс usdcSize,
    # не первую утреннюю пробу — это чинит баг с входом в 8 утра).
    from collections import defaultdict
    pos_trades = defaultdict(list)
    for t in trades:
        title = t.get("title", "")
        if not (_city_from_title(title) and _parse_bucket(title)
                and _parse_resolution_ts(title)):
            continue
        pos_trades[(t.get("conditionId"), t.get("outcome"))].append(t)

    weather_positions = []
    for (cid, outcome), tl in pos_trades.items():
        # основной вход = сделка с наибольшим usdcSize (туда вложил больше всего)
        main_t = max(tl, key=lambda x: float(x.get("usdcSize", 0) or 0))
        weather_positions.append((main_t, outcome, len(tl)))
        if len(weather_positions) >= limit:
            break

    if not weather_positions:
        return ("Не нашёл погодных сделок с распознаваемым городом/бакетом/датой.\n"
                "Возможно, у этого трейдера другой формат title.")

    L = [f"🌡 СВЕРКА ВХОДОВ С ФАКТОМ (пилот, {len(weather_positions)} позиций)", ""]
    L.append("Гипотеза: входят, когда исход УЖЕ ясен по факту погоды.")
    L.append("(YES — факт уже в бакете; NO — факт уже вне бакета)")
    L.append("")

    on_fact = 0      # вход когда исход УЖЕ подтверждён фактом (с учётом стороны)
    against = 0      # вход против факта (рискованная ставка)
    unclear = 0      # факт ещё не определил исход (макс не наступил)
    checked = 0

    for main_t, outcome, n_entries in weather_positions:
        title = main_t.get("title", "")
        city = _city_from_title(title)
        lat, lon, tz = CITY_COORDS[city]
        bucket = _parse_bucket(title)
        fahr = _is_fahrenheit(title)
        entry_ts = main_t.get("timestamp", 0)
        res_ts = _parse_resolution_ts(title)
        import datetime
        res_date = datetime.datetime.fromtimestamp(res_ts, datetime.timezone.utc)
        date_str = res_date.strftime("%Y-%m-%d")

        hourly = await fetch_hourly_actual(session, lat, lon, date_str, fahr)
        await asyncio.sleep(0.2)
        if not hourly:
            continue
        checked += 1

        entry_dt_utc = datetime.datetime.fromtimestamp(entry_ts, datetime.timezone.utc)
        entry_local_hour = (entry_dt_utc.hour + tz) % 24

        # Факт. максимум С НАЧАЛА ДНЯ ДО часа входа + ПОЛНЫЙ дневной максимум.
        max_so_far = None
        day_max = None
        for tstr, temp in hourly:
            if temp is None:
                continue
            h_utc = int(tstr[11:13])
            h_local = (h_utc + tz) % 24
            if day_max is None or temp > day_max:
                day_max = temp
            if h_local <= entry_local_hour:
                if max_so_far is None or temp > max_so_far:
                    max_so_far = temp
        if max_so_far is None:
            max_so_far = hourly[0][1]

        lo, hi = bucket
        is_yes = (outcome == "Yes")
        # Накрыл ли факт-максимум-на-входе бакет?
        in_bucket = (lo <= max_so_far <= hi)
        above_bucket = (max_so_far > hi)
        # Может ли максимум ЕЩЁ вырасти в бакет? (день не закончился)
        can_still_rise = (max_so_far < hi)

        # ЛОГИКА С УЧЁТОМ СТОРОНЫ:
        if is_yes:
            # YES бакета: ставит ЧТО максимум будет в бакете.
            # "на факт" = факт УЖЕ в бакете (подтверждён).
            if in_bucket:
                verdict = "✅ YES: факт уже в бакете"
                on_fact += 1
            elif above_bucket:
                verdict = "❌ YES: факт уже ВЫШЕ (проигран)"
                against += 1
            else:
                verdict = "⬆️ YES: факт ниже, ждёт роста"
                unclear += 1
        else:
            # NO бакета: ставит ЧТО максимум НЕ в бакете.
            # "на факт" = факт УЖЕ вне бакета (выше — бакет точно проигран → NO выигр).
            if above_bucket:
                verdict = "✅ NO: факт уже выше бакета (NO подтверждён)"
                on_fact += 1
            elif in_bucket:
                verdict = "❌ NO: факт в бакете (NO под угрозой)"
                against += 1
            else:
                verdict = "⬇️ NO: факт ниже, бакет ещё возможен"
                unclear += 1

        unit = "F" if fahr else "C"
        side_s = "YES" if is_yes else "NO"
        L.append(f"{city[:11]} {date_str[5:]} {side_s} вх{entry_local_hour}ч: "
                 f"факт-макс {max_so_far:.0f}°{unit} "
                 f"(день {day_max:.0f}°), бакет {lo:.0f}-{hi:.0f} → {verdict}")

    L.append("")
    if checked:
        L.append(f"ИТОГ ({checked} проверено) — вход относительно факта:")
        L.append(f"  ✅ исход уже подтверждён фактом: {on_fact} ({on_fact/checked*100:.0f}%)")
        L.append(f"  ❌ вход против факта: {against}")
        L.append(f"  ◐ факт ещё не определил исход: {unclear}")
        L.append("")
        if on_fact / checked > 0.5:
            L.append("→ ГИПОТЕЗА ПОДТВЕРЖДАЕТСЯ: входят когда исход уже виден!")
            L.append("  Edge — поздний вход на свершившийся факт, не прогноз.")
        elif (on_fact + unclear) / checked > 0.7 and on_fact > against:
            L.append("→ ЧАСТИЧНО: чаще входят на факт, чем против.")
        else:
            L.append("→ Гипотеза слабая на этой выборке.")
    L.append("")
    L.append("⚠️ Пилот, малая выборка. Open-Meteo ≈ станция (±1-2°).")
    L.append("Часовой пояс без лета. Учтена сторона YES/NO. Для тренда.")
    return "\n".join(L)


def build_inputs(activity):
    """МАТРИЦА ВХОДОВ: связывает цену и время входа с ИСХОДОМ (выиграл/нет).
    Отвечает: на какой цене входа и в какое время трейдер выигрывает чаще.
    Для сравнения с нашими входами (медиана ~2¢, за 33ч до резолюции)."""
    from collections import defaultdict
    import datetime
    trades = [a for a in activity if a.get("type") == "TRADE"]
    redeems = [a for a in activity if a.get("type") == "REDEEM"]
    if not trades:
        return "Нет сделок для анализа входов."

    # conditionId, по которым был REDEEM = выигрыш (погасил по $1)
    redeemed_cids = set(a.get("conditionId") for a in redeems)

    # Группируем BUY по рынку+стороне
    by_pos = defaultdict(list)
    for t in trades:
        if t.get("side") == "BUY":
            key = (t.get("conditionId"), t.get("outcome"))
            by_pos[key].append(t)

    # Для каждой позиции: средняя цена входа, выиграл ли, час входа
    positions = []
    for (cid, outcome), buys in by_pos.items():
        prices = [float(b.get("price",0) or 0) for b in buys if b.get("price")]
        if not prices:
            continue
        avg_entry = sum(prices)/len(prices)
        # Выигрыш: был REDEEM по рынку. (грубо, но REDEEM = погашение $1)
        won = cid in redeemed_cids
        # Час первого входа (UTC)
        ts = min(b.get("timestamp",0) for b in buys if b.get("timestamp"))
        hour = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).hour if ts else -1
        positions.append({"entry": avg_entry, "won": won, "hour": hour,
                          "title": buys[0].get("title","")})

    if not positions:
        return "Не удалось разобрать входы."

    n = len(positions)
    L = ["📥 МАТРИЦА ВХОДОВ (цена/время → исход)", ""]
    L.append(f"Позиций: {n}, из них выиграло (REDEEM): "
             f"{sum(1 for p in positions if p['won'])}")
    L.append("")

    # ── ВИНРЕЙТ ПО ЗОНЕ ЦЕНЫ ВХОДА (главное!) ──
    zones = [(0,0.03,"1-3¢"),(0.03,0.07,"3-7¢"),(0.07,0.15,"7-15¢"),
             (0.15,0.35,"15-35¢"),(0.35,0.65,"35-65¢"),(0.65,1.01,"65¢+")]
    L.append("Винрейт по ЦЕНЕ входа (где edge):")
    for lo, hi, lbl in zones:
        sub = [p for p in positions if lo <= p["entry"] < hi]
        if not sub:
            continue
        w = sum(1 for p in sub if p["won"])
        L.append(f"  {lbl}: {w}/{len(sub)} ({w/len(sub)*100:.0f}% выиграло)")
    L.append("")

    # ── РАСПРЕДЕЛЕНИЕ входов по цене ──
    cheap = sum(1 for p in positions if p["entry"] < 0.03)
    med_entry = sorted([p["entry"] for p in positions])[n//2]
    L.append(f"Медиана входа: {med_entry*100:.0f}¢")
    L.append(f"  Очень дешёвых (<3¢): {cheap} ({cheap/n*100:.0f}%)")
    L.append("")

    # ── ВИНРЕЙТ ПО ЧАСУ ВХОДА (рано/поздно) ──
    L.append("Винрейт по часу входа (UTC):")
    by_hour = defaultdict(lambda: {"n":0,"w":0})
    for p in positions:
        if p["hour"] < 0: continue
        # группируем в блоки по 6 часов
        block = (p["hour"]//6)*6
        by_hour[block]["n"] += 1
        if p["won"]: by_hour[block]["w"] += 1
    for block in sorted(by_hour):
        d = by_hour[block]
        L.append(f"  {block:02d}-{block+6:02d}ч: {d['w']}/{d['n']} "
                 f"({d['w']/d['n']*100:.0f}%)")

    L.append("")
    L.append("→ Сравни с нашими: медиана входа ~2¢, вход за ~33ч.")
    L.append("Если у них дороже (7-17¢) выигрывает чаще — мы лезем")
    L.append("в слишком дешёвые хвосты.")
    return "\n".join(L)


def _parse_resolution_ts(title):
    """Парсит дату резолюции из title погодного рынка. Формат обычно:
    '...temperature in City on June 19' или '...on 2026-06-19'.
    Возвращает timestamp конца того дня (UTC) или None."""
    import datetime, re
    t = title or ""
    # Формат 2026-06-19
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime.datetime(y, mo, d, 23, 59, tzinfo=datetime.timezone.utc).timestamp()
        except Exception:
            return None
    # Формат 'June 19' / 'Jun 19'
    months = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,
              "aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
    m = re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+(\d{1,2})\b",
                  t.lower())
    if m:
        mo = months[m.group(1)]
        d = int(m.group(2))
        # год берём текущий (2026); если месяц явно прошлый — ок, приблизительно
        y = 2026
        try:
            return datetime.datetime(y, mo, d, 23, 59, tzinfo=datetime.timezone.utc).timestamp()
        except Exception:
            return None
    return None


def build_size_time(activity):
    """РАЗМЕР и ВРЕМЯ: связь размера ставки с исходом (ставят ли крупнее
    на то, что играет?), время удержания, за сколько часов до резолюции
    входят. Отвечает на пункт 2 (размер как параметр отбора) и проверяет
    наш вывод про поздний вход."""
    from collections import defaultdict
    trades = [a for a in activity if a.get("type") == "TRADE"]
    redeems = [a for a in activity if a.get("type") == "REDEEM"]
    if not trades:
        return "Нет сделок для анализа."
    redeemed_cids = set(a.get("conditionId") for a in redeems)

    # Позиции: группируем BUY по рынку+стороне
    by_pos = defaultdict(list)
    for t in trades:
        if t.get("side") == "BUY":
            by_pos[(t.get("conditionId"), t.get("outcome"))].append(t)

    L = ["💰 РАЗМЕР и ВРЕМЯ", ""]

    # ── 1. РАЗМЕР СТАВКИ ↔ ИСХОД (главное — параметр отбора?) ──
    # Для каждой позиции: суммарный вложенный размер + выиграла ли.
    won_sizes, lost_sizes = [], []
    for (cid, outcome), buys in by_pos.items():
        total_usd = sum(float(b.get("usdcSize", 0) or 0) for b in buys)
        if total_usd <= 0:
            continue
        if cid in redeemed_cids:
            won_sizes.append(total_usd)
        else:
            lost_sizes.append(total_usd)
    if won_sizes and lost_sizes:
        won_med = sorted(won_sizes)[len(won_sizes)//2]
        lost_med = sorted(lost_sizes)[len(lost_sizes)//2]
        won_avg = sum(won_sizes)/len(won_sizes)
        lost_avg = sum(lost_sizes)/len(lost_sizes)
        L.append("Размер ставки ↔ исход (ставят крупнее на то, что играет?):")
        L.append(f"  ВЫИГРАВШИЕ позиции: медиана ${won_med:.0f}, средн ${won_avg:.0f}")
        L.append(f"  ПРОИГРАВШИЕ: медиана ${lost_med:.0f}, средн ${lost_avg:.0f}")
        if won_med > lost_med * 1.3:
            L.append("  → ДА: на выигравшие ставили ЗАМЕТНО крупнее!")
            L.append("    Это параметр отбора — они ЗНАЮТ, на что ставить больше.")
        elif lost_med > won_med * 1.3:
            L.append("  → НЕТ, наоборот: крупнее на проигравшие (или усреднение вниз).")
        else:
            L.append("  → Размер примерно одинаков — НЕ параметр отбора.")
        L.append("")

    # ── 2. ВРЕМЯ УДЕРЖАНИЯ (BUY → SELL/REDEEM) ──
    # Первый BUY по рынку → первый REDEEM/последний SELL.
    sells_by_cid = defaultdict(list)
    for a in activity:
        if a.get("type") == "REDEEM":
            sells_by_cid[a.get("conditionId")].append(("REDEEM", a.get("timestamp", 0)))
        elif a.get("type") == "TRADE" and a.get("side") == "SELL":
            sells_by_cid[a.get("conditionId")].append(("SELL", a.get("timestamp", 0)))
    hold_hours = []
    for (cid, outcome), buys in by_pos.items():
        first_buy = min((b.get("timestamp", 0) for b in buys if b.get("timestamp")),
                        default=0)
        exits = sells_by_cid.get(cid, [])
        if first_buy and exits:
            exit_ts = min(ts for _, ts in exits if ts > first_buy) if any(ts > first_buy for _, ts in exits) else 0
            if exit_ts:
                hold_hours.append((exit_ts - first_buy) / 3600)
    if hold_hours:
        hold_med = sorted(hold_hours)[len(hold_hours)//2]
        L.append(f"Время удержания (вход → выход): медиана {hold_med:.0f}ч")
        fast = sum(1 for h in hold_hours if h < 1)
        slow = sum(1 for h in hold_hours if h >= 24)
        L.append(f"  быстрых (<1ч, ловля всплеска): {fast} │ "
                 f"долгих (≥24ч, держание): {slow}")
        L.append("")

    # ── 3. ВРЕМЯ ДО РЕЗОЛЮЦИИ при входе (поздно ли входят?) ──
    hours_before_res = []
    for (cid, outcome), buys in by_pos.items():
        first_buy = min((b.get("timestamp", 0) for b in buys if b.get("timestamp")),
                        default=0)
        res_ts = _parse_resolution_ts(buys[0].get("title", ""))
        if first_buy and res_ts and res_ts > first_buy:
            hours_before_res.append((res_ts - first_buy) / 3600)
    if hours_before_res:
        hb_med = sorted(hours_before_res)[len(hours_before_res)//2]
        L.append(f"Вход за СКОЛЬКО часов до резолюции: медиана {hb_med:.0f}ч")
        late = sum(1 for h in hours_before_res if h < 12)
        early = sum(1 for h in hours_before_res if h >= 24)
        L.append(f"  поздних (<12ч): {late} │ ранних (≥24ч): {early}")
        L.append(f"  → Сравни с нами: мы входим за ~33ч (рано).")
        if hb_med < 24:
            L.append(f"    Они входят ПОЗЖЕ нас (медиана {hb_med:.0f}ч). Подтверждает.")
    else:
        L.append("Время до резолюции: не удалось распарсить даты из title.")

    return "\n".join(L)


def build_start_impact(activity):
    """СТАРТ и ВЛИЯНИЕ: с чего начинал кошелёк (первые сделки, был ли
    капитал), как менялся размер во времени, двигал ли цену своими
    залпами (намёк на 'сам создаю всплеск')."""
    from collections import defaultdict
    import datetime
    trades = [a for a in activity if a.get("type") == "TRADE"
              and a.get("side") == "BUY"]
    if not trades:
        return "Нет покупок для анализа."
    # Сортируем по времени
    trades_sorted = sorted(trades, key=lambda x: x.get("timestamp", 0))

    L = ["🚀 СТАРТ и ВЛИЯНИЕ НА ЦЕНУ", ""]

    # ── 1. ПЕРВЫЕ СДЕЛКИ (с чего начал) ──
    L.append("Первые сделки в доступной истории:")
    for t in trades_sorted[:6]:
        ts = t.get("timestamp", 0)
        try:
            dt = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%d.%m %H:%M")
        except Exception:
            dt = "?"
        price = float(t.get("price", 0) or 0)
        usd = float(t.get("usdcSize", 0) or 0)
        L.append(f"  {dt}: {price*100:.0f}¢ ${usd:.0f} ({t.get('outcome','')})")
    first_usd = float(trades_sorted[0].get("usdcSize", 0) or 0)
    if first_usd < 5:
        L.append("  → начинал с МИКРО-ставок (как мы, без большого капитала)")
    elif first_usd > 100:
        L.append("  → начинал СРАЗУ КРУПНО (был капитал на старте)")
    L.append("⚠️ API отдаёт последние ~10000 сделок — у старых кошельков")
    L.append("   самое начало истории может быть НЕ видно.")
    L.append("")

    # ── 2. ЭВОЛЮЦИЯ РАЗМЕРА (первая треть vs последняя) ──
    n = len(trades_sorted)
    if n >= 30:
        third = n // 3
        early = [float(t.get("usdcSize", 0) or 0) for t in trades_sorted[:third]]
        late = [float(t.get("usdcSize", 0) or 0) for t in trades_sorted[-third:]]
        early_med = sorted(early)[len(early)//2]
        late_med = sorted(late)[len(late)//2]
        L.append(f"Эволюция размера ставки:")
        L.append(f"  ранние сделки: медиана ${early_med:.0f}")
        L.append(f"  поздние сделки: медиана ${late_med:.0f}")
        if late_med > early_med * 1.5:
            L.append("  → РОС размер (наращивал по мере роста капитала/уверенности)")
        elif early_med > late_med * 1.5:
            L.append("  → УМЕНЬШАЛ размер")
        else:
            L.append("  → размер стабилен")
        L.append("")

    # ── 3. ДВИГАЛ ЛИ ЦЕНУ (рост внутри залпа на одном рынке) ──
    # Группируем по рынку, смотрим залпы (много покупок за короткое время),
    # сравниваем цену первой и последней покупки в залпе.
    by_market = defaultdict(list)
    for t in trades_sorted:
        by_market[t.get("conditionId")].append(t)
    moved_up = 0
    moved_examples = []
    for cid, tl in by_market.items():
        if len(tl) < 3:
            continue
        tl_sorted = sorted(tl, key=lambda x: x.get("timestamp", 0))
        first_p = float(tl_sorted[0].get("price", 0) or 0)
        last_p = float(tl_sorted[-1].get("price", 0) or 0)
        first_ts = tl_sorted[0].get("timestamp", 0)
        last_ts = tl_sorted[-1].get("timestamp", 0)
        span_min = (last_ts - first_ts) / 60 if last_ts > first_ts else 0
        # Залп = много покупок за <60 мин, цена выросла
        if span_min < 60 and last_p > first_p * 1.5 and first_p > 0:
            moved_up += 1
            if len(moved_examples) < 3:
                title = (tl_sorted[0].get("title", "?") or "?")[:30]
                moved_examples.append(
                    f"  {title}: {first_p*100:.0f}¢→{last_p*100:.0f}¢ "
                    f"за {span_min:.0f}мин ({len(tl)} покупок)")
    L.append(f"Цена росла ВНУТРИ его залпа (намёк на 'сам двигал'):")
    L.append(f"  таких рынков: {moved_up}")
    for ex in moved_examples:
        L.append(ex)
    L.append("⚠️ Это НЕ доказательство манипуляции — цену мог двигать")
    L.append("   и рынок сам. Видим только ЕГО сделки, не весь стакан.")

    return "\n".join(L)


def build_deep(activity):
    """ГЛУБОКИЙ посделочный разбор: группирует сырые сделки по рынку,
    восстанавливает траекторию входов (когда, по какой цене, докупки),
    выявляет РЕАЛЬНУЮ механику (усреднение, докупки в плюс/минус, тайминг)."""
    from collections import defaultdict
    trades = [a for a in activity if a.get("type") == "TRADE"]
    if not trades:
        return "Нет сделок типа TRADE для глубокого разбора."

    # Группируем по рынку (conditionId)
    by_market = defaultdict(list)
    for t in trades:
        by_market[t.get("conditionId")].append(t)

    redeems = [a for a in activity if a.get("type") == "REDEEM"]
    merges = [a for a in activity if a.get("type") == "MERGE"]
    splits = [a for a in activity if a.get("type") == "SPLIT"]

    L = ["🔬 ГЛУБОКИЙ РАЗБОР (посделочно)", ""]
    L.append(f"Всего действий: {len(activity)}")
    L.append(f"  TRADE {len(trades)} │ REDEEM {len(redeems)} │ "
             f"MERGE {len(merges)} │ SPLIT {len(splits)}")
    L.append(f"Уникальных рынков: {len(by_market)}")
    L.append("")

    # ── Паттерн докупок: сколько входов на рынок в среднем ──
    entries_per_market = [len([t for t in tl if t.get("side")=="BUY"])
                          for tl in by_market.values()]
    avg_entries = sum(entries_per_market)/len(entries_per_market) if entries_per_market else 0
    multi = sum(1 for e in entries_per_market if e > 1)
    L.append(f"Входов (BUY) на рынок: среднее {avg_entries:.1f}")
    L.append(f"  Докупал (>1 входа): {multi}/{len(by_market)} рынков "
             f"({multi/len(by_market)*100:.0f}%)")

    # ── Усреднение: докупал по более низкой или высокой цене? ──
    avg_down = avg_up = same = 0
    for tl in by_market.values():
        buys = sorted([t for t in tl if t.get("side")=="BUY"],
                      key=lambda x: x.get("timestamp",0))
        if len(buys) < 2:
            continue
        first_p = float(buys[0].get("price",0) or 0)
        last_p = float(buys[-1].get("price",0) or 0)
        if last_p < first_p - 0.02: avg_down += 1
        elif last_p > first_p + 0.02: avg_up += 1
        else: same += 1
    L.append(f"  Из докупавших: усреднял ВНИЗ {avg_down}, "
             f"ВВЕРХ {avg_up}, ровно {same}")
    L.append("")

    # ── Тайминг: BUY/SELL и держит ли ──
    buys = [t for t in trades if t.get("side")=="BUY"]
    sells = [t for t in trades if t.get("side")=="SELL"]
    L.append(f"BUY {len(buys)} / SELL {len(sells)}")
    if len(sells) < len(buys)*0.2:
        L.append("  → почти НЕ продаёт, держит до REDEEM")
    L.append(f"REDEEM (погашений): {len(redeems)} — забирал выигрыш")
    if merges:
        L.append(f"⚠️ MERGE {len(merges)} — склеивал YES+NO пары (арбитраж/хедж)")
    if splits:
        L.append(f"⚠️ SPLIT {len(splits)} — дробил $1 на YES+NO")
    L.append("")

    # ── Цена входа по СЫРЫМ сделкам (не усреднённая!) ──
    buy_prices = [float(t.get("price",0) or 0) for t in buys if t.get("price")]
    if buy_prices:
        med = sorted(buy_prices)[len(buy_prices)//2]
        zones = {"1-5¢":0,"5-15¢":0,"15-35¢":0,"35-50¢":0,"50-65¢":0,"65¢+":0}
        for pr in buy_prices:
            if pr < 0.05: zones["1-5¢"] += 1
            elif pr < 0.15: zones["5-15¢"] += 1
            elif pr < 0.35: zones["15-35¢"] += 1
            elif pr < 0.50: zones["35-50¢"] += 1
            elif pr < 0.65: zones["50-65¢"] += 1
            else: zones["65¢+"] += 1
        L.append(f"Цена РЕАЛЬНЫХ входов (BUY): медиана {med*100:.0f}¢")
        zline = " ".join(f"{z}:{c}" for z,c in zones.items() if c)
        L.append(f"  {zline}")
        # YES/NO по сырым сделкам
        yes_b = sum(1 for t in buys if t.get("outcome")=="Yes")
        no_b = sum(1 for t in buys if t.get("outcome")=="No")
        L.append(f"  Покупки: YES {yes_b} / NO {no_b}")
    L.append("")

    # ── Размер сделок ──
    usds = [float(t.get("usdcSize",0) or 0) for t in buys if t.get("usdcSize")]
    if usds:
        med_u = sorted(usds)[len(usds)//2]
        L.append(f"Размер сделки (USDC): медиана ${med_u:.0f} "
                 f"(${min(usds):.0f}–${max(usds):.0f})")

    # ── Частота: сделок в день, всплески ──
    ts = sorted([t.get("timestamp",0) for t in trades if t.get("timestamp")])
    if len(ts) > 1:
        span = (ts[-1]-ts[0])/86400
        if span >= 1:
            L.append(f"Период: {span:.0f} дней, {len(trades)/span:.1f} сделок/день")

    return "\n".join(L)


def build_market_detail(activity, n=3):
    """Детализация НЕСКОЛЬКИХ рынков: полная траектория входов/выходов."""
    from collections import defaultdict
    trades = [a for a in activity if a.get("type") == "TRADE"]
    by_market = defaultdict(list)
    for t in trades:
        by_market[t.get("conditionId")].append(t)
    # Берём рынки с наибольшим числом сделок (где видна механика докупок)
    markets_sorted = sorted(by_market.items(), key=lambda kv: -len(kv[1]))
    L = ["🔍 ДЕТАЛИ РЫНКОВ (траектория входов)", ""]
    for cid, tl in markets_sorted[:n]:
        title = (tl[0].get("title","?") or "?")[:40]
        L.append(f"📍 {title}")
        tl_sorted = sorted(tl, key=lambda x: x.get("timestamp",0))
        import datetime
        for t in tl_sorted[:12]:  # до 12 сделок на рынок
            side = t.get("side","?")
            price = float(t.get("price",0) or 0)
            usd = float(t.get("usdcSize",0) or 0)
            out = t.get("outcome","")
            ts = t.get("timestamp",0)
            try:
                dt = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%d.%m %H:%M")
            except Exception:
                dt = "?"
            emoji = "🟢" if side=="BUY" else "🔴"
            L.append(f"  {emoji} {dt} {side} {out} {price*100:.0f}¢ ${usd:.0f}")
        L.append("")
    return "\n".join(L)


async def build_frequency(session, wallet, window_days=7):
    """ЧЕСТНАЯ частота ставок: считает уникальные рынки и сделки в ФИКСИРОВАННОМ
    окне последних N полных дней. Пагинирует /activity, пока не выйдем за окно —
    так числитель (рынки) и знаменатель (дни) оба корректны, без искажения
    потолком. Отвечает: сколько РЫНКОВ/день и сколько ДОКУПОК на рынок реально."""
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc)
    # Окно: последние N ПОЛНЫХ суток (от начала дня N дней назад до сейчас)
    window_start = (now - datetime.timedelta(days=window_days)).timestamp()

    # Пагинируем activity, пока сделки свежее window_start
    all_trades = []
    offset = 0
    page = 500
    pages_done = 0
    reached_window_end = False
    for _ in range(60):  # до 30000 записей, с запасом
        p = {"user": wallet, "limit": page, "offset": offset}
        try:
            async with session.get(f"{DATA_API}/activity", params=p,
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    break
                batch = await r.json()
        except Exception as e:
            log.warning("freq fetch offset=%s: %s", offset, e)
            break
        if not batch:
            break
        pages_done += 1
        # Оставляем только TRADE BUY в окне
        for a in batch:
            if a.get("type") != "TRADE" or a.get("side") != "BUY":
                continue
            ts = a.get("timestamp", 0)
            if ts < window_start:
                reached_window_end = True
                continue
            all_trades.append(a)
        # Если последняя запись в батче уже старше окна — дальше только старее
        oldest_ts = min((a.get("timestamp", 1e18) for a in batch), default=0)
        if oldest_ts < window_start:
            reached_window_end = True
            break
        if len(batch) < page:
            break
        offset += page
        await asyncio.sleep(PAUSE)

    if not all_trades:
        return (f"За последние {window_days} дней BUY-сделок не найдено "
                f"(или кошелёк неактивен в этом окне).")

    # Считаем уникальные рынки (conditionId) и докупки
    from collections import defaultdict
    by_market = defaultdict(list)
    for a in all_trades:
        by_market[a.get("conditionId")].append(a)

    n_markets = len(by_market)
    n_trades = len(all_trades)
    # Реальный охваченный период (от самой старой до самой свежей сделки в окне)
    ts_list = [a.get("timestamp", 0) for a in all_trades]
    span_days = max(1e-9, (max(ts_list) - min(ts_list)) / 86400)
    # Но если данные покрыли всё окно — делим на window_days; если оборвались
    # раньше (не дотянули до края окна) — на реальный span
    effective_days = window_days if reached_window_end else span_days

    markets_per_day = n_markets / effective_days
    trades_per_day = n_trades / effective_days
    addons = [len(v) for v in by_market.values()]
    avg_addon = sum(addons) / len(addons)
    multi = sum(1 for v in by_market.values() if len(v) > 1)

    # Распределение докупок
    one = sum(1 for v in by_market.values() if len(v) == 1)
    few = sum(1 for v in by_market.values() if 2 <= len(v) <= 5)
    many = sum(1 for v in by_market.values() if len(v) > 5)

    L = [f"📊 ЧАСТОТА СТАВОК — {wallet[:10]}…", ""]
    L.append(f"Окно: последние {window_days} дней"
             + ("" if reached_window_end else f" (данные дотянулись только до {span_days:.1f}д — пагинация не покрыла всё окно, возможно потолок)"))
    L.append("")
    L.append(f"Уникальных РЫНКОВ: {n_markets}")
    L.append(f"Всего BUY-сделок: {n_trades}")
    L.append(f"Охвачено дней: {effective_days:.1f}")
    L.append("")
    L.append(f"➡️ РЫНКОВ в день: {markets_per_day:.1f}")
    L.append(f"➡️ BUY-сделок в день: {trades_per_day:.1f}")
    L.append(f"➡️ Докупок на рынок (средн): {avg_addon:.1f}")
    L.append("")
    L.append(f"Рынков с докупками (>1 входа): {multi}/{n_markets} "
             f"({multi/n_markets*100:.0f}%)")
    L.append(f"Разбивка: 1 вход {one} │ 2-5 входов {few} │ >5 входов {many}")
    L.append("")
    L.append("СРАВНИ С НАШИМ БОТОМ:")
    L.append(f"  Их рынков/день: {markets_per_day:.1f}")
    L.append(f"  Если наш бот делает БОЛЬШЕ — берём лишние рынки (мусор?)")
    L.append(f"  Если МЕНЬШЕ — не дотягиваем по охвату")
    L.append("")
    L.append("⚠️ Если выше написано 'пагинация не покрыла всё окно' —")
    L.append("число дней оценено по span, точность ниже. Иначе — надёжно.")
    return "\n".join(L)


async def fetch_daily_max_actual(session, lat, lon, start, end):
    """Фактический дневной максимум за период (Open-Meteo Archive)."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {"latitude": lat, "longitude": lon, "start_date": start,
              "end_date": end, "daily": "temperature_2m_max", "timezone": "UTC"}
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                return {}
            d = (await r.json()).get("daily", {})
            return dict(zip(d.get("time", []), d.get("temperature_2m_max", [])))
    except Exception as e:
        log.warning("hist actual: %s", e)
        return {}


async def fetch_forecast_models(session, lat, lon, start, end):
    """Архив прогнозов по нескольким моделям (Historical Forecast API).
    Возвращает {date: [прогноз_модель1, прогноз_модель2, ...]}.
    Разброс моделей = наша σ (как в ансамбле бота)."""
    from collections import defaultdict
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    models = ["gfs_seamless", "ecmwf_ifs025", "icon_seamless", "gem_seamless"]
    out = defaultdict(list)
    for model in models:
        params = {"latitude": lat, "longitude": lon, "start_date": start,
                  "end_date": end, "daily": "temperature_2m_max",
                  "timezone": "UTC", "models": model}
        try:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status != 200:
                    continue
                d = (await r.json()).get("daily", {})
                for date, temp in zip(d.get("time", []),
                                      d.get("temperature_2m_max", [])):
                    if temp is not None:
                        out[date].append(temp)
        except Exception as e:
            log.warning("hist forecast %s: %s", model, e)
        await asyncio.sleep(0.2)
    return out


def _normal_cdf(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


async def build_histcalib(session, progress_cb=None):
    """ИСТОРИЧЕСКАЯ КАЛИБРОВКА: тянет прогноз+факт за 60 дней по городам,
    проверяет — занижена ли σ (распределение узкое) и/или толстые ли хвосты.
    Различает ДВЕ болезни с разными лекарствами."""
    import datetime
    # 5 городов по умолчанию — баланс скорости и покрытия климатов
    cities = {
        "Tokyo":    (35.55, 139.78),
        "New York": (40.78, -73.97),
        "London":   (51.51, -0.06),
        "Karachi":  (24.90, 67.17),
        "Chicago":  (41.98, -87.90),
    }
    days_back = 60
    today = datetime.date.today()
    end = today - datetime.timedelta(days=2)
    start = end - datetime.timedelta(days=days_back)
    start_s, end_s = start.isoformat(), end.isoformat()

    all_z = []
    raw_dev = []
    for i, (city, (lat, lon)) in enumerate(cities.items(), 1):
        if progress_cb:
            await progress_cb(f"⏳ {city} ({i}/{len(cities)})…")
        actual = await fetch_daily_max_actual(session, lat, lon, start_s, end_s)
        await asyncio.sleep(0.2)
        fc = await fetch_forecast_models(session, lat, lon, start_s, end_s)
        for date, vals in fc.items():
            if len(vals) < 2 or date not in actual or actual[date] is None:
                continue
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / len(vals)
            sigma = max(0.5, math.sqrt(var))   # пол 0.5° как в боте
            dev = actual[date] - mean
            all_z.append(dev / sigma)
            raw_dev.append(dev)

    n = len(all_z)
    if n < 30:
        return f"⚠️ Мало данных ({n} пар). Возможно, архив прогнозов недоступен на эту глубину."

    L = [f"📊 ИСТОРИЧЕСКАЯ КАЛИБРОВКА ({n} пар, {len(cities)} городов, {days_back}д)", ""]

    # 1. Ширина: средний |z|
    mean_abs_z = sum(abs(z) for z in all_z) / n
    L.append(f"1. Средний |z| = {mean_abs_z:.3f} (эталон нормали 0.798)")
    wide = mean_abs_z > 0.95
    if wide:
        L.append(f"   → σ ЗАНИЖЕНА: факт отклоняется ШИРЕ нашей σ (×{mean_abs_z/0.798:.2f})")
    elif mean_abs_z < 0.65:
        L.append(f"   → σ завышена (распределение шире реального)")
    else:
        L.append(f"   → σ по ширине калибрована")
    L.append("")

    # 2. Хвосты
    p2 = sum(1 for z in all_z if abs(z) > 2) / n
    p3 = sum(1 for z in all_z if abs(z) > 3) / n
    fat = p2 > 0.07 or p3 > 0.01
    L.append(f"2. Хвосты: |z|>2σ {p2*100:.1f}% (норма 4.6%), |z|>3σ {p3*100:.1f}% (норма 0.27%)")
    if fat:
        L.append(f"   → ТОЛСТЫЕ ХВОСТЫ: экстремумы чаще нормали!")
    else:
        L.append(f"   → хвосты как у нормали")
    L.append("")

    # 3. Bias
    mean_dev = sum(raw_dev) / len(raw_dev)
    L.append(f"3. Сдвиг (факт−прогноз): {mean_dev:+.2f}°C")
    if abs(mean_dev) > 0.5:
        d = "выше" if mean_dev > 0 else "ниже"
        L.append(f"   → прогноз смещён: факт систематически {d} (отдельная проблема)")
    else:
        L.append(f"   → центрирован верно")
    L.append("")

    # ВЕРДИКТ
    L.append("ВЕРДИКТ — лекарство:")
    if wide and fat:
        L.append(f"σ занижена И хвосты толстые → расширить σ (×{mean_abs_z/0.798:.2f}) + t-распределение.")
    elif wide and not fat:
        L.append(f"σ занижена, форма нормальная → расширить σ в ×{mean_abs_z/0.798:.2f}.")
        L.append("Чинит И завышение центра, И занижение хвостов (одна болезнь!).")
    elif fat and not wide:
        L.append("σ по ширине ок, хвосты толстые → t-распределение (df 3-5), σ не трогать.")
    else:
        L.append("Распределение калибровано → дело не в форме. Причина в другом (bias/модели).")
    L.append("")
    L.append("⚠️ 'σ' = разброс 4 моделей Open-Meteo, приближение нашего ансамбля.")
    L.append("Тренд показателен, точные числа — ориентир.")
    return "\n".join(L)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="ℹ️ Как пользоваться")]],
        resize_keyboard=True, persistent=True)
    await message.answer(
        "🔍 Trader Check\n\n"
        "Разбор любого трейдера Polymarket по кошельку.\n\n"
        "Просто пришли адрес кошелька (0x…) — и я выдам:\n"
        "• P&L, ROI, винрейт (с учётом проигравших)\n"
        "• Зоны входа, YES/NO, размер ставок\n"
        "• Держит до резолюции или торгует\n"
        "• Для погодных — разбор по городам\n\n"
        "Или команда: /check 0x…",
         reply_markup=kb)


@dp.message(lambda m: m.text == "ℹ️ Как пользоваться")
async def btn_help(message: types.Message):
    await message.answer(
        "Пришли адрес кошелька Polymarket в формате 0x… (40 символов).\n\n"
        "Найти кошелёк: на странице профиля трейдера он в URL после /profile/.\n\n"
        "Бот скачает всю историю и разберёт стратегию.")


@dp.message(Command("freq"))
async def cmd_freq(message: types.Message):
    """Честная частота ставок трейдера за фиксированное окно (с пагинацией)."""
    m = WALLET_RE.search(message.text or "")
    if not m:
        await message.answer("Укажи адрес: /freq 0x… (частота ставок за 7 дней)")
        return
    status = await message.answer("📊 Считаю частоту ставок (пагинация, ~30 сек)…")
    try:
        async with aiohttp.ClientSession() as session:
            result = await build_frequency(session, m.group(0), window_days=7)
        await status.edit_text(result[:4000], disable_web_page_preview=True)
    except Exception as e:
        log.exception("freq failed")
        await status.edit_text(f"❌ Ошибка: {type(e).__name__}: {e}")


@dp.message(Command("histcalib"))
async def cmd_histcalib(message: types.Message):
    """Историческая калибровка распределения: занижена ли σ / толстые ли хвосты.
    Тянет прогноз+факт за 60 дней по 5 городам. ~3-5 мин."""
    status = await message.answer("📊 Историческая калибровка распределения.\n"
                                  "Тяну прогноз+факт за 60 дней (~3-5 мин)…")
    async def progress(text):
        try:
            await status.edit_text(text)
        except Exception:
            pass
    try:
        async with aiohttp.ClientSession() as session:
            result = await build_histcalib(session, progress_cb=progress)
        await status.edit_text(result[:4000], disable_web_page_preview=True)
    except Exception as e:
        log.exception("histcalib failed")
        await status.edit_text(f"❌ Ошибка калибровки: {type(e).__name__}: {e}")


@dp.message(Command("check"))
async def cmd_check(message: types.Message):
    m = WALLET_RE.search(message.text or "")
    if not m:
        await message.answer("Укажи адрес: /check 0x…")
        return
    await run_analysis(message, m.group(0))


@dp.message(Command("deep"))
async def cmd_deep(message: types.Message):
    m = WALLET_RE.search(message.text or "")
    if not m:
        await message.answer("Укажи адрес: /deep 0x…")
        return
    await run_deep(message, m.group(0))


async def run_deep(message, wallet):
    msg = await message.answer(f"🔬 Глубокий разбор {wallet[:12]}…\n"
                               f"Качаю ВСЕ сделки посделочно…")
    try:
        async with aiohttp.ClientSession() as session:
            # /activity — СЫРЫЕ сделки с timestamp, ценой, размером каждая
            activity = await fetch_all(session, "activity",
                                       {"user": wallet, "sortBy": "TIMESTAMP"},
                                       page_size=500, max_pages=20)
        if not activity:
            await msg.edit_text("❌ Активности не нашёл. Проверь адрес.")
            return
        last_analysis[message.chat.id] = {"activity": activity, "wallet": wallet,
                                          "deep": True}
        text = build_deep(activity)
        if len(text) > 4000:
            text = text[:4000]
        await msg.edit_text(text, disable_web_page_preview=True)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Траектории рынков",
                                  callback_data="d:detail")],
            [InlineKeyboardButton(text="📥 Матрица входов (цена/время→исход)",
                                  callback_data="d:inputs")],
            [InlineKeyboardButton(text="💰 Размер и время",
                                  callback_data="d:sizetime")],
            [InlineKeyboardButton(text="🚀 Старт и влияние на цену",
                                  callback_data="d:startimpact")],
            [InlineKeyboardButton(text="🌡 Сверка входов с фактом погоды",
                                  callback_data="d:weather")]])
        await message.answer("Подробнее:", reply_markup=kb)
    except Exception as e:
        log.exception("run_deep failed")
        err = f"❌ Ошибка: {type(e).__name__}: {e}"
        try:
            await msg.edit_text(err)
        except Exception:
            await message.answer(err)


@dp.callback_query(lambda c: c.data == "d:detail")
async def cb_detail(callback: types.CallbackQuery):
    await callback.answer()
    data = last_analysis.get(callback.message.chat.id)
    if not data or "activity" not in data:
        await callback.message.answer("Сначала сделай /deep 0x…")
        return
    await callback.message.answer(build_market_detail(data["activity"]),
                                  disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == "d:inputs")
async def cb_inputs(callback: types.CallbackQuery):
    await callback.answer()
    data = last_analysis.get(callback.message.chat.id)
    if not data or "activity" not in data:
        await callback.message.answer("Сначала сделай /deep 0x…")
        return
    await callback.message.answer(build_inputs(data["activity"]),
                                  disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == "d:sizetime")
async def cb_sizetime(callback: types.CallbackQuery):
    await callback.answer()
    data = last_analysis.get(callback.message.chat.id)
    if not data or "activity" not in data:
        await callback.message.answer("Сначала сделай /deep 0x…")
        return
    await callback.message.answer(build_size_time(data["activity"]),
                                  disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == "d:startimpact")
async def cb_startimpact(callback: types.CallbackQuery):
    await callback.answer()
    data = last_analysis.get(callback.message.chat.id)
    if not data or "activity" not in data:
        await callback.message.answer("Сначала сделай /deep 0x…")
        return
    await callback.message.answer(build_start_impact(data["activity"]),
                                  disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == "d:weather")
async def cb_weather(callback: types.CallbackQuery):
    await callback.answer()
    data = last_analysis.get(callback.message.chat.id)
    if not data or "activity" not in data:
        await callback.message.answer("Сначала сделай /deep 0x…")
        return
    msg = await callback.message.answer("🌡 Тяну факт погоды с Open-Meteo… "
                                        "(до минуты)")
    try:
        async with aiohttp.ClientSession() as session:
            text = await build_weather_check(session, data["activity"], limit=10)
        if len(text) > 4000:
            text = text[:4000]
        await msg.edit_text(text, disable_web_page_preview=True)
    except Exception as e:
        log.exception("weather check failed")
        await msg.edit_text(f"❌ Ошибка сверки: {type(e).__name__}: {e}")


# Любое сообщение с адресом кошелька → разбор
@dp.message(lambda m: m.text and WALLET_RE.search(m.text))
async def on_wallet(message: types.Message):
    await run_analysis(message, WALLET_RE.search(message.text).group(0))


# ─── Кнопки углубления ────────────────────────────────────────────────────────

@dp.callback_query(lambda c: c.data == "a:cities")
async def cb_cities(callback: types.CallbackQuery):
    await callback.answer()
    data = last_analysis.get(callback.message.chat.id)
    if not data:
        await callback.message.answer("Сначала пришли кошелёк.")
        return
    await callback.message.answer(build_cities(data["closed"]),
                                   disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == "a:recent")
async def cb_recent(callback: types.CallbackQuery):
    await callback.answer()
    data = last_analysis.get(callback.message.chat.id)
    if not data:
        await callback.message.answer("Сначала пришли кошелёк.")
        return
    await callback.message.answer(build_recent(data["trades"]),
                                   disable_web_page_preview=True)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set!")
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start", description="Старт"),
        BotCommand(command="check", description="Разбор кошелька: /check 0x…"),
        BotCommand(command="deep", description="Глубокий посделочный: /deep 0x…"),
        BotCommand(command="freq", description="Частота ставок за 7 дней: /freq 0x…"),
        BotCommand(command="histcalib", description="Калибровка распределения погоды (σ/хвосты)"),
    ])
    log.info("Trader Check запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
