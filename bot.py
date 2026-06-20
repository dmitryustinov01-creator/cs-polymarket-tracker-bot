import asyncio
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


@dp.message(Command("start"))
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
                                  callback_data="d:startimpact")]])
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
    ])
    log.info("Trader Check запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
