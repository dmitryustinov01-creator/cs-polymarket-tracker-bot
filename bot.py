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

    L = [f"📊 <b>{name}</b>"]
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

    L.append(f"💰 Реализованный P&L: <b>${fmt_money(realized)}</b>")
    if invested > 0:
        L.append(f"   ROI: {realized/invested*100:+.0f}% (вложено ${invested:,.0f})")
    if nc:
        L.append(f"📈 Винрейт: <b>{len(wins)}/{nc}</b> ({pct(len(wins), nc)}) по закрытым")
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
        L.append(f"🎯 Вход: медиана <b>{med*100:.0f}¢</b>, "
                 f"чаще {top_zone[0]} ({pct(top_zone[1], len(prices))})")
        zline = " ".join(f"{z}:{c}" for z,c in zones.items() if c)
        L.append(f"   {zline}")

        # Винрейт ПО ЗОНАМ — где трейдер реально зарабатывает
        zone_stats = {}
        for p in closed:
            pr = float(p.get("avgPrice",0) or 0)
            rp = float(p.get("realizedPnl",0) or 0)
            if pr < 0.15: z="дешёвые<15¢"
            elif pr < 0.50: z="средние15-50¢"
            else: z="дорогие50¢+"
            if z not in zone_stats: zone_stats[z]={"n":0,"w":0,"pnl":0.0}
            zone_stats[z]["n"]+=1
            zone_stats[z]["pnl"]+=rp
            if rp>0: zone_stats[z]["w"]+=1
        L.append("   P&L по зонам входа:")
        for z in ["дешёвые<15¢","средние15-50¢","дорогие50¢+"]:
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
        L.append(f"📏 Ставка: медиана <b>${med_s:.0f}</b> "
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
    L = ["🏙 <b>По городам</b> (реализованный P&L):", ""]
    for c, d in items[:25]:
        if d["n"] < 1: continue
        mark = "🟢" if d["pnl"] > 0 else "🔴"
        L.append(f"{mark} {c}: {d['win']}/{d['n']} ${fmt_money(d['pnl'])}")
    return "\n".join(L)


def build_recent(trades, limit=15):
    """Последние сделки."""
    L = ["🕐 <b>Последние сделки</b>:", ""]
    for t in trades[:limit]:
        side = t.get("side", "?")
        price = float(t.get("price", 0) or 0)
        title = (t.get("title", "?") or "?")[:35]
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
        await msg.edit_text(text, parse_mode="HTML",
                            disable_web_page_preview=True)
        await message.answer("Подробнее:", reply_markup=analysis_keyboard(is_w))
    except Exception as e:
        log.exception("run_analysis failed")
        try:
            await msg.edit_text(f"❌ Ошибка при разборе: {type(e).__name__}: {e}\n"
                                f"Попробуй ещё раз.")
        except Exception:
            await message.answer(f"❌ Ошибка: {type(e).__name__}: {e}")


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="ℹ️ Как пользоваться")]],
        resize_keyboard=True, persistent=True)
    await message.answer(
        "<b>🔍 Trader Check</b>\n\n"
        "Разбор любого трейдера Polymarket по кошельку.\n\n"
        "Просто пришли адрес кошелька (0x…) — и я выдам:\n"
        "• P&L, ROI, винрейт (с учётом проигравших)\n"
        "• Зоны входа, YES/NO, размер ставок\n"
        "• Держит до резолюции или торгует\n"
        "• Для погодных — разбор по городам\n\n"
        "Или команда: /check 0x…",
        parse_mode="HTML", reply_markup=kb)


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
                                  parse_mode="HTML", disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == "a:recent")
async def cb_recent(callback: types.CallbackQuery):
    await callback.answer()
    data = last_analysis.get(callback.message.chat.id)
    if not data:
        await callback.message.answer("Сначала пришли кошелёк.")
        return
    await callback.message.answer(build_recent(data["trades"]),
                                  parse_mode="HTML", disable_web_page_preview=True)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set!")
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start", description="Старт"),
        BotCommand(command="check", description="Разбор кошелька: /check 0x…"),
    ])
    log.info("Trader Check запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
